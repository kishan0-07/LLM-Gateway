import time
from dataclasses import dataclass
from typing import AsyncIterator
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.models import GatewayRequest, ProviderAttempt
from app.application.services.budget_authorizer import BudgetAuthorizer
from app.application.services.routing_engine import RoutingEngine
from app.application.services.response_validator import ResponseValidator
from app.application.services.sanitizer import sanitize
from app.application.services.token_estimator import TokenEstimator
from app.infrastructure.redis.circuit_breaker import CircuitBreaker
from app.application.ports.rate_limiter import RateLimiter
from app.application.ports.event_sink import EventSink
from app.application.services import model_catalog
from app.domain.provider import ProviderStreamEvent, ProviderError
from app.core.logging import logger
from sqlalchemy import update


@dataclass(frozen=True)
class StreamRequest:
    tenant_id: int
    trace_id: str
    model: str
    messages: list[dict]
    max_tokens: int | None = None


class StreamCompletion:
    def __init__(
        self,
        budget_authorizer: BudgetAuthorizer,
        routing_engine: RoutingEngine,
        circuit_breaker: CircuitBreaker,
        response_validator: ResponseValidator,
        rate_limiter: RateLimiter,
        event_sink: EventSink,
        token_estimator: TokenEstimator,
    ):
        self._budget_authorizer = budget_authorizer
        self._routing_engine = routing_engine
        self._circuit = circuit_breaker
        self._validator = response_validator
        self._rate_limiter = rate_limiter
        self._event_sink = event_sink
        self._token_estimator = token_estimator

    async def stream(self, request: StreamRequest) -> AsyncIterator[ProviderStreamEvent]:
        try:
            model_catalog.get(request.model)
        except KeyError as exc:
            yield ProviderStreamEvent(type="error", content=str(exc))
            return
        # --- Pre-stream setup (before any yield) ---
        gateway_request_id = await self._create_gateway_request(request)
        await self._rate_limiter.check(request.tenant_id, 0)

        reservation = await self._budget_authorizer.authorize(
            tenant_id=request.tenant_id, gateway_request_id=gateway_request_id,
            model=request.model, messages=request.messages,
            requested_max_tokens=request.max_tokens,
        )
        if not reservation.approved:
            await self._update_gateway_request_status(gateway_request_id, "budget_rejected")
            yield ProviderStreamEvent(type="error", content=reservation.reason or "over_budget")
            return

        # BUG FIX: model_catalog.get() inside plan() raises KeyError for unknown models.
        # In non-streaming, the route catches KeyError → 400. In streaming, the generator
        # is already running — an unhandled KeyError would crash silently.
        try:
            candidates = self._routing_engine.plan(request.model)
        except KeyError as exc:
            await self._update_gateway_request_status(gateway_request_id, "failed")
            await self._budget_authorizer.settle(
                reservation_id=reservation.reservation_id,
                provider="none", model=request.model,
                input_tokens=0, output_tokens=0, status="error",
            )
            yield ProviderStreamEvent(type="error", content=str(exc))
            return
        provider_found = False

        for candidate in candidates:
            if not await self._circuit.is_available(candidate.provider.metadata.name, candidate.model):
                logger.info("stream_circuit_skipped", provider=candidate.provider.metadata.name,
                            model=candidate.model, trace_id=request.trace_id)
                continue

            # --- Stream from this candidate ---
            attempt_id = await self._start_provider_attempt(
                gateway_request_id=gateway_request_id,
                provider=candidate.provider.metadata.name,
                model=candidate.model, attempt_number=1,
            )

            accumulated_text = ""
            approx_output_tokens = 0
            next_budget_check_at = 100  # BUG FIX: threshold instead of modulo
            actual_input_tokens: int | None = None
            actual_output_tokens: int | None = None
            final_status = "error"
            start = time.perf_counter()

            try:
                async for event in candidate.provider.stream(candidate.model, request.messages):
                    if event.type == "delta":
                        accumulated_text += event.content or ""
                        approx_output_tokens += len(
                            self._token_estimator._get_encoder(
                                model_catalog.get(candidate.model).tokenizer_hint
                            ).encode(event.content or "")
                        )
                        yield event

                        # Mid-stream budget check every ~100 output tokens
                        # BUG FIX: uses threshold comparison, not modulo
                        # modulo fails because token counts jump by 2-5 per chunk
                        # (e.g. 98 → 103), so % 100 would never equal 0
                        if approx_output_tokens >= next_budget_check_at:
                            next_budget_check_at += 100
                            remaining = await self._budget_authorizer.remaining_usd(request.tenant_id)
                            if remaining <= 0:
                                final_status = "budget_exceeded"
                                yield ProviderStreamEvent(type="error", content="budget_exceeded_mid_stream")
                                return  # finally block will finalize

                    elif event.type == "usage":
                        actual_input_tokens = event.input_tokens
                        actual_output_tokens = event.output_tokens

                    elif event.type == "error":
                        # Provider yielded an error event (timeout, rate_limit, etc.)
                        final_status = "provider_error"
                        # Parse error category from content for circuit breaker
                        error_content = event.content or ""
                        if any(cat in error_content for cat in ("timeout", "rate_limited", "server_error")):
                            await self._circuit.record_failure(
                                candidate.provider.metadata.name, candidate.model
                            )
                        yield event
                        return  # finally block will finalize

                    elif event.type == "done":
                        final_status = "success"
                        break
                # Stream completed without explicit "done" — treat as success if we got data
                if final_status != "success" and accumulated_text:
                    final_status = "success"

                provider_found = True
                break  # success — don't try next candidate

            except Exception as exc:
                final_status = "error"
                logger.warning("stream_exception", provider=candidate.provider.metadata.name,
                               model=candidate.model, error=str(exc), trace_id=request.trace_id)
                yield ProviderStreamEvent(type="error", content=f"stream_error: {exc}")
                return  # finally block will finalize

            finally:
                latency_ms = int((time.perf_counter() - start) * 1000)
                await self._finish_provider_attempt(attempt_id, status=final_status, latency_ms=latency_ms)

                # --- Determine token counts for settlement ---
                if actual_input_tokens is not None and actual_output_tokens is not None:
                    # Provider gave us real counts (stream_options={"include_usage": True})
                    settle_input = actual_input_tokens
                    settle_output = actual_output_tokens
                    usage_source_label = "actual"
                else:
                    # Fallback: estimate from accumulated text
                    settle_input = self._token_estimator.estimate_input_tokens(
                        request.messages, candidate.model
                    )
                    settle_output = len(
                        self._token_estimator._get_encoder(
                            model_catalog.get(candidate.model).tokenizer_hint
                        ).encode(accumulated_text)
                    ) if accumulated_text else 0
                    usage_source_label = "estimated"

                # --- Circuit breaker update ---
                if final_status == "success":
                    await self._circuit.record_success(
                        candidate.provider.metadata.name, candidate.model
                    )

                # --- Budget settlement (exactly once — Decision 8) ---
                settle_status = "success" if final_status == "success" else "error"
                await self._budget_authorizer.settle(
                    reservation_id=reservation.reservation_id,
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    input_tokens=settle_input,
                    output_tokens=settle_output,
                    status=settle_status,
                )

                # --- Update gateway request ---
                gw_status = "completed" if final_status == "success" else "failed"
                await self._update_gateway_request_status(gateway_request_id, gw_status)

                # --- PII-safe event emit ---
                cost_usd = model_catalog.estimate_cost_usd(
                    candidate.model, settle_input, settle_output
                )
                await self._emit_event(
                    event_type=f"stream_{final_status}",
                    trace_id=request.trace_id,
                    tenant_id=request.tenant_id,
                    gateway_request_id=gateway_request_id,
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    input_tokens=settle_input,
                    output_tokens=settle_output,
                    cost_usd=cost_usd,
                    usage_source=usage_source_label,
                    prompt_excerpt=request.messages[-1].get("content", "") if request.messages else "",
                    response_excerpt=accumulated_text,
                )

        if not provider_found:
            # All candidates circuit-broken or missing — never started streaming
            await self._budget_authorizer.settle(
                reservation_id=reservation.reservation_id,
                provider="none", model=request.model,
                input_tokens=0, output_tokens=0, status="error",
            )
            await self._update_gateway_request_status(gateway_request_id, "failed")
            yield ProviderStreamEvent(type="error", content="all_providers_unavailable")

    # --- Helper methods (same pattern as ExecuteCompletion) ---

    async def _emit_event(self, event_type: str, trace_id: str, tenant_id: int,
                          gateway_request_id: int, provider: str, model: str,
                          input_tokens: int, output_tokens: int, cost_usd: float,
                          prompt_excerpt: str, response_excerpt: str, **extra) -> None:
        event = {
            "event": event_type,
            "trace_id": trace_id,
            "tenant_id": tenant_id,
            "request_id": gateway_request_id,
            "provider": provider,
            "model": model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost_usd": f"{cost_usd:.6f}",
            "prompt_excerpt": sanitize(prompt_excerpt),
            "response_excerpt": sanitize(response_excerpt),
            **extra,
        }
        await self._event_sink.emit(event)

    async def _create_gateway_request(self, request: StreamRequest) -> int:
        async with AsyncSessionLocal() as session:
            row = GatewayRequest(
                tenant_id=request.tenant_id, trace_id=request.trace_id, status="pending",
            )
            session.add(row)
            await session.commit()
            return row.id

    async def _update_gateway_request_status(self, gateway_request_id: int, status: str) -> None:
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(GatewayRequest)
                .where(GatewayRequest.id == gateway_request_id)
                .values(status=status)
            )
            await session.commit()

    async def _start_provider_attempt(
        self, gateway_request_id: int, provider: str, model: str, attempt_number: int,
    ) -> int:
        async with AsyncSessionLocal() as session:
            attempt = ProviderAttempt(
                gateway_request_id=gateway_request_id,
                provider=provider, model=model,
                attempt_number=attempt_number, status="in_progress",
            )
            session.add(attempt)
            await session.commit()
            return attempt.id

    async def _finish_provider_attempt(
        self, attempt_id: int, status: str, latency_ms: int,
    ) -> None:
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(ProviderAttempt)
                .where(ProviderAttempt.id == attempt_id)
                .values(status=status, latency_ms=latency_ms)
            )
            await session.commit()