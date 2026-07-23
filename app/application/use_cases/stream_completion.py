import time
import asyncio
from dataclasses import dataclass
from typing import AsyncIterator
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.models import GatewayRequest, ProviderAttempt
from app.application.services.budget_authorizer import BudgetAuthorizer
from app.application.services.routing_engine import RoutingEngine, RouteCandidate
from app.application.services.response_validator import ResponseValidator
from app.application.services.sanitizer import sanitize
from app.application.services.token_estimator import TokenEstimator
from app.infrastructure.redis.circuit_breaker import CircuitBreaker
from app.application.ports.rate_limiter import (
    RateLimiter,
    RateLimitExceeded,
    RateLimitBackendUnavailable,
)
from app.application.ports.event_sink import EventSink
from app.application.services import model_catalog
from app.domain.provider import ProviderStreamEvent, ProviderError
from app.core.logging import logger
from sqlalchemy import update
from app.application.ports.budget_store import (
    BudgetBackendUnavailable,
    DatabaseUnavailable,
    BudgetExceededMidStream,
)


@dataclass(frozen=True)
class StreamRequest:
    tenant_id: int
    api_key_id: int
    trace_id: str
    model: str
    messages: list[dict]
    max_tokens: int | None = None


@dataclass(frozen=True)
class PreparedStream:
    request: StreamRequest
    gateway_request_id: int
    reservation_id: str
    output_cap: int
    candidates: list[RouteCandidate]


DEFAULT_STREAM_TIMEOUT_SECONDS = 30.0


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
        *,
        stream_timeout_seconds: float = DEFAULT_STREAM_TIMEOUT_SECONDS,
    ) -> None:
        if stream_timeout_seconds <= 0:
            raise ValueError("stream timeout must be positive")

        self._budget_authorizer = budget_authorizer
        self._routing_engine = routing_engine
        self._circuit = circuit_breaker
        self._validator = response_validator
        self._rate_limiter = rate_limiter
        self._event_sink = event_sink
        self._token_estimator = token_estimator
        self._stream_timeout_seconds = stream_timeout_seconds

    async def prepare(self, request: StreamRequest) -> PreparedStream:
        try:
            model_catalog.get(request.model)
        except KeyError as exc:
            raise ProviderError(
                provider="gateway",
                category="invalid_request",
                message=str(exc),
                retryable=False,
            ) from exc

        try:
            gateway_request_id = await self._create_gateway_request(request)
        except Exception as exc:
            raise DatabaseUnavailable() from exc

        try:
            await self._rate_limiter.check(request.tenant_id, request.api_key_id)
        except RateLimitExceeded:
            await self._update_gateway_request_status(
                gateway_request_id, "rate_limited"
            )
            raise
        except RateLimitBackendUnavailable:
            await self._update_gateway_request_status(
                gateway_request_id,
                "rate_limit_unavailable",
            )
            raise

        try:
            output_cap = self._token_estimator.output_cap(
                request.messages,
                request.model,
                request.max_tokens,
            )
        except ValueError as exc:
            await self._update_gateway_request_status(gateway_request_id, "failed")
            raise ProviderError(
                provider="gateway",
                category="invalid_request",
                message=str(exc),
                retryable=False,
            ) from exc

        try:
            reservation = await self._budget_authorizer.authorize(
                tenant_id=request.tenant_id,
                gateway_request_id=gateway_request_id,
                model=request.model,
                messages=request.messages,
                requested_max_tokens=output_cap,
            )
        except BudgetBackendUnavailable:
            await self._update_gateway_request_status(
                gateway_request_id,
                "budget_backend_unavailable",
            )
            raise
        if not reservation.approved:
            await self._update_gateway_request_status(
                gateway_request_id,
                "budget_rejected",
            )
            raise ProviderError(
                provider="gateway",
                category="invalid_request",
                message=reservation.reason or "over budget",
                retryable=False,
            )

        if reservation.reservation_id is None:
            raise RuntimeError("Approved reservation is missing an ID")

        candidates = self._routing_engine.plan(request.model)
        if not candidates:
            await self._budget_authorizer.settle(
                reservation_id=reservation.reservation_id,
                provider="none",
                model=request.model,
                input_tokens=0,
                output_tokens=0,
                status="error",
                usage_source="estimated",
            )
            await self._update_gateway_request_status(gateway_request_id, "failed")
            raise ProviderError(
                provider="gateway",
                category="server_error",
                message="no provider candidates are configured",
                retryable=True,
            )

        return PreparedStream(
            request=request,
            gateway_request_id=gateway_request_id,
            reservation_id=reservation.reservation_id,
            output_cap=output_cap,
            candidates=candidates,
        )

    async def stream(
        self, prepared: PreparedStream
    ) -> AsyncIterator[ProviderStreamEvent]:
        request = prepared.request
        gateway_request_id = prepared.gateway_request_id
        reservation_id = prepared.reservation_id
        output_cap = prepared.output_cap
        candidates = prepared.candidates

        provider_found = False

        for candidate in candidates:
            if not await self._circuit.is_available(
                candidate.provider.metadata.name, candidate.model
            ):
                logger.info(
                    "stream_circuit_skipped",
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    trace_id=request.trace_id,
                )
                continue

            # --- Stream from this candidate ---
            attempt_id = await self._start_provider_attempt(
                gateway_request_id=gateway_request_id,
                provider=candidate.provider.metadata.name,
                model=candidate.model,
                attempt_number=1,
            )

            accumulated_text = ""
            accumulated_parts: list[str] = []
            last_checked_chars = 0
            CHECK_EVERY_CHARACTERS = 400
            actual_input_tokens: int | None = None
            actual_output_tokens: int | None = None
            final_status = "error"
            start = time.perf_counter()

            try:
                async with asyncio.timeout(self._stream_timeout_seconds):
                    async for event in candidate.provider.stream(
                        candidate.model, request.messages, max_tokens=output_cap
                    ):
                        if event.type == "delta":
                            accumulated_parts.append(event.content or "")
                            accumulated_text = "".join(accumulated_parts)
                            yield event

                            if (
                                len(accumulated_text) - last_checked_chars
                                >= CHECK_EVERY_CHARACTERS
                            ):
                                last_checked_chars = len(accumulated_text)
                                try:
                                    await self._budget_authorizer.assert_provisional_stream_usage_within_reservation(
                                        reservation_id=reservation_id,
                                        model=candidate.model,
                                        messages=request.messages,
                                        accumulated_text=accumulated_text,
                                    )
                                except BudgetExceededMidStream:
                                    final_status = "budget_exceeded"
                                    yield ProviderStreamEvent(
                                        type="error",
                                        content="budget_exceeded_mid_stream",
                                    )
                                    return
                                except BudgetBackendUnavailable:
                                    final_status = "budget_backend_unavailable"
                                    yield ProviderStreamEvent(
                                        type="error",
                                        content="budget_backend_unavailable_mid_stream",
                                    )
                                    return

                        elif event.type == "usage":
                            actual_input_tokens = event.input_tokens
                            actual_output_tokens = event.output_tokens

                        elif event.type == "error":
                            final_status = "provider_error"
                            error_content = event.content or ""
                            if any(
                                cat in error_content
                                for cat in ("timeout", "rate_limited", "server_error")
                            ):
                                await self._circuit.record_failure(
                                    candidate.provider.metadata.name, candidate.model
                                )
                            # Sanitize: never forward raw provider error text to client
                            yield ProviderStreamEvent(
                                type="error", content="provider_stream_failed"
                            )
                            return

                        elif event.type == "done":
                            final_status = "success"
                            break

                    if final_status != "success" and accumulated_text:
                        final_status = "success"

                    provider_found = True
                    break

            except TimeoutError:
                final_status = "timeout"
                await self._circuit.record_failure(
                    candidate.provider.metadata.name, candidate.model
                )
                yield ProviderStreamEvent(type="error", content="stream_timeout")
                return
            except asyncio.CancelledError:
                final_status = "cancelled"
                raise
            except Exception as exc:
                final_status = "error"
                logger.warning(
                    "stream_exception",
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    error_type=type(exc).__name__,
                    trace_id=request.trace_id,
                )
                yield ProviderStreamEvent(type="error", content="internal_stream_error")
                return

            finally:
                latency_ms = int((time.perf_counter() - start) * 1000)
                if actual_input_tokens is not None and actual_output_tokens is not None:
                    settle_input = actual_input_tokens
                    settle_output = actual_output_tokens
                    usage_source_label = "actual"
                else:
                    settle_input = self._token_estimator.estimate_input_tokens(
                        request.messages, candidate.model
                    )
                    settle_output = (
                        len(
                            self._token_estimator._get_encoder(
                                model_catalog.get(candidate.model).tokenizer_hint
                            ).encode(accumulated_text)
                        )
                        if accumulated_text
                        else 0
                    )
                    usage_source_label = "estimated"
                cancelled_during_finalize = False
                try:
                    await asyncio.shield(
                        self._finalize_stream(
                            reservation_id=reservation_id,
                            gateway_request_id=gateway_request_id,
                            attempt_id=attempt_id,
                            final_status=final_status,
                            provider=candidate.provider.metadata.name,
                            model=candidate.model,
                            input_tokens=settle_input,
                            output_tokens=settle_output,
                            latency_ms=latency_ms,
                            usage_source=usage_source_label,
                        )
                    )
                except asyncio.CancelledError:
                    cancelled_during_finalize = True
                    logger.warning(
                        "stream_finalization_interrupted_by_cancellation",
                        reservation_id=reservation_id,
                    )
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
                    prompt_excerpt=request.messages[-1].get("content", "")
                    if request.messages
                    else "",
                    response_excerpt=accumulated_text,
                )
                if cancelled_during_finalize:
                    raise asyncio.CancelledError()

        if not provider_found:
            await self._budget_authorizer.settle(
                reservation_id=reservation_id,
                provider="none",
                model=request.model,
                input_tokens=0,
                output_tokens=0,
                status="error",
                usage_source="estimated",
            )
            await self._update_gateway_request_status(gateway_request_id, "failed")
            yield ProviderStreamEvent(type="error", content="all_providers_unavailable")

    # --- Helper methods (same pattern as ExecuteCompletion) ---

    async def _emit_event(
        self,
        event_type: str,
        trace_id: str,
        tenant_id: int,
        gateway_request_id: int,
        provider: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        cost_usd: float,
        prompt_excerpt: str,
        response_excerpt: str,
        **extra,
    ) -> None:
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
                tenant_id=request.tenant_id,
                api_key_id=request.api_key_id,
                trace_id=request.trace_id,
                status="pending",
                is_stream=True,
            )
            session.add(row)
            await session.commit()
            return row.id

    async def _update_gateway_request_status(
        self, gateway_request_id: int, status: str
    ) -> None:
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(GatewayRequest)
                .where(GatewayRequest.id == gateway_request_id)
                .values(status=status)
            )
            await session.commit()

    async def _start_provider_attempt(
        self,
        gateway_request_id: int,
        provider: str,
        model: str,
        attempt_number: int,
    ) -> int:
        async with AsyncSessionLocal() as session:
            attempt = ProviderAttempt(
                gateway_request_id=gateway_request_id,
                provider=provider,
                model=model,
                attempt_number=attempt_number,
                status="in_progress",
            )
            session.add(attempt)
            await session.commit()
            return attempt.id

    async def _finish_provider_attempt(
        self,
        attempt_id: int,
        status: str,
        latency_ms: int,
    ) -> None:
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(ProviderAttempt)
                .where(ProviderAttempt.id == attempt_id)
                .values(status=status, latency_ms=latency_ms)
            )
            await session.commit()

    async def _finalize_stream(
        self,
        *,
        reservation_id: str,
        gateway_request_id: int,
        attempt_id: int,
        final_status: str,
        provider: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        latency_ms: int,
        usage_source: str,
    ) -> None:
        try:
            await self._finish_provider_attempt(
                attempt_id, status=final_status, latency_ms=latency_ms
            )
            await self._budget_authorizer.settle(
                reservation_id=reservation_id,
                provider=provider,
                model=model,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                status="success" if final_status == "success" else "error",
                usage_source=usage_source,
            )
            if final_status == "success":
                await self._circuit.record_success(provider, model)
        except Exception as exc:
            logger.error(
                "stream_settlement_failed",
                reservation_id=reservation_id,
                error=str(exc),
            )
            try:
                await self._budget_authorizer.mark_needs_reconciliation(
                    reservation_id=reservation_id,
                    reason="settlement_failed_after_provider_attempt",
                )
            except Exception:
                pass
            raise
        else:
            await self._update_gateway_request_status(
                gateway_request_id,
                "completed" if final_status == "success" else "failed",
            )
