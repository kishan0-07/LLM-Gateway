import asyncio
import time
from dataclasses import dataclass, replace

from sqlalchemy.exc import SQLAlchemyError

from app.application.ports.budget_store import (
    DatabaseUnavailable,
)
from app.application.ports.event_sink import EventSink
from app.application.ports.rate_limiter import (
    RateLimitBackendUnavailable,
    RateLimitExceeded,
    RateLimiter,
)
from app.application.services import model_catalog
from app.application.services.budget_authorizer import (
    BudgetAuthorizer,
    CandidateExposure,
)
from app.application.services.response_validator import ResponseValidator
from app.application.services.routing_engine import RoutingEngine
from app.application.services.sanitizer import sanitize
from app.application.services.token_estimator import TokenEstimator
from app.core.logging import logger
from app.domain.budget import micros_to_decimal
from app.domain.provider import ProviderError, ProviderResult
from app.infrastructure.db.models import GatewayRequest, ProviderAttempt
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.redis.circuit_breaker import CircuitBreaker


@dataclass(frozen=True)
class CompletionRequest:
    tenant_id: int
    api_key_id: int
    trace_id: str
    model: str
    messages: list[dict]
    max_tokens: int | None = None


@dataclass(frozen=True)
class CompletionResponse:
    gateway_request_id: int
    content: str
    provider: str
    model: str
    input_tokens: int
    output_tokens: int
    cost_usd: float


class AllProvidersFailedError(Exception):
    """Every usable candidate failed or was unavailable."""


class ExecuteCompletion:
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
        self._token_estimator = token_estimator
        self._budget_authorizer = budget_authorizer
        self._routing_engine = routing_engine
        self._circuit = circuit_breaker
        self._validator = response_validator
        self._rate_limiter = rate_limiter
        self._event_sink = event_sink

    def _normalize_usage(
        self,
        result: ProviderResult,
        messages: list[dict],
    ) -> ProviderResult:
        if result.usage_source == "actual":
            return result
        return replace(
            result,
            input_tokens=self._token_estimator.estimate_input_tokens(
                messages,
                result.model,
            ),
            output_tokens=self._token_estimator.estimate_output_tokens_for_text(
                text=result.content,
                model=result.model,
            ),
            usage_source="estimated",
        )

    async def execute(self, request: CompletionRequest) -> CompletionResponse:
        try:
            model_catalog.get(request.model)
            self._budget_authorizer.estimate_candidate_exposure(
                model=request.model,
                messages=request.messages,
                requested_max_tokens=request.max_tokens,
            )
        except (KeyError, ValueError) as exc:
            raise ProviderError(
                provider="gateway",
                category="invalid_request",
                message=str(exc),
                retryable=False,
            ) from exc

        started_at = time.perf_counter()
        provider_latency_ms_total = 0
        request_cost_micros = 0

        gateway_request_id = await self._create_gateway_request(request)

        try:
            await self._rate_limiter.check(request.tenant_id, request.api_key_id)
        except RateLimitExceeded:
            await self._update_gateway_request_status(
                gateway_request_id,
                "rate_limited",
            )
            raise
        except RateLimitBackendUnavailable:
            await self._update_gateway_request_status(
                gateway_request_id,
                "rate_limit_unavailable",
            )
            raise

        reservation = await self._budget_authorizer.authorize(
            tenant_id=request.tenant_id,
            gateway_request_id=gateway_request_id,
            model=request.model,
            messages=request.messages,
            requested_max_tokens=request.max_tokens,
        )
        if not reservation.approved:
            await self._update_gateway_request_status(
                gateway_request_id,
                "budget_rejected",
            )
            raise ProviderError(
                provider="gateway",
                category="invalid_request",
                message=reservation.reason or "over_budget",
                retryable=False,
            )
        if reservation.reservation_id is None:
            raise RuntimeError("approved reservation is missing an ID")

        candidates = self._routing_engine.plan(request.model)
        last_error: ProviderError | None = None
        attempt_number = 0
        budget_blocked_fallback = False

        for candidate in candidates:
            if not await self._circuit.is_available(
                candidate.provider.metadata.name,
                candidate.model,
            ):
                logger.info(
                    "circuit_skipped",
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    trace_id=request.trace_id,
                )
                continue

            try:
                exposure = self._budget_authorizer.estimate_candidate_exposure(
                    model=candidate.model,
                    messages=request.messages,
                    requested_max_tokens=request.max_tokens,
                )
            except ValueError:
                logger.info(
                    "candidate_context_rejected",
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    trace_id=request.trace_id,
                )
                continue

            if not await self._budget_authorizer.ensure_attempt_capacity(
                reservation_id=reservation.reservation_id,
                exposure=exposure,
            ):
                budget_blocked_fallback = True
                break

            attempt_number += 1
            attempt_id = await self._start_provider_attempt(
                gateway_request_id=gateway_request_id,
                provider=candidate.provider.metadata.name,
                model=candidate.model,
                attempt_number=attempt_number,
                exposure=exposure,
            )

            attempt_started_at = time.perf_counter()
            try:
                raw_result = await candidate.provider.complete(
                    candidate.model,
                    request.messages,
                    max_tokens=exposure.output_cap,
                )
            except asyncio.CancelledError:
                try:
                    await self._budget_authorizer.mark_needs_reconciliation(
                        reservation_id=reservation.reservation_id,
                        reason="nonstream_provider_call_cancelled",
                    )
                except Exception:
                    logger.error(
                        "nonstream_cancellation_reconciliation_marker_failed",
                        reservation_id=reservation.reservation_id,
                        trace_id=request.trace_id,
                    )
                raise
            except ProviderError as exc:
                latency_ms = int((time.perf_counter() - attempt_started_at) * 1000)
                provider_latency_ms_total += latency_ms

                charged_micros = await self._record_failed_attempt(
                    reservation_id=reservation.reservation_id,
                    attempt_id=attempt_id,
                    candidate_exposure=exposure,
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    error=exc,
                    latency_ms=latency_ms,
                )
                request_cost_micros += charged_micros

                if exc.category in {"timeout", "rate_limited", "server_error"}:
                    await self._circuit.record_failure(
                        candidate.provider.metadata.name,
                        candidate.model,
                    )

                logger.warning(
                    "provider_attempt_failed",
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    error_category=exc.category,
                    retryable=exc.retryable,
                    trace_id=request.trace_id,
                )
                last_error = exc
                if not exc.retryable:
                    break
                continue
            except Exception as exc:
                latency_ms = int((time.perf_counter() - attempt_started_at) * 1000)
                provider_latency_ms_total += latency_ms
                normalized_error = ProviderError(
                    provider=candidate.provider.metadata.name,
                    category="server_error",
                    message="unexpected provider adapter failure",
                    retryable=True,
                )
                request_cost_micros += await self._record_failed_attempt(
                    reservation_id=reservation.reservation_id,
                    attempt_id=attempt_id,
                    candidate_exposure=exposure,
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    error=normalized_error,
                    latency_ms=latency_ms,
                )
                await self._circuit.record_failure(
                    candidate.provider.metadata.name,
                    candidate.model,
                )
                logger.warning(
                    "provider_adapter_unexpected_failure",
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    error_type=type(exc).__name__,
                    trace_id=request.trace_id,
                )
                last_error = normalized_error
                continue

            latency_ms = int((time.perf_counter() - attempt_started_at) * 1000)
            provider_latency_ms_total += latency_ms
            result = self._normalize_usage(raw_result, request.messages)
            is_valid = self._validator.is_valid(result.content)

            charged_micros = await self._budget_authorizer.record_attempt_usage(
                reservation_id=reservation.reservation_id,
                provider_attempt_id=attempt_id,
                provider=result.provider,
                model=result.model,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                usage_source=result.usage_source,
                attempt_status="success" if is_valid else "invalid_output",
                latency_ms=latency_ms,
            )
            request_cost_micros += charged_micros

            # The provider completed normally even when the content was unusable.
            await self._circuit.record_success(
                candidate.provider.metadata.name,
                candidate.model,
            )

            if not is_valid:
                logger.warning(
                    "invalid_output_failover",
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    trace_id=request.trace_id,
                )
                last_error = ProviderError(
                    provider=candidate.provider.metadata.name,
                    category="empty_output",
                    message="response failed validation",
                    retryable=True,
                )
                continue

            gateway_overhead_ms = max(
                0,
                int((time.perf_counter() - started_at) * 1000)
                - provider_latency_ms_total,
            )
            await self._budget_authorizer.finalize_reservation(
                reservation_id=reservation.reservation_id,
                final_status="completed",
                gateway_overhead_ms=gateway_overhead_ms,
            )

            request_cost_usd = float(micros_to_decimal(request_cost_micros))
            await self._emit_event(
                event_type="request_completed",
                trace_id=request.trace_id,
                tenant_id=request.tenant_id,
                gateway_request_id=gateway_request_id,
                provider=result.provider,
                model=result.model,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                cost_usd=request_cost_usd,
                usage_source=result.usage_source,
                gateway_overhead_ms=gateway_overhead_ms,
                attempt_count=attempt_number,
                failover_count=max(0, attempt_number - 1),
                outcome="success",
                reconciliation_state="settled",
                final_provider_attempt_id=attempt_id,
                prompt_excerpt=(
                    request.messages[-1].get("content", "") if request.messages else ""
                ),
                response_excerpt=result.content,
            )

            return CompletionResponse(
                gateway_request_id=gateway_request_id,
                content=result.content,
                provider=result.provider,
                model=result.model,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                cost_usd=request_cost_usd,
            )

        overhead_ms = max(
            0,
            int((time.perf_counter() - started_at) * 1000) - provider_latency_ms_total,
        )
        await self._budget_authorizer.finalize_reservation(
            reservation_id=reservation.reservation_id,
            final_status="failed",
            gateway_overhead_ms=overhead_ms,
        )
        await self._emit_event(
            event_type="request_failed",
            trace_id=request.trace_id,
            tenant_id=request.tenant_id,
            gateway_request_id=gateway_request_id,
            provider="none",
            model=request.model,
            input_tokens=0,
            output_tokens=0,
            cost_usd=float(micros_to_decimal(request_cost_micros)),
            usage_source="attempt_aggregate",
            gateway_overhead_ms=overhead_ms,
            attempt_count=attempt_number,
            failover_count=max(0, attempt_number - 1),
            outcome="failed",
            reconciliation_state="settled",
            prompt_excerpt=(
                request.messages[-1].get("content", "") if request.messages else ""
            ),
            response_excerpt="",
            error_category=(last_error.category if last_error else "unavailable"),
        )

        if budget_blocked_fallback:
            raise ProviderError(
                provider="gateway",
                category="invalid_request",
                message="over_budget_for_fallback",
                retryable=False,
            )
        if last_error is not None and not last_error.retryable:
            raise last_error
        raise AllProvidersFailedError(
            "All configured providers are unavailable or returned unusable output"
        )

    async def _record_failed_attempt(
        self,
        *,
        reservation_id: str,
        attempt_id: int,
        candidate_exposure: CandidateExposure,
        provider: str,
        model: str,
        error: ProviderError,
        latency_ms: int,
    ) -> int:
        # Rejected requests normally perform no inference. Timeouts and server
        # failures are financially uncertain, so keep a conservative exposure.
        if error.category in {"invalid_request", "rate_limited"}:
            return await self._budget_authorizer.record_attempt_usage(
                reservation_id=reservation_id,
                provider_attempt_id=attempt_id,
                provider=provider,
                model=model,
                input_tokens=0,
                output_tokens=0,
                usage_source="conservative",
                attempt_status=error.category,
                latency_ms=latency_ms,
            )
        return await self._budget_authorizer.record_conservative_attempt(
            reservation_id=reservation_id,
            provider_attempt_id=attempt_id,
            provider=provider,
            model=model,
            exposure=candidate_exposure,
            attempt_status=error.category,
            latency_ms=latency_ms,
        )

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
        try:
            await self._event_sink.emit(event)
        except Exception as exc:
            logger.warning(
                "completion_event_sink_failed",
                event_type=event_type,
                error_type=type(exc).__name__,
            )

    async def _create_gateway_request(self, request: CompletionRequest) -> int:
        try:
            async with AsyncSessionLocal() as session:
                row = GatewayRequest(
                    tenant_id=request.tenant_id,
                    api_key_id=request.api_key_id,
                    trace_id=request.trace_id,
                    status="pending",
                    is_stream=False,
                )
                session.add(row)
                await session.commit()
                return row.id
        except SQLAlchemyError as exc:
            raise DatabaseUnavailable() from exc

    async def _update_gateway_request_status(
        self,
        gateway_request_id: int,
        status: str,
    ) -> None:
        try:
            async with AsyncSessionLocal() as session:
                row = await session.get(GatewayRequest, gateway_request_id)
                if row is not None:
                    row.status = status
                    await session.commit()
        except SQLAlchemyError as exc:
            raise DatabaseUnavailable() from exc

    async def _start_provider_attempt(
        self,
        gateway_request_id: int,
        provider: str,
        model: str,
        attempt_number: int,
        exposure: CandidateExposure,
    ) -> int:
        try:
            async with AsyncSessionLocal() as session:
                attempt = ProviderAttempt(
                    gateway_request_id=gateway_request_id,
                    provider=provider,
                    model=model,
                    attempt_number=attempt_number,
                    status="in_progress",
                    authorized_cost_micros=exposure.max_cost_micros,
                    estimated_input_tokens=exposure.input_tokens,
                    estimated_output_tokens=exposure.output_cap,
                )
                session.add(attempt)
                await session.commit()
                return attempt.id
        except SQLAlchemyError as exc:
            raise DatabaseUnavailable() from exc
