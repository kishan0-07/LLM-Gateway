from dataclasses import dataclass
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.models import GatewayRequest, ProviderAttempt
from app.application.services.budget_authorizer import BudgetAuthorizer
from app.application.services.routing_engine import RoutingEngine
from app.application.services.response_validator import ResponseValidator
from app.application.services.sanitizer import sanitize
from app.infrastructure.redis.circuit_breaker import CircuitBreaker
from app.application.ports.rate_limiter import (
    RateLimiter,
    RateLimitExceeded,
    RateLimitBackendUnavailable,
)
from app.application.ports.event_sink import EventSink
from app.application.services import model_catalog
from app.application.services.token_estimator import TokenEstimator
from app.domain.provider import ProviderResult, ProviderError
from app.core.logging import logger
from sqlalchemy import update
from app.application.ports.budget_store import (
    BudgetBackendUnavailable,
    DatabaseUnavailable,
)
import time


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
    """Raised when every candidate in the routing plan either failed or was circuit-broken."""

    pass


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

    async def execute(self, request: CompletionRequest) -> CompletionResponse:
        try:
            model_catalog.get(request.model)
        except KeyError as exc:
            raise ProviderError(
                provider="gateway",
                category="invalid_request",
                message=str(exc),
                retryable=False,
            )

        started_at = time.perf_counter()
        provider_latency_ms_total = 0

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
                request.messages, request.model, request.max_tokens
            )
        except ValueError as exc:
            await self._update_gateway_request_status(gateway_request_id, "failed")
            raise ProviderError(
                provider="gateway",
                category="invalid_request",
                message=str(exc),
                retryable=False,
            )

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
                gateway_request_id, "budget_rejected"
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

        last_error: Exception | None = None
        attempt_number = 0

        for candidate in candidates:
            if not await self._circuit.is_available(
                candidate.provider.metadata.name, candidate.model
            ):
                logger.info(
                    "circuit_skipped",
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    trace_id=request.trace_id,
                )
                continue

            attempt_number += 1
            attempt_id = await self._start_provider_attempt(
                gateway_request_id=gateway_request_id,
                provider=candidate.provider.metadata.name,
                model=candidate.model,
                attempt_number=attempt_number,
            )

            start = time.perf_counter()
            try:
                result: ProviderResult = await candidate.provider.complete(
                    candidate.model,
                    request.messages,
                    max_tokens=output_cap,
                )
            except ProviderError as exc:
                latency_ms = int((time.perf_counter() - start) * 1000)
                provider_latency_ms_total += latency_ms
                await self._finish_provider_attempt(
                    attempt_id, status=exc.category, latency_ms=latency_ms
                )

                if exc.category in ("timeout", "rate_limited", "server_error"):
                    await self._circuit.record_failure(
                        candidate.provider.metadata.name, candidate.model
                    )

                logger.warning(
                    "provider_attempt_failed",
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    error=str(exc),
                    trace_id=request.trace_id,
                )
                last_error = exc
                continue

            latency_ms = int((time.perf_counter() - start) * 1000)
            provider_latency_ms_total += latency_ms

            if not self._validator.is_valid(result.content):
                await self._finish_provider_attempt(
                    attempt_id, status="invalid_output", latency_ms=latency_ms
                )
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
                    retryable=False,
                )
                continue

            await self._circuit.record_success(
                candidate.provider.metadata.name, candidate.model
            )
            await self._finish_provider_attempt(
                attempt_id, status="success", latency_ms=latency_ms
            )

            await self._budget_authorizer.settle(
                reservation_id=reservation.reservation_id,
                provider=result.provider,
                model=result.model,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                status="success",
                usage_source=result.usage_source,
            )

            cost_usd = model_catalog.estimate_cost_usd(
                result.model, result.input_tokens, result.output_tokens
            )
            gateway_overhead_ms = max(
                0,
                int((time.perf_counter() - started_at) * 1000)
                - provider_latency_ms_total,
            )
            await self._update_gateway_request_status(
                gateway_request_id,
                "completed",
                gateway_overhead_ms=gateway_overhead_ms,
            )

            await self._emit_event(
                event_type="request_completed",
                trace_id=request.trace_id,
                tenant_id=request.tenant_id,
                gateway_request_id=gateway_request_id,
                provider=result.provider,
                model=result.model,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                cost_usd=cost_usd,
                prompt_excerpt=request.messages[-1].get("content", "")
                if request.messages
                else "",
                response_excerpt=result.content,
            )

            return CompletionResponse(
                gateway_request_id=gateway_request_id,
                content=result.content,
                provider=result.provider,
                model=result.model,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                cost_usd=cost_usd,
            )

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

        await self._emit_event(
            event_type="request_failed",
            trace_id=request.trace_id,
            tenant_id=request.tenant_id,
            gateway_request_id=gateway_request_id,
            provider="none",
            model=request.model,
            input_tokens=0,
            output_tokens=0,
            cost_usd=0.0,
            prompt_excerpt=request.messages[-1].get("content", "")
            if request.messages
            else "",
            response_excerpt="",
            error=str(last_error),
        )

        raise AllProvidersFailedError(
            f"All providers unavailable or returned invalid responses. Last error: {last_error}"
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
        """Build a sanitized event dict and emit through the event sink."""
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

    async def _create_gateway_request(self, request: CompletionRequest) -> int:
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

    async def _update_gateway_request_status(
        self,
        gateway_request_id: int,
        status: str,
        *,
        gateway_overhead_ms: int | None = None,
    ) -> None:
        values: dict[str, object] = {"status": status}
        if gateway_overhead_ms is not None:
            values["gateway_overhead_ms"] = gateway_overhead_ms

        async with AsyncSessionLocal() as session:
            await session.execute(
                update(GatewayRequest)
                .where(GatewayRequest.id == gateway_request_id)
                .values(**values)
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
