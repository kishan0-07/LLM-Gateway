import asyncio
import time
from dataclasses import dataclass
from typing import AsyncIterator

from sqlalchemy.exc import SQLAlchemyError

from app.application.ports.budget_store import DatabaseUnavailable
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
from app.application.services.routing_engine import RouteCandidate, RoutingEngine
from app.application.services.sanitizer import sanitize
from app.application.services.token_estimator import TokenEstimator
from app.core.logging import logger
from app.domain.budget import micros_to_decimal
from app.domain.provider import ProviderError, ProviderStreamEvent
from app.infrastructure.db.models import GatewayRequest, ProviderAttempt
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.redis.circuit_breaker import CircuitBreaker


DEFAULT_STREAM_IDLE_TIMEOUT_SECONDS = 20.0
DEFAULT_STREAM_TOTAL_TIMEOUT_SECONDS = 180.0
DEFAULT_FINALIZATION_TIMEOUT_SECONDS = 10.0


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


@dataclass(frozen=True)
class StreamUsage:
    input_tokens: int
    output_tokens: int
    source: str


class StreamFailure(Exception):
    def __init__(
        self,
        public_code: str,
        *,
        retryable: bool,
        circuit_failure: bool = False,
    ) -> None:
        super().__init__(public_code)
        self.public_code = public_code
        self.retryable = retryable
        self.circuit_failure = circuit_failure


async def with_idle_timeout(
    iterator: AsyncIterator[ProviderStreamEvent],
    *,
    timeout_seconds: float,
) -> AsyncIterator[ProviderStreamEvent]:
    async_iterator = iterator.__aiter__()
    while True:
        try:
            event = await asyncio.wait_for(
                anext(async_iterator),
                timeout=timeout_seconds,
            )
        except StopAsyncIteration:
            return
        yield event


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
        stream_timeout_seconds: float | None = None,
        stream_idle_timeout_seconds: float = DEFAULT_STREAM_IDLE_TIMEOUT_SECONDS,
        stream_total_timeout_seconds: float = DEFAULT_STREAM_TOTAL_TIMEOUT_SECONDS,
        finalization_timeout_seconds: float = DEFAULT_FINALIZATION_TIMEOUT_SECONDS,
    ) -> None:
        # Keep stream_timeout_seconds as a compatibility alias for existing callers.
        if stream_timeout_seconds is not None:
            stream_total_timeout_seconds = stream_timeout_seconds
        if (
            min(
                stream_idle_timeout_seconds,
                stream_total_timeout_seconds,
                finalization_timeout_seconds,
            )
            <= 0
        ):
            raise ValueError("stream timeouts must be positive")

        self._budget_authorizer = budget_authorizer
        self._routing_engine = routing_engine
        self._circuit = circuit_breaker
        self._validator = response_validator
        self._rate_limiter = rate_limiter
        self._event_sink = event_sink
        self._token_estimator = token_estimator
        self._stream_idle_timeout_seconds = stream_idle_timeout_seconds
        self._stream_total_timeout_seconds = stream_total_timeout_seconds
        self._finalization_timeout_seconds = finalization_timeout_seconds
        self._finalizer_tasks: set[asyncio.Task[None]] = set()

    async def prepare(self, request: StreamRequest) -> PreparedStream:
        try:
            model_catalog.get(request.model)
            primary_exposure = self._budget_authorizer.estimate_candidate_exposure(
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
        if not candidates:
            await self._budget_authorizer.finalize_reservation(
                reservation_id=reservation.reservation_id,
                final_status="failed",
            )
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
            output_cap=primary_exposure.output_cap,
            candidates=candidates,
        )

    async def stream(
        self,
        prepared: PreparedStream,
    ) -> AsyncIterator[ProviderStreamEvent]:
        request = prepared.request
        attempt_number = 0
        request_cost_micros = 0
        last_attempt_candidate: RouteCandidate | None = None
        last_attempt_id: int | None = None

        for candidate in prepared.candidates:
            if not await self._circuit.is_available(
                candidate.provider.metadata.name,
                candidate.model,
            ):
                continue

            try:
                exposure = self._budget_authorizer.estimate_candidate_exposure(
                    model=candidate.model,
                    messages=request.messages,
                    requested_max_tokens=request.max_tokens,
                )
            except ValueError:
                continue

            if not await self._budget_authorizer.ensure_attempt_capacity(
                reservation_id=prepared.reservation_id,
                exposure=exposure,
            ):
                await self._budget_authorizer.finalize_reservation(
                    reservation_id=prepared.reservation_id,
                    final_status="failed",
                )
                yield ProviderStreamEvent(
                    type="error",
                    content="budget_exceeded_for_fallback",
                )
                return

            attempt_number += 1
            attempt_id = await self._start_provider_attempt(
                gateway_request_id=prepared.gateway_request_id,
                provider=candidate.provider.metadata.name,
                model=candidate.model,
                attempt_number=attempt_number,
                exposure=exposure,
            )
            last_attempt_candidate = candidate
            last_attempt_id = attempt_id

            parts: list[str] = []
            emitted_content = False
            saw_done = False
            actual_input_tokens: int | None = None
            actual_output_tokens: int | None = None
            started_at = time.perf_counter()
            chunk_token_upper_bound = 0
            output_limit_finalization_started = False
            output_limit_task: asyncio.Task[None] | None = None

            try:
                async with asyncio.timeout(self._stream_total_timeout_seconds):
                    provider_events = candidate.provider.stream(
                        candidate.model,
                        request.messages,
                        max_tokens=exposure.output_cap,
                    )
                    async for event in with_idle_timeout(
                        provider_events,
                        timeout_seconds=self._stream_idle_timeout_seconds,
                    ):
                        if event.type == "usage":
                            if (
                                event.input_tokens is None
                                or event.output_tokens is None
                                or event.input_tokens < 0
                                or event.output_tokens < 0
                            ):
                                raise StreamFailure(
                                    "invalid_provider_usage",
                                    retryable=False,
                                    circuit_failure=False,
                                )
                            actual_input_tokens = event.input_tokens
                            actual_output_tokens = event.output_tokens
                            if actual_output_tokens > exposure.output_cap:
                                output_limit_finalization_started = True
                                output_limit_task = self._track_finalizer(
                                    self._finalize_output_limit(
                                        prepared=prepared,
                                        candidate=candidate,
                                        attempt_id=attempt_id,
                                        attempt_count=attempt_number,
                                        prior_request_cost_micros=request_cost_micros,
                                        exposure=exposure,
                                        latency_ms=int(
                                            (time.perf_counter() - started_at) * 1000
                                        ),
                                        provider_events=provider_events,
                                        actual_input_tokens=actual_input_tokens,
                                        actual_output_tokens=actual_output_tokens,
                                    )
                                )
                                try:
                                    async with asyncio.timeout(
                                        self._finalization_timeout_seconds
                                    ):
                                        await asyncio.shield(output_limit_task)
                                except TimeoutError:
                                    logger.error(
                                        "stream_output_limit_finalization_timed_out",
                                        trace_id=request.trace_id,
                                        reservation_id=prepared.reservation_id,
                                    )
                                yield ProviderStreamEvent(
                                    type="error",
                                    content="output_limit_exceeded",
                                )
                                return
                            continue

                        if event.type == "delta":
                            content = event.content or ""
                            if not content:
                                continue

                            delta_upper_bound = (
                                self._token_estimator.count_output_tokens(
                                    text=content,
                                    model=candidate.model,
                                )
                            )
                            candidate_upper_bound = (
                                chunk_token_upper_bound + delta_upper_bound
                            )
                            if candidate_upper_bound < exposure.output_cap:
                                parts.append(content)
                                chunk_token_upper_bound = candidate_upper_bound
                                emitted_content = True
                                yield event
                                continue

                            current_text = "".join(parts)
                            candidate_text = current_text + content
                            bounded_text, _bounded_tokens, limit_reached = (
                                self._token_estimator.truncate_output_text(
                                    text=candidate_text,
                                    model=candidate.model,
                                    max_tokens=exposure.output_cap,
                                )
                            )
                            if not limit_reached:
                                parts.append(content)
                                chunk_token_upper_bound = candidate_upper_bound
                                emitted_content = True
                                yield event
                                continue

                            if not bounded_text.startswith(current_text):
                                raise StreamFailure(
                                    "stream_token_boundary_error",
                                    retryable=False,
                                    circuit_failure=False,
                                )

                            safe_delta = bounded_text[len(current_text) :]
                            parts[:] = [bounded_text]
                            emitted_content = bool(bounded_text)
                            output_limit_finalization_started = True
                            output_limit_task = self._track_finalizer(
                                self._finalize_output_limit(
                                    prepared=prepared,
                                    candidate=candidate,
                                    attempt_id=attempt_id,
                                    attempt_count=attempt_number,
                                    prior_request_cost_micros=request_cost_micros,
                                    exposure=exposure,
                                    latency_ms=int(
                                        (time.perf_counter() - started_at) * 1000
                                    ),
                                    provider_events=provider_events,
                                    actual_input_tokens=actual_input_tokens,
                                    actual_output_tokens=actual_output_tokens,
                                )
                            )
                            if safe_delta:
                                yield ProviderStreamEvent(
                                    type="delta",
                                    content=safe_delta,
                                )

                            try:
                                async with asyncio.timeout(
                                    self._finalization_timeout_seconds
                                ):
                                    await asyncio.shield(output_limit_task)
                            except TimeoutError:
                                logger.error(
                                    "stream_output_limit_finalization_timed_out",
                                    trace_id=request.trace_id,
                                    reservation_id=prepared.reservation_id,
                                )

                            yield ProviderStreamEvent(
                                type="error",
                                content="output_limit_exceeded",
                            )
                            return

                        if event.type == "error":
                            raise self._provider_event_failure(event.content)

                        if event.type == "done":
                            saw_done = True
                            break

                if not saw_done:
                    raise StreamFailure(
                        "stream_terminated_unexpectedly",
                        retryable=True,
                        circuit_failure=True,
                    )

                content = "".join(parts)
                usage = self._normalize_stream_usage(
                    model=candidate.model,
                    messages=request.messages,
                    content=content,
                    actual_input_tokens=actual_input_tokens,
                    actual_output_tokens=actual_output_tokens,
                )
                is_valid = self._validator.is_valid(content)
                latency_ms = int((time.perf_counter() - started_at) * 1000)
                charged_micros = await self._budget_authorizer.record_attempt_usage(
                    reservation_id=prepared.reservation_id,
                    provider_attempt_id=attempt_id,
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                    usage_source=usage.source,
                    attempt_status=("success" if is_valid else "invalid_output"),
                    latency_ms=latency_ms,
                )
                request_cost_micros += charged_micros
                await self._circuit.record_success(
                    candidate.provider.metadata.name,
                    candidate.model,
                )

                if not is_valid:
                    if not emitted_content:
                        continue
                    await self._budget_authorizer.finalize_reservation(
                        reservation_id=prepared.reservation_id,
                        final_status="failed",
                    )
                    yield ProviderStreamEvent(
                        type="error",
                        content="invalid_stream_output",
                    )
                    return

                reconciliation_state = (
                    await self._budget_authorizer.finalize_reservation(
                        reservation_id=prepared.reservation_id,
                        final_status="completed",
                    )
                )
                await self._emit_event(
                    event_type="stream_completed",
                    request=request,
                    gateway_request_id=prepared.gateway_request_id,
                    candidate=candidate,
                    attempt_id=attempt_id,
                    attempt_count=attempt_number,
                    usage=usage,
                    cost_micros=request_cost_micros,
                    response_excerpt=content,
                    outcome="success",
                    reconciliation_state=reconciliation_state,
                )
                yield ProviderStreamEvent(type="done")
                return

            except asyncio.CancelledError:
                latency_ms = int((time.perf_counter() - started_at) * 1000)
                if output_limit_finalization_started and output_limit_task is not None:
                    try:
                        async with asyncio.timeout(self._finalization_timeout_seconds):
                            await asyncio.shield(output_limit_task)
                    except (asyncio.CancelledError, TimeoutError):
                        pass
                    raise
                task = self._track_finalizer(
                    self._finalize_cancelled_stream(
                        prepared=prepared,
                        candidate=candidate,
                        exposure=exposure,
                        attempt_id=attempt_id,
                        content="".join(parts),
                        actual_input_tokens=actual_input_tokens,
                        actual_output_tokens=actual_output_tokens,
                        latency_ms=latency_ms,
                    )
                )
                try:
                    async with asyncio.timeout(self._finalization_timeout_seconds):
                        await asyncio.shield(task)
                except (asyncio.CancelledError, TimeoutError):
                    # The strongly referenced task continues. It marks
                    # reconciliation itself if durable finalization fails.
                    pass
                raise

            except DatabaseUnavailable:
                try:
                    await self._budget_authorizer.mark_needs_reconciliation(
                        reservation_id=prepared.reservation_id,
                        reason="stream_durable_finalization_failed",
                    )
                except Exception:
                    logger.error(
                        "stream_reconciliation_marker_failed",
                        reservation_id=prepared.reservation_id,
                    )
                raise
            except TimeoutError:
                failure = StreamFailure(
                    "stream_timeout",
                    retryable=True,
                    circuit_failure=True,
                )
            except StreamFailure as exc:
                failure = exc
            except Exception as exc:
                logger.warning(
                    "stream_provider_exception",
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    error_type=type(exc).__name__,
                    trace_id=request.trace_id,
                )
                failure = StreamFailure(
                    "provider_stream_failed",
                    retryable=True,
                    circuit_failure=True,
                )

            latency_ms = int((time.perf_counter() - started_at) * 1000)
            charged_micros = await self._record_failed_stream_attempt(
                reservation_id=prepared.reservation_id,
                attempt_id=attempt_id,
                candidate=candidate,
                exposure=exposure,
                messages=request.messages,
                content="".join(parts),
                actual_input_tokens=actual_input_tokens,
                actual_output_tokens=actual_output_tokens,
                latency_ms=latency_ms,
                failure=failure,
            )
            request_cost_micros += charged_micros

            if failure.circuit_failure:
                await self._circuit.record_failure(
                    candidate.provider.metadata.name,
                    candidate.model,
                )

            if not emitted_content and failure.retryable:
                continue

            await self._budget_authorizer.finalize_reservation(
                reservation_id=prepared.reservation_id,
                final_status="failed",
            )
            yield ProviderStreamEvent(
                type="error",
                content=failure.public_code,
            )
            return

        reconciliation_state = await self._budget_authorizer.finalize_reservation(
            reservation_id=prepared.reservation_id,
            final_status="failed",
        )
        await self._emit_event(
            event_type="stream_failed",
            request=request,
            gateway_request_id=prepared.gateway_request_id,
            candidate=last_attempt_candidate,
            attempt_id=last_attempt_id,
            attempt_count=attempt_number,
            usage=StreamUsage(0, 0, "attempt_aggregate"),
            cost_micros=request_cost_micros,
            response_excerpt="",
            outcome="failed",
            reconciliation_state=reconciliation_state,
        )
        yield ProviderStreamEvent(
            type="error",
            content="all_providers_unavailable",
        )

    def _provider_event_failure(self, category: str | None) -> StreamFailure:
        normalized = category or "provider_stream_failed"
        if normalized == "rate_limited":
            return StreamFailure(
                "provider_stream_failed",
                retryable=True,
                circuit_failure=True,
            )
        if normalized in {"timeout", "server_error"}:
            return StreamFailure(
                "provider_stream_failed",
                retryable=True,
                circuit_failure=True,
            )
        if normalized == "invalid_request":
            return StreamFailure(
                "provider_stream_failed",
                retryable=False,
            )
        return StreamFailure(
            "provider_stream_failed",
            retryable=True,
            circuit_failure=True,
        )

    def _normalize_stream_usage(
        self,
        *,
        model: str,
        messages: list[dict],
        content: str,
        actual_input_tokens: int | None,
        actual_output_tokens: int | None,
    ) -> StreamUsage:
        if actual_input_tokens is not None and actual_output_tokens is not None:
            return StreamUsage(
                input_tokens=actual_input_tokens,
                output_tokens=actual_output_tokens,
                source="actual",
            )
        return StreamUsage(
            input_tokens=self._token_estimator.estimate_input_tokens(
                messages,
                model,
            ),
            output_tokens=self._token_estimator.estimate_output_tokens_for_text(
                text=content,
                model=model,
            ),
            source="estimated",
        )

    async def _record_failed_stream_attempt(
        self,
        *,
        reservation_id: str,
        attempt_id: int,
        candidate: RouteCandidate,
        exposure: CandidateExposure,
        messages: list[dict],
        content: str,
        actual_input_tokens: int | None,
        actual_output_tokens: int | None,
        latency_ms: int,
        failure: StreamFailure,
    ) -> int:
        if actual_input_tokens is not None and actual_output_tokens is not None:
            return await self._budget_authorizer.record_attempt_usage(
                reservation_id=reservation_id,
                provider_attempt_id=attempt_id,
                provider=candidate.provider.metadata.name,
                model=candidate.model,
                input_tokens=actual_input_tokens,
                output_tokens=actual_output_tokens,
                usage_source="actual",
                attempt_status=failure.public_code,
                latency_ms=latency_ms,
            )
        if content:
            # The visible partial text is a lower bound. Preserve the complete
            # candidate exposure conservatively because hidden provider work
            # may still be billed.
            return await self._budget_authorizer.record_conservative_attempt(
                reservation_id=reservation_id,
                provider_attempt_id=attempt_id,
                provider=candidate.provider.metadata.name,
                model=candidate.model,
                exposure=exposure,
                attempt_status=failure.public_code,
                latency_ms=latency_ms,
            )
        if not failure.circuit_failure:
            return await self._budget_authorizer.record_attempt_usage(
                reservation_id=reservation_id,
                provider_attempt_id=attempt_id,
                provider=candidate.provider.metadata.name,
                model=candidate.model,
                input_tokens=0,
                output_tokens=0,
                usage_source="conservative",
                attempt_status=failure.public_code,
                latency_ms=latency_ms,
            )
        return await self._budget_authorizer.record_conservative_attempt(
            reservation_id=reservation_id,
            provider_attempt_id=attempt_id,
            provider=candidate.provider.metadata.name,
            model=candidate.model,
            exposure=exposure,
            attempt_status=failure.public_code,
            latency_ms=latency_ms,
        )

    async def _finalize_cancelled_stream(
        self,
        *,
        prepared: PreparedStream,
        candidate: RouteCandidate,
        exposure: CandidateExposure,
        attempt_id: int,
        content: str,
        actual_input_tokens: int | None,
        actual_output_tokens: int | None,
        latency_ms: int,
    ) -> None:
        try:
            if actual_input_tokens is not None and actual_output_tokens is not None:
                await self._budget_authorizer.record_attempt_usage(
                    reservation_id=prepared.reservation_id,
                    provider_attempt_id=attempt_id,
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    input_tokens=actual_input_tokens,
                    output_tokens=actual_output_tokens,
                    usage_source="actual",
                    attempt_status="cancelled",
                    latency_ms=latency_ms,
                )
            else:
                await self._budget_authorizer.record_conservative_attempt(
                    reservation_id=prepared.reservation_id,
                    provider_attempt_id=attempt_id,
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    exposure=exposure,
                    attempt_status="cancelled",
                    latency_ms=latency_ms,
                )
            await self._budget_authorizer.finalize_reservation(
                reservation_id=prepared.reservation_id,
                final_status="cancelled",
            )
        except Exception as exc:
            logger.error(
                "cancelled_stream_finalization_failed",
                reservation_id=prepared.reservation_id,
                error_type=type(exc).__name__,
            )
            try:
                await self._budget_authorizer.mark_needs_reconciliation(
                    reservation_id=prepared.reservation_id,
                    reason="cancelled_stream_finalization_failed",
                )
            except Exception:
                logger.error(
                    "cancelled_stream_reconciliation_marker_failed",
                    reservation_id=prepared.reservation_id,
                )

    async def _close_provider_iterator(
        self,
        iterator: AsyncIterator[ProviderStreamEvent],
        *,
        trace_id: str,
        provider: str,
        model: str,
    ) -> None:
        """Close a provider stream best-effort without blocking accounting."""
        close = getattr(iterator, "aclose", None)
        if close is None:
            return
        try:
            async with asyncio.timeout(1.0):
                await close()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "provider_stream_close_failed",
                trace_id=trace_id,
                provider=provider,
                model=model,
                error_type=type(exc).__name__,
            )

    async def _finalize_output_limit(
        self,
        *,
        prepared: PreparedStream,
        candidate: RouteCandidate,
        attempt_id: int,
        attempt_count: int,
        prior_request_cost_micros: int,
        exposure: CandidateExposure,
        latency_ms: int,
        provider_events: AsyncIterator[ProviderStreamEvent],
        actual_input_tokens: int | None,
        actual_output_tokens: int | None,
    ) -> None:
        """Account an output-limit violation without clamping actual usage."""
        request = prepared.request
        charged_micros = 0
        usage = StreamUsage(
            (
                actual_input_tokens
                if actual_input_tokens is not None
                else exposure.input_tokens
            ),
            (
                actual_output_tokens
                if actual_output_tokens is not None
                else exposure.output_cap
            ),
            (
                "actual"
                if actual_input_tokens is not None and actual_output_tokens is not None
                else "conservative"
            ),
        )

        reconciliation_state: str | None = None
        try:
            if actual_input_tokens is not None and actual_output_tokens is not None:
                charged_micros = await self._budget_authorizer.record_attempt_usage(
                    reservation_id=prepared.reservation_id,
                    provider_attempt_id=attempt_id,
                    provider=candidate.provider.metadata.name,
                    model=candidate.model,
                    input_tokens=actual_input_tokens,
                    output_tokens=actual_output_tokens,
                    usage_source="actual",
                    attempt_status="output_limit_exceeded",
                    latency_ms=latency_ms,
                )
            else:
                charged_micros = (
                    await self._budget_authorizer.record_conservative_attempt(
                        reservation_id=prepared.reservation_id,
                        provider_attempt_id=attempt_id,
                        provider=candidate.provider.metadata.name,
                        model=candidate.model,
                        exposure=exposure,
                        attempt_status="output_limit_exceeded",
                        latency_ms=latency_ms,
                    )
                )

            reconciliation_state = await self._budget_authorizer.finalize_reservation(
                reservation_id=prepared.reservation_id,
                final_status="failed",
            )
        except Exception as exc:
            logger.error(
                "output_limit_finalization_failed",
                reservation_id=prepared.reservation_id,
                error_type=type(exc).__name__,
            )
            try:
                await self._budget_authorizer.mark_needs_reconciliation(
                    reservation_id=prepared.reservation_id,
                    reason="output_limit_finalization_failed",
                )
            except Exception:
                logger.error(
                    "output_limit_reconciliation_marker_failed",
                    reservation_id=prepared.reservation_id,
                )
        finally:
            await self._close_provider_iterator(
                provider_events,
                trace_id=request.trace_id,
                provider=candidate.provider.metadata.name,
                model=candidate.model,
            )

        await self._circuit.record_success(
            candidate.provider.metadata.name,
            candidate.model,
        )
        await self._emit_event(
            event_type="stream_output_limit_exceeded",
            request=request,
            gateway_request_id=prepared.gateway_request_id,
            candidate=candidate,
            attempt_id=attempt_id,
            attempt_count=attempt_count,
            usage=usage,
            cost_micros=prior_request_cost_micros + charged_micros,
            response_excerpt="",
            outcome="output_limit_exceeded",
            reconciliation_state=reconciliation_state,
        )
        logger.error(
            "stream_output_limit_exceeded",
            trace_id=request.trace_id,
            provider=candidate.provider.metadata.name,
            model=candidate.model,
            provider_attempt_id=attempt_id,
            authorized_output_cap=exposure.output_cap,
        )

    def _track_finalizer(self, coroutine) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine)
        self._finalizer_tasks.add(task)
        task.add_done_callback(self._finalizer_tasks.discard)
        return task

    async def drain_finalizers(self, timeout_seconds: float = 15.0) -> None:
        if not self._finalizer_tasks:
            return
        async with asyncio.timeout(timeout_seconds):
            await asyncio.gather(
                *tuple(self._finalizer_tasks),
                return_exceptions=True,
            )

    async def _emit_event(
        self,
        *,
        event_type: str,
        request: StreamRequest,
        gateway_request_id: int,
        candidate: RouteCandidate | None,
        attempt_id: int | None,
        attempt_count: int,
        usage: StreamUsage,
        cost_micros: int,
        response_excerpt: str,
        outcome: str,
        reconciliation_state: str | None = None,
    ) -> None:
        event = {
            "event": event_type,
            "trace_id": request.trace_id,
            "tenant_id": request.tenant_id,
            "request_id": gateway_request_id,
            "provider": (
                candidate.provider.metadata.name if candidate is not None else "none"
            ),
            "model": candidate.model if candidate is not None else request.model,
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
            "cost_usd": f"{micros_to_decimal(cost_micros):.6f}",
            "usage_source": usage.source,
            "attempt_count": attempt_count,
            "failover_count": max(0, attempt_count - 1),
            "outcome": outcome,
            "reconciliation_state": reconciliation_state,
            "final_provider_attempt_id": attempt_id,
            "prompt_excerpt": sanitize(
                request.messages[-1].get("content", "") if request.messages else ""
            ),
            "response_excerpt": sanitize(response_excerpt),
        }
        try:
            await self._event_sink.emit(event)
        except Exception as exc:
            logger.warning(
                "stream_event_sink_failed",
                event_type=event_type,
                error_type=type(exc).__name__,
            )

    async def _create_gateway_request(self, request: StreamRequest) -> int:
        try:
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
