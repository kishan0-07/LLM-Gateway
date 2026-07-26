from dataclasses import dataclass

from app.application.ports.budget_store import BudgetStore
from app.application.ports.usage_ledger import UsageLedger
from app.application.services import model_catalog
from app.application.services.token_estimator import TokenEstimator
from app.domain.budget import (
    ReservationRequest,
    ReservationResult,
    micros_to_decimal,
    to_micros,
)


@dataclass(frozen=True)
class CandidateExposure:
    input_tokens: int
    output_cap: int
    max_cost_micros: int


class BudgetAuthorizer:
    def __init__(
        self,
        budget_store: BudgetStore,
        usage_ledger: UsageLedger,
        token_estimator: TokenEstimator,
    ):
        self._budget_store = budget_store
        self._usage_ledger = usage_ledger
        self._token_estimator = token_estimator

    def estimate_candidate_exposure(
        self,
        *,
        model: str,
        messages: list[dict],
        requested_max_tokens: int | None,
    ) -> CandidateExposure:
        input_tokens = self._token_estimator.estimate_input_tokens(messages, model)
        output_cap = self._token_estimator.output_cap(
            messages,
            model,
            requested_max_tokens,
        )
        return CandidateExposure(
            input_tokens=input_tokens,
            output_cap=output_cap,
            max_cost_micros=to_micros(
                model_catalog.estimate_cost_usd(
                    model,
                    input_tokens,
                    output_cap,
                )
            ),
        )

    async def authorize(
        self,
        tenant_id: int,
        gateway_request_id: int,
        model: str,
        messages: list[dict],
        requested_max_tokens: int | None,
    ) -> ReservationResult:
        exposure = self.estimate_candidate_exposure(
            model=model,
            messages=messages,
            requested_max_tokens=requested_max_tokens,
        )
        return await self._budget_store.try_reserve(
            ReservationRequest(
                tenant_id=tenant_id,
                gateway_request_id=gateway_request_id,
                requested_model=model,
                estimated_input_tokens=exposure.input_tokens,
                estimated_output_tokens=exposure.output_cap,
                estimated_tokens=exposure.input_tokens + exposure.output_cap,
                estimated_cost_micros=exposure.max_cost_micros,
            )
        )

    async def ensure_attempt_capacity(
        self,
        *,
        reservation_id: str,
        exposure: CandidateExposure,
    ) -> bool:
        return await self._budget_store.ensure_attempt_capacity(
            reservation_id=reservation_id,
            required_micros=exposure.max_cost_micros,
        )

    async def record_attempt_usage(
        self,
        *,
        reservation_id: str,
        provider_attempt_id: int,
        provider: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        usage_source: str,
        attempt_status: str,
        latency_ms: int,
    ) -> int:
        if usage_source not in {"actual", "estimated", "conservative"}:
            raise ValueError(f"unsupported usage source: {usage_source}")
        cost_micros = to_micros(
            model_catalog.estimate_cost_usd(
                model,
                input_tokens,
                output_tokens,
            )
        )
        await self._usage_ledger.record_attempt_usage(
            reservation_id=reservation_id,
            provider_attempt_id=provider_attempt_id,
            provider=provider,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_micros=cost_micros,
            usage_source=usage_source,  # type: ignore[arg-type]
            attempt_status=attempt_status,
            latency_ms=latency_ms,
        )
        return cost_micros

    async def record_conservative_attempt(
        self,
        *,
        reservation_id: str,
        provider_attempt_id: int,
        provider: str,
        model: str,
        exposure: CandidateExposure,
        attempt_status: str,
        latency_ms: int,
    ) -> int:
        await self._usage_ledger.record_attempt_usage(
            reservation_id=reservation_id,
            provider_attempt_id=provider_attempt_id,
            provider=provider,
            model=model,
            input_tokens=exposure.input_tokens,
            output_tokens=exposure.output_cap,
            cost_micros=exposure.max_cost_micros,
            usage_source="conservative",
            attempt_status=attempt_status,
            latency_ms=latency_ms,
        )
        await self.mark_needs_reconciliation(
            reservation_id=reservation_id,
            reason="provider_usage_unavailable",
        )
        return exposure.max_cost_micros

    async def finalize_reservation(
        self,
        *,
        reservation_id: str,
        final_status: str,
        gateway_overhead_ms: int | None = None,
    ) -> None:
        if final_status not in {"completed", "failed", "cancelled"}:
            raise ValueError(f"unsupported final status: {final_status}")
        await self._budget_store.finalize_reservation(
            reservation_id=reservation_id,
            final_status=final_status,  # type: ignore[arg-type]
            gateway_overhead_ms=gateway_overhead_ms,
        )

    async def remaining_usd(self, tenant_id: int) -> float:
        remaining_micros = await self._budget_store.remaining_micros(tenant_id)
        return float(micros_to_decimal(remaining_micros))

    async def mark_needs_reconciliation(
        self,
        *,
        reservation_id: str,
        reason: str,
    ) -> None:
        await self._budget_store.mark_needs_reconciliation(
            reservation_id=reservation_id,
            reason=reason,
        )
