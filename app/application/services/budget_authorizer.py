from decimal import Decimal
from app.application.services import model_catalog
from app.application.services.token_estimator import TokenEstimator
from app.application.ports.budget_store import BudgetStore, BudgetExceededMidStream
from app.application.ports.usage_ledger import UsageLedger
from app.domain.budget import ReservationRequest, ReservationResult


class BudgetAuthorizer:
    def __init__(self, budget_store: BudgetStore, usage_ledger: UsageLedger, token_estimator: TokenEstimator):
        self._budget_store = budget_store
        self._usage_ledger = usage_ledger
        self._token_estimator = token_estimator

    async def authorize(
        self, tenant_id: int, gateway_request_id: int, model: str, messages: list[dict], requested_max_tokens: int | None
    ) -> ReservationResult:
        input_tokens = self._token_estimator.estimate_input_tokens(messages, model)
        output_tokens = self._token_estimator.estimate_max_output_tokens(model, requested_max_tokens)
        estimated_cost_usd = model_catalog.estimate_cost_usd(model, input_tokens, output_tokens)

        return await self._budget_store.try_reserve(ReservationRequest(
            tenant_id=tenant_id, gateway_request_id=gateway_request_id,
            estimated_tokens=input_tokens + output_tokens, estimated_cost_usd=estimated_cost_usd,
        ))

    async def assert_provisional_stream_usage_within_reservation(
        self, *, reservation_id: str, model: str, messages: list[dict], accumulated_text: str
    ) -> None:
        input_tokens = self._token_estimator.estimate_input_tokens(messages, model)
        output_tokens = len(self._token_estimator._get_encoder(model_catalog.get(model).tokenizer_hint).encode(accumulated_text))
        provisional_cost_usd = Decimal(str(model_catalog.estimate_cost_usd(model, input_tokens, output_tokens)))

        approved_estimated_cost_usd = await self._budget_store.reservation_estimated_cost_usd(reservation_id)
        if provisional_cost_usd > approved_estimated_cost_usd:
            raise BudgetExceededMidStream(
                f"Provisional stream cost {provisional_cost_usd} exceeds approved reservation {approved_estimated_cost_usd}"
            )

    async def settle(self, reservation_id: str, provider: str, model: str, input_tokens: int, output_tokens: int, status: str) -> None:
        actual_cost_usd = model_catalog.estimate_cost_usd(model, input_tokens, output_tokens)
        await self._usage_ledger.settle(
            reservation_id=reservation_id, provider=provider, model=model,
            input_tokens=input_tokens, output_tokens=output_tokens,
            actual_cost_usd=actual_cost_usd, status=status,
        )

    async def remaining_usd(self, tenant_id: int) -> float:
        return await self._budget_store.remaining_usd(tenant_id)