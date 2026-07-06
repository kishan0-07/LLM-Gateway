from app.application.services import model_catalog
from app.application.services.token_estimator import TokenEstimator
from app.application.ports.budget_store import BudgetStore
from app.domain.budget import ReservationRequest, ReservationResult


class BudgetAuthorizer:
    def __init__(self, budget_store: BudgetStore, token_estimator: TokenEstimator):
        self._budget_store = budget_store
        self._token_estimator = token_estimator

    async def authorize(self, tenant_id: int, gateway_request_id: int, model: str,messages: list[dict], requested_max_tokens: int | None,) -> ReservationResult:
        input_tokens = self._token_estimator.estimate_input_tokens(messages)
        output_tokens = self._token_estimator.estimate_max_output_tokens(model, requested_max_tokens)
        estimated_cost_usd = model_catalog.estimate_cost_usd(model, input_tokens, output_tokens)

        return await self._budget_store.try_reserve(ReservationRequest(
            tenant_id=tenant_id, gateway_request_id=gateway_request_id,
            estimated_tokens=input_tokens + output_tokens, estimated_cost_usd=estimated_cost_usd,
        ))