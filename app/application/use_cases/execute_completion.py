from dataclasses import dataclass
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.models import GatewayRequest
from app.application.services.budget_authorizer import BudgetAuthorizer
from app.application.ports.provider_client import ProviderClient
from app.application.services import model_catalog
from app.domain.provider import ProviderResult, ProviderError


@dataclass(frozen=True)
class CompletionRequest:
    tenant_id: int
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


class ExecuteCompletion:
    def __init__(self, budget_authorizer: BudgetAuthorizer, provider: ProviderClient):
        self._budget_authorizer = budget_authorizer
        self._provider = provider

    async def execute(self, request: CompletionRequest) -> CompletionResponse:
        gateway_request_id = await self._create_gateway_request(request)

        reservation = await self._budget_authorizer.authorize(
            tenant_id=request.tenant_id, gateway_request_id=gateway_request_id,
            model=request.model, messages=request.messages, requested_max_tokens=request.max_tokens,
        )
        if not reservation.approved:
            raise ProviderError(provider="gateway", category="invalid_request",
                                 message=reservation.reason or "over budget", retryable=False)

        try:
            result: ProviderResult = await self._provider.complete(request.model, request.messages)
        except ProviderError:
            # stopgap only — NOT the real finalizer (Days 8-9). Settles 0/0 so the reservation
            # doesn't leak as "reserved" forever; doesn't yet handle partial usage or disconnect.
            await self._budget_authorizer.settle(
                reservation_id=reservation.reservation_id, provider=self._provider.metadata.name,
                model=request.model, input_tokens=0, output_tokens=0, status="error",
            )
            raise

        await self._budget_authorizer.settle(
            reservation_id=reservation.reservation_id, provider=result.provider, model=result.model,
            input_tokens=result.input_tokens, output_tokens=result.output_tokens, status="success",
        )

        return CompletionResponse(
            gateway_request_id=gateway_request_id, content=result.content,
            provider=result.provider, model=result.model,
            input_tokens=result.input_tokens, output_tokens=result.output_tokens,
            cost_usd=model_catalog.estimate_cost_usd(result.model, result.input_tokens, result.output_tokens),
        )

    async def _create_gateway_request(self, request: CompletionRequest) -> int:
        async with AsyncSessionLocal() as session:
            row = GatewayRequest(tenant_id=request.tenant_id, trace_id=request.trace_id, status="pending")
            session.add(row)
            await session.commit()
            return row.id  # populated by flush's RETURNING, expire_on_commit=False keeps it readable — no refresh() needed