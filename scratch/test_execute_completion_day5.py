import asyncio
from sqlalchemy import select
from app.application.use_cases.execute_completion import ExecuteCompletion, CompletionRequest
from app.application.services.budget_authorizer import BudgetAuthorizer
from app.application.services.token_estimator import TokenEstimator
from app.infrastructure.redis.budget_store import RedisBudgetStore
from app.infrastructure.providers.mock import MockProvider
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.models import Tenant, BudgetAccount, GatewayRequest


async def setup_tenant() -> int:
    async with AsyncSessionLocal() as session:
        tenant = Tenant(name="day5-shell-test")
        session.add(tenant)
        await session.flush()
        session.add(BudgetAccount(tenant_id=tenant.id, monthly_limit_usd=10))
        await session.commit()
        return tenant.id


async def main():
    tenant_id = await setup_tenant()
    budget_store = RedisBudgetStore()  # one instance, satisfies both BudgetStore and UsageLedger
    authorizer = BudgetAuthorizer(budget_store=budget_store, usage_ledger=budget_store, token_estimator=TokenEstimator())
    use_case = ExecuteCompletion(budget_authorizer=authorizer, provider=MockProvider(mode="success"))

    response = await use_case.execute(CompletionRequest(
        tenant_id=tenant_id, trace_id="test-trace-day5", model="openai/gpt-oss-20b",
        messages=[{"role": "user", "content": "hello"}],
    ))
    print(response)

    async with AsyncSessionLocal() as session:
        row = (await session.execute(
            select(GatewayRequest).where(GatewayRequest.id == response.gateway_request_id)
        )).scalar_one()
    assert row.tenant_id == tenant_id
    print("GATEWAY_REQUESTS ROW CONFIRMED, STUBBED RESPONSE RETURNED")


if __name__ == "__main__":
    asyncio.run(main())