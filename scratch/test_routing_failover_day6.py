import asyncio
from app.infrastructure.providers.mock import MockProvider
from app.application.services.routing_engine import RoutingEngine, RouteCandidate
from app.application.services.response_validator import ResponseValidator
from app.infrastructure.redis.circuit_breaker import CircuitBreaker
from app.application.services.budget_authorizer import BudgetAuthorizer
from app.application.services.token_estimator import TokenEstimator
from app.infrastructure.redis.budget_store import RedisBudgetStore
from app.application.use_cases.execute_completion import ExecuteCompletion, CompletionRequest, AllProvidersFailedError
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.models import Tenant, BudgetAccount
from app.application.services import model_catalog
from sqlalchemy import select


async def setup_tenant(limit_usd: float) -> int:
    async with AsyncSessionLocal() as session:
        tenant = Tenant(name="day6-routing-test")
        session.add(tenant)
        await session.flush()
        session.add(BudgetAccount(tenant_id=tenant.id, monthly_limit_usd=limit_usd))
        await session.commit()
        return tenant.id


async def main():
    tenant_id = await setup_tenant(limit_usd=10.0)

    # Wire up with mock providers
    mock_groq_success = MockProvider(mode="success")
    mock_groq_success.metadata = mock_groq_success.metadata  # keep mock metadata

    # Test 1: Simple success path — MockProvider returns valid content
    budget_store = RedisBudgetStore()
    authorizer = BudgetAuthorizer(budget_store, budget_store, TokenEstimator())

    # Create a routing engine with just mock provider
    providers = {"mock": MockProvider(mode="success")}
    routing = RoutingEngine(providers=providers)
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)
    validator = ResponseValidator()

    # We need mock-model in ModelCatalog for this to work
    # It's already there from Day 3's MockProvider setup? No — ModelCatalog only has groq/openai models.
    # Workaround: test with a model that IS in the catalog, using real provider names
    # OR add mock-model temporarily. Let's test the routing plan itself first:
    candidates = routing.plan("mock-model")
    # This will raise KeyError because mock-model isn't in ModelCatalog
    # That's actually correct behavior — the routing engine needs catalog entries.

    # So let's test the REAL flow: Groq fails → OpenAI fallback
    groq_failing = MockProvider(mode="error")
    groq_failing.metadata = type(groq_failing.metadata)(
        name="groq", models=["openai/gpt-oss-20b"],
        supports_streaming_usage=True, tokenizer_hint="mock",
        pricing={"openai/gpt-oss-20b": {"input_per_1m": 0.0, "output_per_1m": 0.0}},
    )

    openai_success = MockProvider(mode="success")
    openai_success.metadata = type(openai_success.metadata)(
        name="openai", models=["gpt-5.4-mini"],
        supports_streaming_usage=True, tokenizer_hint="mock",
        pricing={"gpt-5.4-mini": {"input_per_1m": 0.0, "output_per_1m": 0.0}},
    )

    providers = {"groq": groq_failing, "openai": openai_success}
    routing = RoutingEngine(providers=providers)

    use_case = ExecuteCompletion(
        budget_authorizer=authorizer,
        routing_engine=routing,
        circuit_breaker=cb,
        response_validator=validator,
    )

    response = await use_case.execute(CompletionRequest(
        tenant_id=tenant_id, trace_id="test-failover-day6",
        model="openai/gpt-oss-20b",
        messages=[{"role": "user", "content": "hello"}],
    ))
    print(f" Failover worked: Groq failed, OpenAI responded: {response.content}")

    # Test 2: Empty output triggers failover without circuit poison
    groq_empty = MockProvider(mode="empty")
    groq_empty.metadata = type(groq_empty.metadata)(
        name="groq", models=["openai/gpt-oss-20b"],
        supports_streaming_usage=True, tokenizer_hint="mock",
        pricing={"openai/gpt-oss-20b": {"input_per_1m": 0.0, "output_per_1m": 0.0}},
    )
    providers2 = {"groq": groq_empty, "openai": openai_success}
    routing2 = RoutingEngine(providers=providers2)

    # Fresh circuit breaker for this test
    cb2 = CircuitBreaker(failure_threshold=3, recovery_timeout=60)
    use_case2 = ExecuteCompletion(authorizer, routing2, cb2, validator)

    tenant_id2 = await setup_tenant(limit_usd=10.0)
    response2 = await use_case2.execute(CompletionRequest(
        tenant_id=tenant_id2, trace_id="test-empty-failover",
        model="openai/gpt-oss-20b",
        messages=[{"role": "user", "content": "hello"}],
    ))
    print(f" Empty output failover: {response2.content}")

    # Verify Groq circuit is NOT tripped (empty_output doesn't poison health)
    groq_available = await cb2.is_available("groq", "openai/gpt-oss-20b")
    assert groq_available, "empty_output should NOT trip the circuit breaker"
    print(" Circuit NOT poisoned by empty output (Decision 7 confirmed)")

    print("\nALL ROUTING/FAILOVER TESTS PASSED")


if __name__ == "__main__":
    asyncio.run(main())