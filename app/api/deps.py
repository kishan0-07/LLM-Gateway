import hashlib
from fastapi import Header, HTTPException, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.infrastructure.db.session import get_db
from app.infrastructure.db.models import ApiKey
from app.domain.auth import Principal
from app.core.config import settings


async def get_principal(x_api_key: str | None = Header(None, alias="X-API-Key"),db: AsyncSession = Depends(get_db),) -> Principal:
    if x_api_key is None:
        raise HTTPException(status_code=401, detail="Missing API key")

    key_hash = hashlib.sha256(x_api_key.encode()).hexdigest()
    result = await db.execute(
        select(ApiKey).where(ApiKey.key_hash == key_hash, ApiKey.status == "active")
    )
    api_key = result.scalar_one_or_none()
    if api_key is None:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return Principal(tenant_id=api_key.tenant_id, api_key_id=api_key.id)

def get_execute_completion():
    from app.infrastructure.providers.groq import GroqProvider
    from app.infrastructure.providers.openai import OpenAIProvider
    from app.infrastructure.redis.budget_store import RedisBudgetStore
    from app.infrastructure.redis.circuit_breaker import CircuitBreaker
    from app.infrastructure.redis.rate_limiter import PermissiveRateLimiter
    from app.infrastructure.observability.event_logger import LogEventSink
    from app.application.services.budget_authorizer import BudgetAuthorizer
    from app.application.services.token_estimator import TokenEstimator
    from app.application.services.routing_engine import RoutingEngine
    from app.application.services.response_validator import ResponseValidator
    from app.application.use_cases.execute_completion import ExecuteCompletion

    # Provider instances — they hold async SDK clients that use connection pools internally
    providers = {}
    if settings.groq_api_key:
        providers["groq"] = GroqProvider(api_key=settings.groq_api_key)
    if settings.openai_api_key:
        providers["openai"] = OpenAIProvider(api_key=settings.openai_api_key)

    budget_store = RedisBudgetStore()

    return ExecuteCompletion(
        budget_authorizer=BudgetAuthorizer(
            budget_store=budget_store,
            usage_ledger=budget_store,  # same object, two Protocol roles (Day 5 design)
            token_estimator=TokenEstimator(),
        ),
        routing_engine=RoutingEngine(providers=providers),
        circuit_breaker=CircuitBreaker(),
        response_validator=ResponseValidator(),
        rate_limiter=PermissiveRateLimiter(),
        event_sink=LogEventSink(),
    )

def get_stream_completion():
    """Builds a fully-wired StreamCompletion use case."""
    from app.infrastructure.providers.groq import GroqProvider
    from app.infrastructure.providers.openai import OpenAIProvider
    from app.infrastructure.redis.budget_store import RedisBudgetStore
    from app.infrastructure.redis.circuit_breaker import CircuitBreaker
    from app.infrastructure.redis.rate_limiter import PermissiveRateLimiter
    from app.infrastructure.observability.event_logger import LogEventSink
    from app.application.services.budget_authorizer import BudgetAuthorizer
    from app.application.services.token_estimator import TokenEstimator
    from app.application.services.routing_engine import RoutingEngine
    from app.application.services.response_validator import ResponseValidator
    from app.application.use_cases.stream_completion import StreamCompletion

    providers = {}
    if settings.groq_api_key:
        providers["groq"] = GroqProvider(api_key=settings.groq_api_key)
    if settings.openai_api_key:
        providers["openai"] = OpenAIProvider(api_key=settings.openai_api_key)

    budget_store = RedisBudgetStore()
    token_estimator = TokenEstimator()

    return StreamCompletion(
        budget_authorizer=BudgetAuthorizer(
            budget_store=budget_store,
            usage_ledger=budget_store,
            token_estimator=token_estimator,
        ),
        routing_engine=RoutingEngine(providers=providers),
        circuit_breaker=CircuitBreaker(),
        response_validator=ResponseValidator(),
        rate_limiter=PermissiveRateLimiter(),
        event_sink=LogEventSink(),
        token_estimator=token_estimator,
    )