import hashlib
from fastapi import Header, HTTPException, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from app.infrastructure.db.session import get_db
from app.infrastructure.db.models import ApiKey
from app.domain.auth import Principal
from app.core.config import settings
from app.core.logging import logger
from dataclasses import dataclass
from app.application.use_cases.execute_completion import ExecuteCompletion
from app.application.use_cases.stream_completion import StreamCompletion

from functools import lru_cache
from app.infrastructure.observability.event_logger import LogEventSink
from app.infrastructure.observability.event_sinks import CompositeEventSink
from app.infrastructure.observability.langfuse_sink import (
    LangfuseEventSink,
    get_langfuse_client,
)


async def get_principal(
    x_api_key: str | None = Header(None, alias="X-API-Key"),
    db: AsyncSession = Depends(get_db),
) -> Principal:
    if x_api_key is None:
        raise HTTPException(
            status_code=401,
            detail={
                "code": "authentication_failed",
                "message": "Missing API key",
            },
        )
    key_hash = hashlib.sha256(x_api_key.encode()).hexdigest()
    try:
        result = await db.execute(
            select(ApiKey).where(ApiKey.key_hash == key_hash, ApiKey.status == "active")
        )
        api_key = result.scalar_one_or_none()
    except SQLAlchemyError as exc:
        logger.warning(
            "principal_lookup_database_unavailable",
            error_type=type(exc).__name__,
        )
        raise HTTPException(
            status_code=503,
            detail={
                "code": "database_unavailable",
                "message": "Database temporarily unavailable",
            },
        ) from exc

    if api_key is None:
        raise HTTPException(
            status_code=401,
            detail={
                "code": "authentication_failed",
                "message": "Invalid API key",
            },
        )

    return Principal(tenant_id=api_key.tenant_id, api_key_id=api_key.id)


@dataclass(frozen=True)
class CompletionUseCases:
    execute: ExecuteCompletion
    stream: StreamCompletion


@lru_cache
def get_event_sink():
    from app.application.ports.event_sink import EventSink
    sinks: list[EventSink] = [LogEventSink()]
    langfuse_client = get_langfuse_client()
    if langfuse_client is not None:
        sinks.append(LangfuseEventSink(langfuse_client))
    return CompositeEventSink(*sinks)


@lru_cache
def get_completion_use_cases() -> CompletionUseCases:
    from app.infrastructure.providers.base import BaseProvider
    from app.infrastructure.providers.groq import GroqProvider
    from app.infrastructure.providers.openai import OpenAIProvider
    from app.infrastructure.redis.budget_store import RedisBudgetStore
    from app.infrastructure.redis.circuit_breaker import CircuitBreaker
    from app.infrastructure.redis.rate_limiter import RedisRateLimiter
    from app.application.services.budget_authorizer import BudgetAuthorizer
    from app.application.services.token_estimator import TokenEstimator
    from app.application.services.routing_engine import RoutingEngine
    from app.application.services.response_validator import ResponseValidator
    from app.application.use_cases.execute_completion import ExecuteCompletion
    from app.application.use_cases.stream_completion import StreamCompletion

    providers: dict[str, BaseProvider] = {}
    if settings.groq_api_key:
        providers["groq"] = GroqProvider(api_key=settings.groq_api_key)
    if settings.openai_api_key:
        providers["openai"] = OpenAIProvider(api_key=settings.openai_api_key)

    budget_store = RedisBudgetStore()
    token_estimator = TokenEstimator()
    budget_authorizer = BudgetAuthorizer(
        budget_store=budget_store,
        usage_ledger=budget_store,
        token_estimator=token_estimator,
    )
    routing = RoutingEngine(providers=providers)
    circuit = CircuitBreaker()
    rate_limiter = RedisRateLimiter()
    event_sink = get_event_sink()
    validator = ResponseValidator()

    return CompletionUseCases(
        execute=ExecuteCompletion(
            budget_authorizer,
            routing,
            circuit,
            validator,
            rate_limiter,
            event_sink,
            token_estimator,
        ),
        stream=StreamCompletion(
            budget_authorizer,
            routing,
            circuit,
            validator,
            rate_limiter,
            event_sink,
            token_estimator,
        ),
    )
