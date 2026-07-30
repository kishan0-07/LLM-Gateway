import asyncio
from contextlib import asynccontextmanager, suppress
from typing import Any, cast

from fastapi import Depends, FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.deps import get_completion_use_cases, get_principal
from app.api.errors import (
    http_exception_handler,
    unhandled_exception_handler,
    validation_exception_handler,
)
from app.api.middleware import TraceIDMiddleware
from app.api.routes import completions, health, stats
from app.core.config import settings
from app.core.logging import logger
from app.domain.auth import Principal
from app.infrastructure.db.postgres_budget_store import PostgreSQLBudgetStore
from app.infrastructure.db.session import close_database
from app.infrastructure.observability.langfuse_sink import shutdown_langfuse
from app.infrastructure.redis.client import close_redis
from app.workers.reservation_reconciler import ReservationReconciler

LANGFUSE_SHUTDOWN_TIMEOUT_SECONDS = 5.0
DEPENDENCY_SHUTDOWN_TIMEOUT_SECONDS = 5.0


@asynccontextmanager
async def lifespan(app: FastAPI):
    reconciler = ReservationReconciler(
        PostgreSQLBudgetStore(),
        interval_seconds=settings.reservation_reconcile_interval_seconds,
    )
    reconciler_task = asyncio.create_task(
        reconciler.run(),
        name="reservation-reconciler",
    )
    app.state.reservation_reconciler = reconciler
    app.state.reservation_reconciler_task = reconciler_task

    logger.info("application_started")
    try:
        yield
    finally:
        logger.info("shutdown_initiated")
        reconciler.stop()

        try:
            await asyncio.wait_for(
                reconciler_task,
                timeout=settings.shutdown_grace_seconds,
            )
        except TimeoutError:
            logger.warning("reconciler_shutdown_timed_out")
            reconciler_task.cancel()
            with suppress(asyncio.CancelledError):
                await reconciler_task

        if get_completion_use_cases.cache_info().currsize:
            use_cases = get_completion_use_cases()
            try:
                await use_cases.stream.drain_finalizers(
                    timeout_seconds=settings.shutdown_grace_seconds,
                )
            except TimeoutError:
                logger.warning("stream_finalizer_shutdown_timed_out")

        try:
            await asyncio.wait_for(
                shutdown_langfuse(),
                timeout=LANGFUSE_SHUTDOWN_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.warning("langfuse_shutdown_timed_out")

        try:
            dependency_results = await asyncio.wait_for(
                asyncio.gather(
                    close_redis(),
                    close_database(),
                    return_exceptions=True,
                ),
                timeout=DEPENDENCY_SHUTDOWN_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.warning("dependency_shutdown_timed_out")
        else:
            for dependency, result in zip(
                ("redis", "database"),
                dependency_results,
                strict=True,
            ):
                if isinstance(result, BaseException):
                    logger.warning(
                        "dependency_shutdown_failed",
                        dependency=dependency,
                        error_type=type(result).__name__,
                    )
        logger.info("shutdown_complete")


app = FastAPI(title="LLM Gateway", lifespan=lifespan)
app.add_middleware(TraceIDMiddleware)
app.include_router(health.router)
app.include_router(completions.router)
app.include_router(stats.router)

app.add_exception_handler(HTTPException, cast(Any, http_exception_handler))
app.add_exception_handler(StarletteHTTPException, cast(Any, http_exception_handler))
app.add_exception_handler(
    RequestValidationError, cast(Any, validation_exception_handler)
)
app.add_exception_handler(Exception, unhandled_exception_handler)


@app.get("/whoami")
async def whoami(principal: Principal = Depends(get_principal)):
    return {
        "tenant_id": principal.tenant_id,
        "api_key_id": principal.api_key_id,
    }
