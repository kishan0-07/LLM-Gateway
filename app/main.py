import asyncio
from contextlib import asynccontextmanager, suppress
from fastapi import Depends, FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
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
from app.api.deps import get_principal
from app.infrastructure.db.session import close_database
from app.infrastructure.redis.budget_store import RedisBudgetStore
from app.infrastructure.redis.client import close_redis
from app.workers.reservation_reconciler import ReservationReconciler

@asynccontextmanager
async def lifespan(app: FastAPI):
    reconciler = ReservationReconciler(
        RedisBudgetStore(),
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

        await close_redis()
        await close_database()
        logger.info("shutdown_complete")

app = FastAPI(title="LLM Gateway", lifespan=lifespan)
app.add_middleware(TraceIDMiddleware)
app.include_router(health.router)
app.include_router(completions.router)
app.include_router(stats.router)

app.add_exception_handler(HTTPException, http_exception_handler)
app.add_exception_handler(StarletteHTTPException, http_exception_handler)
app.add_exception_handler(RequestValidationError, validation_exception_handler)
app.add_exception_handler(Exception, unhandled_exception_handler)

@app.get("/whoami")
async def whoami(principal: Principal = Depends(get_principal)):
    return {
        "tenant_id": principal.tenant_id,
        "api_key_id": principal.api_key_id,
    }