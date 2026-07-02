from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.core.logging import configure_logging, logger
from app.core.middleware import TraceIDMiddleware

configure_logging()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("app_starting")
    yield
    logger.info("app_stopping")

app = FastAPI(title="LLM Gateway", lifespan=lifespan)
app.add_middleware(TraceIDMiddleware)

@app.get("/health")
async def health():
    logger.info("health_check_hit")
    return {"status": "ok"}