from fastapi import FastAPI
from app.api.middleware import TraceIDMiddleware
from app.api.routes import health

app = FastAPI(title="LLM Gateway")
app.add_middleware(TraceIDMiddleware)
app.include_router(health.router)