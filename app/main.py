from fastapi import FastAPI
from app.api.middleware import TraceIDMiddleware
from app.api.routes import health
from fastapi import Depends
from app.api.deps import get_principal
from app.domain.auth import Principal

app = FastAPI(title="LLM Gateway")
app.add_middleware(TraceIDMiddleware)
app.include_router(health.router)

@app.get("/whoami")
async def whoami(principal: Principal = Depends(get_principal)):
    return {"tenant_id": principal.tenant_id, "api_key_id": principal.api_key_id}