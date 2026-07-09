from fastapi import APIRouter, Depends, HTTPException
from app.api.schemas.completion import CompletionCreateRequest, CompletionCreateResponse, UsageResponse
from app.api.deps import get_principal, get_execute_completion
from app.domain.auth import Principal
from app.domain.provider import ProviderError
from app.application.use_cases.execute_completion import ExecuteCompletion, CompletionRequest, AllProvidersFailedError
from starlette.requests import Request

router = APIRouter()


@router.post("/v1/chat/completions", response_model=CompletionCreateResponse)
async def create_completion(
    body: CompletionCreateRequest,
    request: Request,
    principal: Principal = Depends(get_principal),
    use_case: ExecuteCompletion = Depends(get_execute_completion),
):
    if body.stream:
        raise HTTPException(
            status_code=501,
            detail="Streaming not yet implemented. Landing Days 8-9.",
        )

    trace_id = request.scope.get("state", {}).get("trace_id", "unknown")

    try:
        result = await use_case.execute(CompletionRequest(
            tenant_id=principal.tenant_id,
            trace_id=trace_id,
            model=body.model,
            messages=[m.model_dump() for m in body.messages],
            max_tokens=body.max_tokens,
        ))
    except ProviderError as exc:
        if exc.category == "invalid_request" and "over budget" in exc.message:
            raise HTTPException(status_code=429, detail=exc.message)
        raise HTTPException(status_code=502, detail=str(exc))
    except AllProvidersFailedError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    except KeyError as exc:
        # Model not in catalog
        raise HTTPException(status_code=400, detail=str(exc))

    return CompletionCreateResponse(
        gateway_request_id=result.gateway_request_id,
        content=result.content,
        provider=result.provider,
        model=result.model,
        usage=UsageResponse(
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            cost_usd=f"{result.cost_usd:.6f}",
        ),
    )