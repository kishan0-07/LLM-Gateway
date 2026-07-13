import json ,asyncio
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from app.api.schemas.completion import (
    CompletionCreateRequest, CompletionCreateResponse, UsageResponse,
)
from app.api.deps import get_principal, get_completion_use_cases , CompletionUseCases
from app.domain.auth import Principal
from app.domain.provider import ProviderError
from app.application.use_cases.execute_completion import (
    ExecuteCompletion, CompletionRequest, AllProvidersFailedError,
)
from app.application.use_cases.stream_completion import PreparedStream , StreamCompletion , StreamRequest
from starlette.requests import Request
from app.application.ports.rate_limiter import RateLimitExceeded, RateLimitBackendUnavailable

router = APIRouter()

def _http_error_for_provider_error(exc: ProviderError) -> HTTPException:
    if exc.category != "invalid_request":
        return HTTPException(status_code=502, detail=str(exc))

    if "over budget" in exc.message or "over_budget" in exc.message:
        return HTTPException(status_code=429, detail=exc.message)
    return HTTPException(status_code=400, detail=exc.message)

@router.post("/v1/chat/completions")
async def create_completion(
    body: CompletionCreateRequest,
    request: Request,
    principal: Principal = Depends(get_principal),
    use_cases: CompletionUseCases = Depends(get_completion_use_cases),
):
    trace_id = request.scope.get("state", {}).get("trace_id", "unknown")

    if body.stream:
        return await _prepare_stream_response(body, principal, use_cases.stream, trace_id)

    try:
        result = await use_cases.execute.execute(CompletionRequest(
            tenant_id=principal.tenant_id,
            api_key_id=principal.api_key_id,
            trace_id=trace_id,
            model=body.model,
            messages=[m.model_dump() for m in body.messages],
            max_tokens=body.max_tokens,
        ))
    except RateLimitExceeded as exc:
        raise HTTPException(
            status_code=429,
            detail="rate limit exceeded",
            headers={"Retry-After": str(exc.retry_after_seconds)},
        ) from exc
    except RateLimitBackendUnavailable as exc:
        raise HTTPException(
            status_code=503,
            detail="rate limiter temporarily unavailable",
            headers={"Retry-After": "1"},
        ) from exc
    except ProviderError as exc:
        raise _http_error_for_provider_error(exc) from exc
    except AllProvidersFailedError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    except KeyError as exc:
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


async def _prepare_stream_response(
    body: CompletionCreateRequest,
    principal: Principal,
    stream_use_case: StreamCompletion,
    trace_id: str,
) -> StreamingResponse:
    try:
        prepared = await stream_use_case.prepare(
            StreamRequest(
                tenant_id=principal.tenant_id,
                api_key_id=principal.api_key_id,
                trace_id=trace_id,
                model=body.model,
                messages=[message.model_dump() for message in body.messages],
                max_tokens=body.max_tokens,
            )
        )
    except RateLimitExceeded as exc:
        raise HTTPException(
            status_code=429,
            detail="rate limit exceeded",
            headers={"Retry-After": str(exc.retry_after_seconds)},
        ) from exc
    except RateLimitBackendUnavailable as exc:
        raise HTTPException(
            status_code=503,
            detail="rate limiter temporarily unavailable",
            headers={"Retry-After": "1"},
        ) from exc
    except ProviderError as exc:
        raise _http_error_for_provider_error(exc) from exc

    return _stream_response(stream_use_case, prepared, trace_id)


def _stream_response(
    stream_use_case: StreamCompletion,
    prepared: PreparedStream,
    trace_id: str,
) -> StreamingResponse:
    async def generate():
        try:
            async for event in stream_use_case.stream(prepared):
                if event.type == "done":
                    continue

                payload = {"type": event.type}
                if event.content is not None:
                    payload["content"] = event.content
                if event.input_tokens is not None:
                    payload["input_tokens"] = event.input_tokens
                if event.output_tokens is not None:
                    payload["output_tokens"] = event.output_tokens
                yield f"data: {json.dumps(payload)}\n\n"
        except asyncio.CancelledError:
            raise
        except Exception:
            yield 'data: {"type":"error","content":"internal_stream_error"}\n\n'

        yield "data: [DONE]\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "X-Trace-ID": trace_id,
        },
    )