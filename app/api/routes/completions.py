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
from app.application.use_cases.stream_completion import StreamCompletion, StreamRequest
from starlette.requests import Request

router = APIRouter()


@router.post("/v1/chat/completions")
async def create_completion(
    body: CompletionCreateRequest,
    request: Request,
    principal: Principal = Depends(get_principal),
    use_cases: CompletionUseCases = Depends(get_completion_use_cases),
):
    trace_id = request.scope.get("state", {}).get("trace_id", "unknown")

    if body.stream:
        return _stream_response(body, principal, use_cases.stream, trace_id)

    # --- Non-streaming path (unchanged from Day 7) ---
    try:
        result = await use_cases.execute.execute(CompletionRequest(
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


def _stream_response(
    body: CompletionCreateRequest,
    principal: Principal,
    stream_use_case: StreamCompletion,
    trace_id: str,
) -> StreamingResponse:
    async def generate():
        try:
            async for event in stream_use_case.stream(StreamRequest(
                tenant_id=principal.tenant_id,
                trace_id=trace_id,
                model=body.model,
                messages=[message.model_dump() for message in body.messages],
                max_tokens=body.max_tokens,
            )):
                if event.type == "done":
                    continue # Do not yield [DONE] here!

                payload = {"type": event.type}
                if event.content is not None: payload["content"] = event.content
                if event.input_tokens is not None: payload["input_tokens"] = event.input_tokens
                if event.output_tokens is not None: payload["output_tokens"] = event.output_tokens
                yield f"data: {json.dumps(payload)}\n\n"

        except asyncio.CancelledError:
            # Client disconnected
            raise
        except Exception:
            yield 'data: {"type":"error","content":"internal_stream_error"}\n\n'

        # This runs safely after StreamCompletion has settled the database
        yield "data: [DONE]\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # prevents nginx from buffering SSE
            "X-Trace-ID": trace_id,
        },
    )