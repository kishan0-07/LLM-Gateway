from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.logging import logger


_STATUS_DEFAULTS: dict[int, tuple[str, str]] = {
    400: ("invalid_request", "Invalid request"),
    401: ("authentication_failed", "Authentication failed"),
    403: ("forbidden", "Forbidden"),
    404: ("not_found", "Resource not found"),
    405: ("method_not_allowed", "Method not allowed"),
    409: ("conflict", "Request conflicts with current state"),
    422: ("validation_error", "Request validation failed"),
    429: ("rate_limited", "Rate limit exceeded"),
    500: ("internal_error", "Internal server error"),
    502: ("provider_unavailable", "Provider request failed"),
    503: ("service_unavailable", "Service temporarily unavailable"),
}


def _trace_id(request: Request) -> str:
    state = request.scope.get("state", {})
    return str(state.get("trace_id", "unknown"))


def _error_response(
    request: Request,
    *,
    status_code: int,
    code: str,
    message: str,
    details: list[dict[str, Any]] | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        headers=dict(headers) if headers else None,
        content={
            "error": {
                "code": code,
                "message": message,
                "trace_id": _trace_id(request),
                "details": details,
            }
        },
    )


def _http_detail(
    status_code: int,
    detail: Any,
) -> tuple[str, str]:
    default_code, default_message = _STATUS_DEFAULTS.get(
        status_code,
        ("http_error", "Request failed"),
    )
    if isinstance(detail, Mapping):
        code = str(detail.get("code", default_code))
        message = str(detail.get("message", default_message))
        return code, message
    if isinstance(detail, str):
        return default_code, detail
    return default_code, default_message


async def http_exception_handler(
    request: Request,
    exc: HTTPException | StarletteHTTPException,
) -> JSONResponse:
    code, message = _http_detail(exc.status_code, exc.detail)
    return _error_response(
        request,
        status_code=exc.status_code,
        code=code,
        message=message,
        headers=exc.headers,
    )


async def validation_exception_handler(
    request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    details = [
        {
            "loc": [str(part) for part in error["loc"]],
            "message": error["msg"],
            "type": error["type"],
        }
        for error in exc.errors()
    ]
    return _error_response(
        request,
        status_code=422,
        code="validation_error",
        message="Request validation failed",
        details=details,
    )


async def unhandled_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    logger.exception(
        "unhandled_request_exception",
        trace_id=_trace_id(request),
        exception_type=type(exc).__name__,
    )
    return _error_response(
        request,
        status_code=500,
        code="internal_error",
        message="Internal server error",
    )
