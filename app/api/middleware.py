import structlog
from app.core.ids import new_trace_id


class TraceIDMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope["headers"])
        trace_id = headers.get(b"x-trace-id", b"").decode() or new_trace_id()
        scope["state"] = scope.get("state", {})
        scope["state"]["trace_id"] = trace_id
        structlog.contextvars.bind_contextvars(trace_id=trace_id)

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                message.setdefault("headers", []).append(
                    (b"x-trace-id", trace_id.encode())
                )
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            structlog.contextvars.clear_contextvars()
