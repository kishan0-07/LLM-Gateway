from __future__ import annotations

import asyncio
from functools import lru_cache

from app.application.ports.event_sink import EventSink
from app.core.config import settings
from app.core.logging import logger


class LangfuseEventSink(EventSink):
    def __init__(self, client):
        self._client = client

    async def emit(self, event: dict) -> None:
        try:
            # Langfuse traces are non-billing, best-effort observability Generation contexts.
            with self._client.start_as_current_observation(
                as_type="generation",
                name=event.get("event", "gateway_request"),
                model=event.get("model"),
            ) as generation:
                generation.update(
                    input={"prompt_excerpt": event.get("prompt_excerpt", "")},
                    output={"response_excerpt": event.get("response_excerpt", "")},
                    metadata={
                        "gatewayTraceId": event.get("trace_id"),
                        "gatewayRequestId": event.get("request_id"),
                        "tenantId": event.get("tenant_id"),
                        "provider": event.get("provider"),
                        "usageSource": event.get("usage_source"),
                    },
                    usage_details={
                        "input": event.get("input_tokens", 0),
                        "output": event.get("output_tokens", 0),
                    },
                    cost_details={"total": float(event.get("cost_usd", 0.0))},
                )
        except Exception as exc:
            logger.warning(
                "langfuse_event_emit_failed",
                event_type=event.get("event", "unknown"),
                error_type=type(exc).__name__,
            )


@lru_cache
def get_langfuse_client():
    if (
        not settings.langfuse_enabled
        or settings.langfuse_public_key is None
        or settings.langfuse_secret_key is None
    ):
        return None

    from langfuse import Langfuse

    return Langfuse(
        public_key=settings.langfuse_public_key.get_secret_value(),
        secret_key=settings.langfuse_secret_key.get_secret_value(),
        base_url=settings.langfuse_base_url,
    )


async def shutdown_langfuse() -> None:
    client = get_langfuse_client()
    if client is None:
        return
    try:
        await asyncio.to_thread(client.shutdown)
    except Exception as exc:
        logger.warning("langfuse_shutdown_failed", error_type=type(exc).__name__)
