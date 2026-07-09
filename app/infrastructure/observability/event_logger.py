from app.core.logging import logger

class LogEventSink:
    async def emit(self, event: dict) -> None:
        try:
            logger.info("gateway_event", **event)
        except Exception:
            # Event emit failure must never break the request.
            # Log the failure itself at warning level, then move on.
            logger.warning("event_emit_failed", event_type=event.get("event", "unknown"))