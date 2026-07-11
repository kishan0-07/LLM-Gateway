from app.core.logging import logger

class LogEventSink:
    async def emit(self, event: dict) -> None:
        try:
            payload = dict(event)
        
            event_name = payload.pop("event", "gateway_event")
            
            logger.info(event_name, **payload)
            
        except Exception:
            
            logger.warning("event_emit_failed", event_type=event.get("event", "unknown"))