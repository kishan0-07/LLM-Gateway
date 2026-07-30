from app.core.logging import logger


class LogEventSink:
    async def emit(self, event: dict) -> None:
        try:
            payload = dict(event)

            event_name = payload.pop("event", "gateway_event")
            # Application logs are metadata-only. Even redacted excerpts can
            # retain unexpected secrets or personal data, so content is sent
            # only to explicitly enabled observability adapters.
            payload.pop("prompt_excerpt", None)
            payload.pop("response_excerpt", None)

            logger.info(event_name, **payload)

        except Exception:
            logger.warning(
                "event_emit_failed", event_type=event.get("event", "unknown")
            )
