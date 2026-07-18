from app.application.ports.event_sink import EventSink
from app.core.logging import logger


class CompositeEventSink(EventSink):
    def __init__(self, *sinks: EventSink):
        self._sinks = sinks

    async def emit(self, event: dict) -> None:
        for sink in self._sinks:
            try:
                await sink.emit(event)
            except Exception as exc:
                logger.warning(
                    "secondary_event_sink_failed",
                    sink_type=type(sink).__name__,
                    event_type=event.get("event", "unknown"),
                    error_type=type(exc).__name__,
                )