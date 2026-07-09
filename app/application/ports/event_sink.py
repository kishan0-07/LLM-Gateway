from typing import Protocol

class EventSink(Protocol):
    async def emit(self, event: dict) -> None: ...