from unittest.mock import patch

import pytest

from app.infrastructure.observability.event_logger import LogEventSink


@pytest.mark.asyncio
async def test_structured_event_log_drops_prompt_and_response_content() -> None:
    sink = LogEventSink()
    event = {
        "event": "completion_finished",
        "trace_id": "trace-safe-metadata",
        "provider": "mock",
        "prompt_excerpt": "customer-secret@example.com",
        "response_excerpt": "private provider output",
    }

    with patch("app.infrastructure.observability.event_logger.logger.info") as log_info:
        await sink.emit(event)

    log_info.assert_called_once_with(
        "completion_finished",
        trace_id="trace-safe-metadata",
        provider="mock",
    )
