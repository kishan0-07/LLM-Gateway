import pytest

from app.core.config import settings
from app.infrastructure.observability.langfuse_sink import (
    LangfuseEventSink,
    get_langfuse_client,
    shutdown_langfuse,
)


class FakeObservation:
    def __init__(self):
        self.updates = []

    def update(self, **kwargs):
        self.updates.append(kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class FakeLangfuseClient:
    def __init__(self, should_fail=False):
        self.should_fail = should_fail
        self.observations = []
        self.shutdown_called = False

    def start_as_current_observation(self, **kwargs):
        if self.should_fail:
            raise RuntimeError("Langfuse API connection timed out")
        obs = FakeObservation()
        self.observations.append((kwargs, obs))
        return obs

    def shutdown(self):
        self.shutdown_called = True


@pytest.mark.asyncio
async def test_langfuse_sink_disabled_returns_none_client():
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(settings, "langfuse_enabled", False)
        # Force cache clear or evaluate fresh
        client = get_langfuse_client()
        assert client is None


@pytest.mark.asyncio
async def test_langfuse_sink_enabled_transfers_only_sanitized_data():
    client = FakeLangfuseClient()
    sink = LangfuseEventSink(client)

    event = {
        "event": "request_completed",
        "model": "gpt-5.4-mini",
        "prompt_excerpt": "Contact [EMAIL] instead of jane@example.com",
        "response_excerpt": "Hello [PHONE]",
        "trace_id": "trace-123",
        "request_id": 456,
        "tenant_id": 789,
        "provider": "openai",
        "usage_source": "actual",
        "input_tokens": 10,
        "output_tokens": 15,
        "cost_usd": "0.000120",
        "gateway_overhead_ms": 12,
        "attempt_count": 1,
        "failover_count": 0,
        "outcome": "success",
        "reconciliation_state": "settled",
        "final_provider_attempt_id": 1001,
        "secret_user_pii_field": "SHOULD_NOT_BE_IN_METADATA",
    }

    await sink.emit(event)

    assert len(client.observations) == 1
    metadata, obs = client.observations[0]
    assert metadata["as_type"] == "generation"
    assert metadata["model"] == "gpt-5.4-mini"

    updates = obs.updates[0]
    assert updates["input"] == {"prompt_excerpt": "Contact [EMAIL] instead of [EMAIL]"}
    assert updates["output"] == {"response_excerpt": "Hello [PHONE]"}
    assert "jane@example.com" not in str(updates)
    assert updates["metadata"]["gatewayTraceId"] == "trace-123"
    assert updates["metadata"]["gatewayRequestId"] == 456
    assert updates["metadata"]["provider"] == "openai"
    assert updates["metadata"]["model"] == "gpt-5.4-mini"
    assert updates["metadata"]["usageSource"] == "actual"
    assert updates["metadata"]["gatewayOverheadMs"] == 12
    assert updates["metadata"]["attemptCount"] == 1
    assert updates["metadata"]["failoverCount"] == 0
    assert updates["metadata"]["outcome"] == "success"
    assert updates["metadata"]["reconciliationState"] == "settled"
    assert updates["metadata"]["finalProviderAttemptId"] == 1001

    # Ensure unallowed metadata fields (like raw PII or unexpected fields) are strictly filtered out
    assert "secret_user_pii_field" not in updates["metadata"]
    assert "tenantId" not in updates["metadata"]
    assert updates["usage_details"] == {"input": 10, "output": 15}
    assert updates["cost_details"] == {"total": 0.000120}


@pytest.mark.asyncio
async def test_langfuse_sink_failure_is_isolated():
    client = FakeLangfuseClient(should_fail=True)
    sink = LangfuseEventSink(client)

    event = {"event": "request_completed"}

    # Sinks should swallow exceptions safely so client completion is not interrupted
    await sink.emit(event)


@pytest.mark.asyncio
async def test_langfuse_shutdown_flushes_once():
    client = FakeLangfuseClient()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.infrastructure.observability.langfuse_sink.get_langfuse_client",
            lambda: client,
        )
        await shutdown_langfuse()
        assert client.shutdown_called
