from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from load.harness import (
    apply_stream_timing,
    new_trace_id,
    observe_stream,
    public_stream_code,
    valid_nonstream_response,
    validated_base_url,
)


def _normalized_response() -> dict[str, object]:
    return {
        "gateway_request_id": 7,
        "content": "SECRET_MODEL_OUTPUT",
        "provider": "groq",
        "model": "openai/gpt-oss-20b",
        "usage": {
            "input_tokens": 4,
            "output_tokens": 8,
            "cost_usd": "0.000006",
        },
    }


class TickingClock:
    def __init__(self, *values: float) -> None:
        self._values = iter(values)

    def __call__(self) -> float:
        return next(self._values)


def test_stream_with_delta_and_done_reports_ttft_and_e2e() -> None:
    observation = observe_stream(
        [
            b'data: {"type":"delta","content":"first"}',
            b'data: {"type":"delta","content":"second"}',
            b"data: [DONE]",
        ],
        started_at=10.0,
        clock=TickingClock(10.025, 10.150),
    )

    assert observation.success is True
    assert observation.error is None
    assert observation.ttft_ms == pytest.approx(25.0)
    assert observation.e2e_ms == pytest.approx(150.0)
    assert observation.response_bytes > 0


def test_ttft_uses_first_nonempty_delta_only() -> None:
    observation = observe_stream(
        [
            'data: {"type":"delta","content":""}',
            'data: {"type":"delta","content":"first"}',
            'data: {"type":"delta","content":"second"}',
            "data: [DONE]",
        ],
        started_at=5.0,
        clock=TickingClock(5.040, 5.090),
    )

    assert observation.ttft_ms == pytest.approx(40.0)
    assert observation.e2e_ms == pytest.approx(90.0)


def test_stream_error_keeps_only_public_error_code() -> None:
    observation = observe_stream(
        ['data: {"type":"error","content":"provider_unavailable"}'],
        started_at=1.0,
        clock=TickingClock(1.1),
    )

    assert observation.success is False
    assert observation.error == "provider_unavailable"


@pytest.mark.parametrize(
    "line",
    [
        "data: not-json",
        "data: []",
        "data: 3",
    ],
)
def test_malformed_sse_fails_with_a_safe_code(line: str) -> None:
    observation = observe_stream(
        [line],
        started_at=2.0,
        clock=TickingClock(2.1),
    )

    assert observation.success is False
    assert observation.error == "malformed_sse_json"


def test_stream_without_done_fails() -> None:
    observation = observe_stream(
        ['data: {"type":"delta","content":"partial"}'],
        started_at=2.0,
        clock=TickingClock(2.05, 2.2),
    )

    assert observation.success is False
    assert observation.error == "missing_terminal_done"


def test_stream_e2e_replaces_header_only_response_time() -> None:
    response = SimpleNamespace(
        request_meta={"response_time": 3.0, "response_length": 0}
    )
    observation = observe_stream(
        ['data: {"type":"delta","content":"ok"}', "data: [DONE]"],
        started_at=1.0,
        clock=TickingClock(1.020, 1.250),
    )

    apply_stream_timing(response, observation)

    assert response.request_meta["response_time"] == pytest.approx(250.0)
    assert response.request_meta["response_time"] > 3.0
    assert response.request_meta["response_length"] > 0


@pytest.mark.parametrize(
    "value",
    [
        None,
        [],
        {},
        {"gateway_request_id": True},
        {**_normalized_response(), "content": None},
        {**_normalized_response(), "provider": ""},
        {
            **_normalized_response(),
            "usage": {
                "input_tokens": -1,
                "output_tokens": 2,
                "cost_usd": "0.1",
            },
        },
        {
            **_normalized_response(),
            "usage": {
                "input_tokens": 1,
                "output_tokens": 2,
                "cost_usd": "Infinity",
            },
        },
    ],
)
def test_invalid_nonstream_contracts_are_rejected(value: object) -> None:
    assert valid_nonstream_response(value) is False


def test_valid_nonstream_contract_is_accepted_without_logging_content(
    capsys,
) -> None:
    value = _normalized_response()

    assert valid_nonstream_response(value) is True
    assert "SECRET_MODEL_OUTPUT" not in capsys.readouterr().out


@pytest.mark.parametrize(
    "value",
    [
        "ftp://gateway.example",
        "https://user:password@gateway.example",
        "https://gateway.example/private",
        "https://gateway.example?key=secret",
        "gateway.example",
    ],
)
def test_base_url_rejects_unsafe_values_without_echoing_them(value: str) -> None:
    with pytest.raises(RuntimeError) as exc_info:
        validated_base_url(value)

    assert value not in str(exc_info.value)


def test_missing_base_url_has_a_clear_error(monkeypatch) -> None:
    monkeypatch.delenv("GATEWAY_BASE_URL", raising=False)

    with pytest.raises(RuntimeError, match="GATEWAY_BASE_URL is required"):
        validated_base_url()


def test_trace_ids_use_full_unique_uuids() -> None:
    traces = [new_trace_id(stream=index % 2 == 0) for index in range(100)]

    assert len(set(traces)) == 100
    assert all(len(trace.rsplit("-", 1)[-1]) == 32 for trace in traces)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("provider_unavailable", "provider_unavailable"),
        ("output_limit_exceeded", "output_limit_exceeded"),
        ("SECRET detail", "stream_error"),
        ({"message": "SECRET"}, "stream_error"),
        ("", "stream_error"),
    ],
)
def test_public_stream_code_never_exposes_raw_values(
    value: object,
    expected: str,
) -> None:
    result = public_stream_code(value)

    assert result == expected
    assert "SECRET" not in result


def test_harness_helpers_do_not_write_prompts_keys_or_responses(capsys) -> None:
    valid_nonstream_response(_normalized_response())
    observe_stream(
        ['data: {"type":"delta","content":"SECRET_STREAM_OUTPUT"}', "data: [DONE]"],
        started_at=0.0,
        clock=TickingClock(0.01, 0.02),
    )

    captured = json.dumps(
        {
            "public_error": public_stream_code("provider_unavailable"),
            "trace_id": new_trace_id(stream=True),
        }
    )
    output = capsys.readouterr().out + captured

    assert "SECRET_STREAM_OUTPUT" not in output
    assert "SECRET_MODEL_OUTPUT" not in output
    assert "SECRET_API_KEY" not in output
