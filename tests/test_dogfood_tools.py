import json
from pathlib import Path

import httpx
import pytest

from app.domain.budget import ReservationRequest
from app.infrastructure.db.models import GatewayRequest, ProviderAttempt
from app.infrastructure.db.postgres_budget_store import PostgreSQLBudgetStore
from app.infrastructure.db.session import AsyncSessionLocal
from scripts.dogfood.reconcile import (
    SAFE_CLIENT_FIELDS,
    load_result_rows,
    reconcile_trace,
    safe_client_fields,
)
from scripts.dogfood.run import (
    load_cases,
    normalized_nonstream_result,
    run_case,
    validate_application_sha,
    validate_base_url,
)
from scripts.dogfood.summarize import build_summary, metric, percentile


def test_reconciled_output_cannot_copy_content_or_secret_fields():
    client_row = {
        "application_sha": "a" * 40,
        "case_id": "safe-1",
        "trace_id": "trace-1",
        "prompt": "must never be copied",
        "response": "must never be copied",
        "api_key": "secret",
        "headers": {"Authorization": "secret"},
    }

    safe = safe_client_fields(client_row)

    assert set(safe) <= SAFE_CLIENT_FIELDS
    assert safe["application_sha"] == "a" * 40
    assert "prompt" not in safe
    assert "response" not in safe
    assert "api_key" not in safe
    assert "headers" not in safe


@pytest.mark.parametrize(
    "payload",
    [
        "not an object",
        {},
        {
            "gateway_request_id": 1,
            "content": "ok",
            "provider": "openai",
            "model": "gpt-5.4-mini",
            "usage": {
                "input_tokens": -1,
                "output_tokens": 2,
                "cost_usd": "0.1",
            },
        },
        {
            "gateway_request_id": 1,
            "content": "ok",
            "provider": "openai",
            "model": "gpt-5.4-mini",
            "usage": {
                "input_tokens": 1,
                "output_tokens": 2,
                "cost_usd": "NaN",
            },
        },
    ],
)
def test_invalid_nonstream_contract_is_rejected(payload):
    assert normalized_nonstream_result(payload) is None


def test_valid_nonstream_contract_returns_only_noncontent_facts():
    result = normalized_nonstream_result(
        {
            "gateway_request_id": 7,
            "content": "private answer",
            "provider": "openai",
            "model": "gpt-5.4-mini",
            "usage": {
                "input_tokens": 3,
                "output_tokens": 4,
                "cost_usd": "0.000007",
            },
        }
    )

    assert result == {
        "gateway_request_id": 7,
        "provider": "openai",
        "model": "gpt-5.4-mini",
        "input_tokens": 3,
        "output_tokens": 4,
        "cost_usd": "0.000007",
        "response_chars": len("private answer"),
    }
    assert "content" not in result


@pytest.mark.asyncio
async def test_nonstream_runner_result_is_redacted():
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-API-Key"] == "local-secret"
        return httpx.Response(
            200,
            json={
                "gateway_request_id": 11,
                "content": "sensitive response body",
                "provider": "openai",
                "model": "gpt-5.4-mini",
                "usage": {
                    "input_tokens": 3,
                    "output_tokens": 4,
                    "cost_usd": "0.000007",
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await run_case(
            client,
            {
                "id": "case-1",
                "category": "coding",
                "stream": False,
                "prompt": "sensitive prompt",
            },
            base_url="http://gateway.test",
            api_key="local-secret",
            model="gpt-5.4-mini",
            application_sha="a" * 40,
        )

    assert result["terminal_event"] == "success"
    assert result["application_sha"] == "a" * 40
    assert result["response_chars"] == len("sensitive response body")
    serialized = json.dumps(result)
    assert "sensitive prompt" not in serialized
    assert "sensitive response body" not in serialized
    assert "local-secret" not in serialized
    assert set(result) <= SAFE_CLIENT_FIELDS


@pytest.mark.asyncio
async def test_stream_runner_records_ttft_characters_and_done():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text=(
                'data: {"type":"delta","content":"hello"}\n\n'
                'data: {"type":"delta","content":" world"}\n\n'
                "data: [DONE]\n\n"
            ),
            headers={"content-type": "text/event-stream"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await run_case(
            client,
            {
                "id": "stream-1",
                "category": "concept",
                "stream": True,
                "prompt": "private",
            },
            base_url="http://gateway.test",
            api_key="secret",
            model="gpt-5.4-mini",
            application_sha="a" * 40,
        )

    assert result["terminal_event"] == "done"
    assert result["stream_ttft_ms"] is not None
    assert result["response_chars"] == len("hello world")
    assert result["error_code"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (
            'data: {"type":"error","content":"provider_stream_failed"}\n\n',
            "provider_stream_failed",
        ),
        ("data: not-json\n\n", "malformed_sse_event"),
        (
            'data: {"type":"delta","content":"partial"}\n\n',
            ("stream_terminated_without_terminal"),
        ),
    ],
)
async def test_stream_runner_preserves_only_public_terminal_code(body, expected):
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text=body,
            headers={"content-type": "text/event-stream"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await run_case(
            client,
            {
                "id": "stream-error",
                "stream": True,
                "prompt": "private",
            },
            base_url="http://gateway.test",
            api_key="secret",
            model="gpt-5.4-mini",
            application_sha="a" * 40,
        )

    assert result["terminal_event"] == "error"
    assert result["error_code"] == expected


@pytest.mark.parametrize(
    "url",
    [
        "ftp://gateway.test",
        "http://user:password@gateway.test",
        "http://gateway.test/private",
        "http://gateway.test?secret=yes",
        "not a URL",
    ],
)
def test_invalid_or_sensitive_base_url_is_rejected(url):
    with pytest.raises(SystemExit) as exc:
        validate_base_url(url)
    assert url not in str(exc.value)


def test_application_sha_must_be_exact_full_git_sha():
    assert validate_application_sha("A" * 40) == "a" * 40
    with pytest.raises(SystemExit, match="40 hexadecimal"):
        validate_application_sha("abc123")


def test_duplicate_case_and_trace_ids_are_rejected(tmp_path: Path):
    cases_path = tmp_path / "cases.jsonl"
    cases_path.write_text(
        '{"id":"same","prompt":"one"}\n{"id":"same","prompt":"two"}\n',
        encoding="utf-8",
    )
    with pytest.raises(SystemExit, match="duplicate case id"):
        load_cases(cases_path)

    results_path = tmp_path / "results.jsonl"
    results_path.write_text(
        '{"trace_id":"same"}\n{"trace_id":"same"}\n',
        encoding="utf-8",
    )
    with pytest.raises(SystemExit, match="duplicate trace id"):
        load_result_rows(results_path)


def test_percentile_and_empty_metrics_are_deterministic():
    assert percentile([], 0.95) is None
    assert metric(None, " ms") == "not measured"
    assert percentile([1.0, 2.0, 3.0, 4.0], 0.50) == pytest.approx(2.5)
    assert percentile([1.0, 2.0, 3.0, 4.0], 0.95) == pytest.approx(3.85)


def test_summary_contains_aggregates_but_no_content():
    markdown = build_summary(
        [
            {
                "case_id": "one",
                "application_sha": "a" * 40,
                "category": "pii_sentinel",
                "stream": False,
                "http_status": 200,
                "terminal_event": "success",
                "found": True,
                "reservation_status": "settled",
                "reservation_held_micros": 0,
                "reconciliation_state": "none",
                "accounting_complete": True,
                "cost_matches_reservation": True,
                "attempt_count": 1,
                "ledger_count": 1,
                "input_tokens": 2,
                "output_tokens": 3,
                "cost_usd": "0.000004",
                "client_e2e_ms": 10,
                "gateway_overhead_ms": 2,
            }
        ],
        application_sha="a" * 40,
    )

    assert f"Application SHA: `{'a' * 40}`" in markdown
    assert "Total cases: 1" in markdown
    assert "Accounted cost: $0.000004" in markdown
    assert "private prompt value" not in markdown
    assert "private response value" not in markdown


def test_summary_rejects_rows_from_a_different_application_sha():
    with pytest.raises(ValueError, match="requested application SHA"):
        build_summary(
            [{"application_sha": "b" * 40}],
            application_sha="a" * 40,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reconciliation_replaces_client_usage_with_postgres_truth(test_env):
    async with AsyncSessionLocal() as session:
        request = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id="dogfood-reconcile",
            status="pending",
            is_stream=False,
        )
        session.add(request)
        await session.flush()
        attempt = ProviderAttempt(
            gateway_request_id=request.id,
            provider="openai",
            model="gpt-5.4-mini",
            attempt_number=1,
            status="in_progress",
        )
        session.add(attempt)
        await session.commit()
        await session.refresh(request)
        await session.refresh(attempt)

    store = PostgreSQLBudgetStore()
    reservation = await store.try_reserve(
        ReservationRequest(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=request.id,
            requested_model="gpt-5.4-mini",
            estimated_input_tokens=5,
            estimated_output_tokens=7,
            estimated_tokens=12,
            estimated_cost_micros=100,
        )
    )
    assert reservation.reservation_id is not None
    await store.record_attempt_usage(
        reservation_id=reservation.reservation_id,
        provider_attempt_id=attempt.id,
        provider="openai",
        model="gpt-5.4-mini",
        input_tokens=5,
        output_tokens=7,
        cost_micros=90,
        usage_source="actual",
        attempt_status="success",
        latency_ms=8,
    )
    await store.finalize_reservation(
        reservation_id=reservation.reservation_id,
        final_status="completed",
        gateway_overhead_ms=3,
    )

    async with AsyncSessionLocal() as session:
        reconciled = await reconcile_trace(
            session,
            {
                "case_id": "case-1",
                "trace_id": "dogfood-reconcile",
                "gateway_request_id": request.id,
                "client_e2e_ms": 22.5,
                "input_tokens": 999,
                "output_tokens": 999,
                "cost_usd": "999",
                "prompt": "must disappear",
                "response": "must disappear",
            },
        )

    assert reconciled["input_tokens"] == 5
    assert reconciled["output_tokens"] == 7
    assert reconciled["cost_usd"] == "0.000090"
    assert reconciled["client_e2e_ms"] == 22.5
    assert reconciled["accounting_complete"] is True
    assert reconciled["cost_matches_reservation"] is True
    assert "prompt" not in reconciled
    assert "response" not in reconciled


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reconciliation_missing_ambiguous_and_mismatch_are_visible(test_env):
    async with AsyncSessionLocal() as session:
        missing = await reconcile_trace(
            session,
            {"case_id": "missing", "trace_id": "does-not-exist"},
        )
        assert missing["found"] is False

        session.add_all(
            [
                GatewayRequest(
                    tenant_id=test_env["tenant_id"],
                    api_key_id=test_env["api_key_id"],
                    trace_id="duplicate-dogfood-trace",
                    status="failed",
                    is_stream=False,
                ),
                GatewayRequest(
                    tenant_id=test_env["tenant_id"],
                    api_key_id=test_env["api_key_id"],
                    trace_id="duplicate-dogfood-trace",
                    status="failed",
                    is_stream=True,
                ),
                GatewayRequest(
                    tenant_id=test_env["tenant_id"],
                    api_key_id=test_env["api_key_id"],
                    trace_id="request-id-mismatch",
                    status="failed",
                    is_stream=False,
                ),
            ]
        )
        await session.commit()

        with pytest.raises(RuntimeError, match="ambiguous duplicate trace"):
            await reconcile_trace(
                session,
                {"trace_id": "duplicate-dogfood-trace"},
            )
        with pytest.raises(RuntimeError, match="gateway request id mismatch"):
            await reconcile_trace(
                session,
                {
                    "trace_id": "request-id-mismatch",
                    "gateway_request_id": 2_147_483_647,
                },
            )
