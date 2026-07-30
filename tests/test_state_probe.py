from __future__ import annotations

import datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import delete

from app.infrastructure.db.models import (
    BudgetReservation,
    GatewayRequest,
    ProviderAttempt,
    UsageLedger,
)
from app.infrastructure.db.session import AsyncSessionLocal
from scripts.chaos.state_probe import assert_mode, snapshot


def _state() -> dict[str, object]:
    return {
        "request": SimpleNamespace(
            id=11,
            status="completed",
            is_stream=False,
            gateway_overhead_ms=4,
        ),
        "reservation": SimpleNamespace(
            status="settled",
            final_status="completed",
            held_micros=0,
            consumed_micros=7,
            reconciliation_state="none",
            reconciliation_reason=None,
        ),
        "attempts": [
            SimpleNamespace(
                id=21,
                attempt_number=1,
                provider="groq",
                model="openai/gpt-oss-20b",
                status="completed",
                latency_ms=12,
                authorized_cost_micros=10,
            )
        ],
        "ledger": [
            SimpleNamespace(
                id=31,
                provider_attempt_id=21,
                provider="groq",
                model="openai/gpt-oss-20b",
                input_tokens=3,
                output_tokens=4,
                usage_source="actual",
                billing_status="known",
                cost_micros=7,
            )
        ],
    }


def _all_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {
            key for nested in value.values() for key in _all_keys(nested)
        }
    if isinstance(value, list):
        return {key for nested in value for key in _all_keys(nested)}
    return set()


def test_inspect_mode_returns_only_allowlisted_state() -> None:
    result = assert_mode(_state(), "inspect")

    assert result["request_id"] == 11
    assert result["attempts"][0]["attempt_id"] == 21
    assert result["ledger"][0]["cost_micros"] == 7
    assert {
        "prompt",
        "messages",
        "response",
        "error",
        "api_key",
        "database_url",
    }.isdisjoint(_all_keys(result))
    assert result["accounting_complete"] is True
    assert result["cost_matches_reservation"] is True


def test_inspect_mode_requires_a_request() -> None:
    with pytest.raises(AssertionError, match="was not found"):
        assert_mode({"request": None}, "inspect")


def test_existing_database_preflight_mode_is_unchanged() -> None:
    assert assert_mode({"request": None}, "database-preflight") == {
        "request": "absent",
        "provider_attempts": 0,
        "ledger_rows": 0,
    }


def test_existing_rate_limit_mode_is_unchanged() -> None:
    result = assert_mode(
        {
            "request": SimpleNamespace(status="rate_limit_unavailable"),
            "reservation": None,
            "attempts": [],
            "ledger": [],
        },
        "rate-limit",
    )

    assert result == {
        "request_status": "rate_limit_unavailable",
        "provider_attempts": 0,
        "ledger_rows": 0,
    }


def test_existing_disconnect_mode_is_unchanged() -> None:
    state = _state()
    request = state["request"]
    reservation = state["reservation"]
    assert isinstance(request, SimpleNamespace)
    assert isinstance(reservation, SimpleNamespace)
    request.status = "cancelled"
    reservation.final_status = "cancelled"

    result = assert_mode(state, "disconnect")

    assert result["request_status"] == "cancelled"
    assert result["held_micros"] == 0
    assert result["ledger_rows"] == 1


@pytest.mark.asyncio
async def test_snapshot_returns_none_for_unknown_trace() -> None:
    assert await snapshot("state-probe-trace-that-does-not-exist") == {"request": None}


@pytest.mark.asyncio
async def test_snapshot_returns_only_records_for_the_exact_request(test_env) -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    period_start = datetime.datetime(
        now.year,
        now.month,
        1,
        tzinfo=datetime.timezone.utc,
    )
    trace_id = "state-probe-exact-records"

    async with AsyncSessionLocal() as session:
        target = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id=trace_id,
            status="completed",
            is_stream=False,
            gateway_overhead_ms=3,
        )
        other = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id="state-probe-other-request",
            status="failed",
            is_stream=True,
        )
        session.add_all([target, other])
        await session.flush()

        reservation = BudgetReservation(
            id="state-probe-target-reservation",
            tenant_id=test_env["tenant_id"],
            gateway_request_id=target.id,
            estimated_tokens=10,
            estimated_cost_usd=Decimal("0.000010"),
            status="settled",
            requested_model="openai/gpt-oss-20b",
            estimated_input_tokens=3,
            estimated_output_tokens=7,
            period_start=period_start,
            held_micros=0,
            consumed_micros=7,
            final_status="completed",
            finalized_at=now,
        )
        other_reservation = BudgetReservation(
            id="state-probe-other-reservation",
            tenant_id=test_env["tenant_id"],
            gateway_request_id=other.id,
            estimated_tokens=10,
            estimated_cost_usd=Decimal("0.000010"),
            status="settled",
            requested_model="openai/gpt-oss-20b",
            estimated_input_tokens=3,
            estimated_output_tokens=7,
            period_start=period_start,
            held_micros=0,
            consumed_micros=9,
            final_status="failed",
            finalized_at=now,
        )
        session.add_all([reservation, other_reservation])
        await session.flush()

        target_attempt = ProviderAttempt(
            gateway_request_id=target.id,
            provider="groq",
            model="openai/gpt-oss-20b",
            attempt_number=1,
            status="completed",
            latency_ms=12,
            authorized_cost_micros=10,
        )
        other_attempt = ProviderAttempt(
            gateway_request_id=other.id,
            provider="openai",
            model="gpt-5-mini",
            attempt_number=1,
            status="failed",
            latency_ms=20,
            authorized_cost_micros=10,
        )
        session.add_all([target_attempt, other_attempt])
        await session.flush()

        target_ledger = UsageLedger(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=target.id,
            reservation_id=reservation.id,
            provider_attempt_id=target_attempt.id,
            provider="groq",
            model="openai/gpt-oss-20b",
            input_tokens=3,
            output_tokens=4,
            cost_usd=Decimal("0.000007"),
            cost_micros=7,
            usage_source="actual",
            billing_status="known",
        )
        other_ledger = UsageLedger(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=other.id,
            reservation_id=other_reservation.id,
            provider_attempt_id=other_attempt.id,
            provider="openai",
            model="gpt-5-mini",
            input_tokens=4,
            output_tokens=5,
            cost_usd=Decimal("0.000009"),
            cost_micros=9,
            usage_source="actual",
            billing_status="known",
        )
        session.add_all([target_ledger, other_ledger])
        await session.commit()

        target_id = target.id
        target_attempt_id = target_attempt.id

    state = await snapshot(trace_id)
    result = assert_mode(state, "inspect")

    assert result["request_id"] == target_id
    assert [item["attempt_id"] for item in result["attempts"]] == [target_attempt_id]
    assert len(result["ledger"]) == 1
    assert result["accounting_complete"] is True
    assert result["cost_matches_reservation"] is True


@pytest.mark.asyncio
async def test_snapshot_rejects_duplicate_trace_ids(test_env) -> None:
    trace_id = "state-probe-duplicate-trace"
    async with AsyncSessionLocal() as session:
        session.add_all(
            [
                GatewayRequest(
                    tenant_id=test_env["tenant_id"],
                    api_key_id=test_env["api_key_id"],
                    trace_id=trace_id,
                    status="completed",
                ),
                GatewayRequest(
                    tenant_id=test_env["tenant_id"],
                    api_key_id=test_env["api_key_id"],
                    trace_id=trace_id,
                    status="failed",
                ),
            ]
        )
        await session.commit()

    with pytest.raises(
        RuntimeError,
        match="trace_id matched 2 gateway requests",
    ):
        await snapshot(trace_id)

    async with AsyncSessionLocal() as session:
        await session.execute(
            delete(GatewayRequest).where(GatewayRequest.trace_id == trace_id)
        )
        await session.commit()
