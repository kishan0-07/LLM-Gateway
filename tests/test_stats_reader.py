from decimal import Decimal

import pytest
from httpx import ASGITransport, AsyncClient

from app.domain.budget import ReservationRequest
from app.infrastructure.db.models import GatewayRequest, ProviderAttempt
from app.infrastructure.db.postgres_budget_store import PostgreSQLBudgetStore
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.stats_reader import SQLAlchemyStatsReader
from app.main import app


@pytest.mark.integration
@pytest.mark.asyncio
async def test_failover_stats_count_one_request_and_all_attempt_usage(test_env):
    async with AsyncSessionLocal() as session:
        request = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id="stats-failover",
            status="pending",
            is_stream=False,
        )
        session.add(request)
        await session.flush()
        first = ProviderAttempt(
            gateway_request_id=request.id,
            provider="groq",
            model="openai/gpt-oss-20b",
            attempt_number=1,
            status="in_progress",
        )
        second = ProviderAttempt(
            gateway_request_id=request.id,
            provider="openai",
            model="gpt-5.4-mini",
            attempt_number=2,
            status="in_progress",
        )
        session.add_all([first, second])
        await session.commit()

    store = PostgreSQLBudgetStore()
    reservation = await store.try_reserve(
        ReservationRequest(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=request.id,
            requested_model="openai/gpt-oss-20b",
            estimated_input_tokens=10,
            estimated_output_tokens=20,
            estimated_tokens=30,
            estimated_cost_micros=1_000,
        )
    )
    assert reservation.reservation_id is not None
    await store.record_attempt_usage(
        reservation_id=reservation.reservation_id,
        provider_attempt_id=first.id,
        provider="groq",
        model="openai/gpt-oss-20b",
        input_tokens=10,
        output_tokens=20,
        cost_micros=300,
        usage_source="conservative",
        attempt_status="timeout",
        latency_ms=30,
    )
    await store.ensure_attempt_capacity(
        reservation_id=reservation.reservation_id,
        required_micros=1_000,
    )
    await store.record_attempt_usage(
        reservation_id=reservation.reservation_id,
        provider_attempt_id=second.id,
        provider="openai",
        model="gpt-5.4-mini",
        input_tokens=11,
        output_tokens=21,
        cost_micros=400,
        usage_source="actual",
        attempt_status="success",
        latency_ms=20,
    )
    await store.finalize_reservation(
        reservation_id=reservation.reservation_id,
        final_status="completed",
        gateway_overhead_ms=7,
    )

    reader = SQLAlchemyStatsReader()
    tenant = await reader.read(tenant_id=test_env["tenant_id"], api_key_id=None)
    key = await reader.read(
        tenant_id=test_env["tenant_id"],
        api_key_id=test_env["api_key_id"],
    )

    for summary in (tenant, key):
        assert summary.total_requests == 1
        assert summary.settled_requests == 1
        assert summary.settled_reservations == 1
        assert summary.provider_attempts == 2
        assert summary.usage_ledger_entries == 2
        assert summary.active_reservations == 0
        assert summary.active_held_micros == 0
        assert summary.reconciliation_needed_reservations == 0
        assert summary.total_input_tokens == 21
        assert summary.total_output_tokens == 41
        assert summary.total_cost_usd == Decimal("0.000700")
        assert summary.failover_count == 1
        assert summary.gateway_overhead.average_ms == 7.0
        assert summary.gateway_overhead.p50_ms == 7.0
        assert summary.gateway_overhead.p95_ms == 7.0
        assert summary.gateway_overhead.p99_ms == 7.0
        assert summary.gateway_overhead.samples == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_empty_tenant_has_zero_counts_and_no_percentiles():
    summary = await SQLAlchemyStatsReader().read(
        tenant_id=2_147_483_647,
        api_key_id=None,
    )

    assert summary.total_requests == 0
    assert summary.settled_reservations == 0
    assert summary.provider_attempts == 0
    assert summary.usage_ledger_entries == 0
    assert summary.active_reservations == 0
    assert summary.active_held_micros == 0
    assert summary.reconciliation_needed_reservations == 0
    assert summary.total_cost_usd == Decimal("0")
    assert summary.gateway_overhead.average_ms is None
    assert summary.gateway_overhead.p50_ms is None
    assert summary.gateway_overhead.p95_ms is None
    assert summary.gateway_overhead.p99_ms is None
    assert summary.gateway_overhead.samples == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reserved_reconciliation_row_is_active_work(test_env):
    async with AsyncSessionLocal() as session:
        request = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id="stats-active-reconciliation",
            status="pending",
            is_stream=True,
        )
        session.add(request)
        await session.commit()
        await session.refresh(request)

    store = PostgreSQLBudgetStore()
    reservation = await store.try_reserve(
        ReservationRequest(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=request.id,
            requested_model="gpt-5.4-mini",
            estimated_input_tokens=10,
            estimated_output_tokens=20,
            estimated_tokens=30,
            estimated_cost_micros=1_000,
        )
    )
    assert reservation.reservation_id is not None
    await store.mark_needs_reconciliation(
        reservation_id=reservation.reservation_id,
        reason="provider_usage_unavailable",
    )

    summary = await SQLAlchemyStatsReader().read(
        tenant_id=test_env["tenant_id"],
        api_key_id=test_env["api_key_id"],
    )

    assert summary.settled_reservations == 0
    assert summary.active_reservations == 1
    assert summary.active_held_micros == 1_000
    assert summary.reconciliation_needed_reservations == 1
    assert summary.usage_ledger_entries == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_settled_reconciliation_row_is_not_an_active_hold(test_env):
    async with AsyncSessionLocal() as session:
        request = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id="stats-settled-reconciliation",
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
            estimated_input_tokens=10,
            estimated_output_tokens=20,
            estimated_tokens=30,
            estimated_cost_micros=1_000,
        )
    )
    assert reservation.reservation_id is not None
    await store.record_attempt_usage(
        reservation_id=reservation.reservation_id,
        provider_attempt_id=attempt.id,
        provider="openai",
        model="gpt-5.4-mini",
        input_tokens=10,
        output_tokens=20,
        cost_micros=500,
        usage_source="conservative",
        attempt_status="output_limit_exceeded",
        latency_ms=5,
    )
    await store.mark_needs_reconciliation(
        reservation_id=reservation.reservation_id,
        reason="provider_usage_unavailable",
    )
    await store.finalize_reservation(
        reservation_id=reservation.reservation_id,
        final_status="failed",
    )

    summary = await SQLAlchemyStatsReader().read(
        tenant_id=test_env["tenant_id"],
        api_key_id=None,
    )

    assert summary.settled_reservations == 1
    assert summary.active_reservations == 0
    assert summary.active_held_micros == 0
    assert summary.reconciliation_needed_reservations == 1
    assert summary.provider_attempts == 1
    assert summary.usage_ledger_entries == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gateway_overhead_percentiles_use_completed_nonstreams(test_env):
    overhead_values = [1, 2, 3, 4]
    async with AsyncSessionLocal() as session:
        session.add_all(
            [
                GatewayRequest(
                    tenant_id=test_env["tenant_id"],
                    api_key_id=test_env["api_key_id"],
                    trace_id=f"stats-percentile-{value}",
                    status="completed",
                    is_stream=False,
                    gateway_overhead_ms=value,
                )
                for value in overhead_values
            ]
        )
        session.add(
            GatewayRequest(
                tenant_id=test_env["tenant_id"],
                api_key_id=test_env["api_key_id"],
                trace_id="stats-percentile-stream-excluded",
                status="completed",
                is_stream=True,
                gateway_overhead_ms=10_000,
            )
        )
        await session.commit()

    summary = await SQLAlchemyStatsReader().read(
        tenant_id=test_env["tenant_id"],
        api_key_id=test_env["api_key_id"],
    )

    assert summary.gateway_overhead.samples == 4
    assert summary.gateway_overhead.average_ms == pytest.approx(2.5)
    assert summary.gateway_overhead.p50_ms == pytest.approx(2.5)
    assert summary.gateway_overhead.p95_ms == pytest.approx(3.85)
    assert summary.gateway_overhead.p99_ms == pytest.approx(3.97)
    assert (
        summary.gateway_overhead.p50_ms
        <= summary.gateway_overhead.p95_ms
        <= summary.gateway_overhead.p99_ms
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stats_routes_serialize_expanded_contract(test_env):
    transport = ASGITransport(app=app)
    headers = {"X-API-Key": test_env["api_key"]}

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        tenant_response = await client.get("/stats", headers=headers)
        key_response = await client.get("/stats/me", headers=headers)

    for response, scope in (
        (tenant_response, "tenant"),
        (key_response, "api_key"),
    ):
        assert response.status_code == 200
        payload = response.json()
        assert payload["scope"] == scope
        assert payload["settled_requests"] == payload["settled_reservations"]
        assert isinstance(payload["provider_attempts"], int)
        assert isinstance(payload["usage_ledger_entries"], int)
        assert isinstance(payload["active_reservations"], int)
        assert isinstance(payload["active_held_micros"], int)
        assert isinstance(
            payload["reconciliation_needed_reservations"],
            int,
        )
        assert payload["total_cost_usd"].count(".") == 1
        assert len(payload["total_cost_usd"].split(".")[1]) == 6
        assert set(payload["gateway_overhead_ms"]) == {
            "average_ms",
            "p50_ms",
            "p95_ms",
            "p99_ms",
            "samples",
        }
