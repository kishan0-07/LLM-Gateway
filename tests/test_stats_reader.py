from decimal import Decimal

import pytest

from app.domain.budget import ReservationRequest
from app.infrastructure.db.models import GatewayRequest, ProviderAttempt
from app.infrastructure.db.postgres_budget_store import PostgreSQLBudgetStore
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.stats_reader import SQLAlchemyStatsReader


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
        assert summary.total_input_tokens == 21
        assert summary.total_output_tokens == 41
        assert summary.total_cost_usd == Decimal("0.000700")
        assert summary.failover_count == 1
        assert summary.gateway_overhead.average_ms == 7.0
        assert summary.gateway_overhead.samples == 1
