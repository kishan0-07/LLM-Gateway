from decimal import Decimal

from sqlalchemy import func, select

from app.application.ports.stats_reader import (
    GatewayOverheadSummary,
    UsageStatsSummary,
)
from app.infrastructure.db.models import (
    BudgetReservation,
    GatewayRequest,
    ProviderAttempt,
    UsageLedger,
)
from app.infrastructure.db.session import AsyncSessionLocal


class SQLAlchemyStatsReader:
    async def read(
        self,
        *,
        tenant_id: int,
        api_key_id: int | None,
    ) -> UsageStatsSummary:
        request_filters = [GatewayRequest.tenant_id == tenant_id]
        if api_key_id is not None:
            request_filters.append(GatewayRequest.api_key_id == api_key_id)

        async with AsyncSessionLocal() as session:
            total_requests = await session.scalar(
                select(func.count(GatewayRequest.id)).where(*request_filters)
            )

            reservation_aggregate = (
                await session.execute(
                    select(
                        func.count(BudgetReservation.id)
                        .filter(BudgetReservation.status == "settled")
                        .label("settled_reservations"),
                        func.count(BudgetReservation.id)
                        .filter(BudgetReservation.status == "reserved")
                        .label("active_reservations"),
                        func.coalesce(
                            func.sum(BudgetReservation.held_micros).filter(
                                BudgetReservation.status == "reserved"
                            ),
                            0,
                        ).label("active_held_micros"),
                        func.count(BudgetReservation.id)
                        .filter(
                            BudgetReservation.reconciliation_state
                            == "needs_reconciliation"
                        )
                        .label("reconciliation_needed_reservations"),
                    )
                    .select_from(BudgetReservation)
                    .join(
                        GatewayRequest,
                        GatewayRequest.id == BudgetReservation.gateway_request_id,
                    )
                    .where(*request_filters)
                )
            ).one()

            settled_reservations = int(reservation_aggregate.settled_reservations or 0)
            active_reservations = int(reservation_aggregate.active_reservations or 0)
            active_held_micros = int(reservation_aggregate.active_held_micros or 0)
            reconciliation_needed = int(
                reservation_aggregate.reconciliation_needed_reservations or 0
            )

            provider_attempts = await session.scalar(
                select(func.count(ProviderAttempt.id))
                .join(
                    GatewayRequest,
                    GatewayRequest.id == ProviderAttempt.gateway_request_id,
                )
                .where(*request_filters)
            )

            ledger_aggregate = (
                await session.execute(
                    select(
                        func.count(UsageLedger.id).label("ledger_entries"),
                        func.coalesce(func.sum(UsageLedger.input_tokens), 0).label(
                            "total_input_tokens"
                        ),
                        func.coalesce(func.sum(UsageLedger.output_tokens), 0).label(
                            "total_output_tokens"
                        ),
                        func.coalesce(func.sum(UsageLedger.cost_usd), 0).label(
                            "total_cost_usd"
                        ),
                    )
                    .select_from(UsageLedger)
                    .join(
                        GatewayRequest,
                        GatewayRequest.id == UsageLedger.gateway_request_id,
                    )
                    .where(*request_filters)
                )
            ).one()

            failover_request_ids = (
                select(ProviderAttempt.gateway_request_id)
                .join(
                    GatewayRequest,
                    GatewayRequest.id == ProviderAttempt.gateway_request_id,
                )
                .where(*request_filters)
                .group_by(ProviderAttempt.gateway_request_id)
                .having(func.count(ProviderAttempt.id) > 1)
                .subquery()
            )
            failover_count = await session.scalar(
                select(func.count()).select_from(failover_request_ids)
            )

            overhead_aggregate = (
                await session.execute(
                    select(
                        func.avg(GatewayRequest.gateway_overhead_ms).label(
                            "average_ms"
                        ),
                        func.percentile_cont(0.50)
                        .within_group(GatewayRequest.gateway_overhead_ms)
                        .label("p50_ms"),
                        func.percentile_cont(0.95)
                        .within_group(GatewayRequest.gateway_overhead_ms)
                        .label("p95_ms"),
                        func.percentile_cont(0.99)
                        .within_group(GatewayRequest.gateway_overhead_ms)
                        .label("p99_ms"),
                        func.count(GatewayRequest.id).label("samples"),
                    ).where(
                        *request_filters,
                        GatewayRequest.status == "completed",
                        GatewayRequest.is_stream.is_(False),
                        GatewayRequest.gateway_overhead_ms.is_not(None),
                    )
                )
            ).one()

        cost = ledger_aggregate.total_cost_usd or Decimal("0")
        average_ms = overhead_aggregate.average_ms

        return UsageStatsSummary(
            total_requests=int(total_requests or 0),
            settled_requests=settled_reservations,
            settled_reservations=settled_reservations,
            provider_attempts=int(provider_attempts or 0),
            usage_ledger_entries=int(ledger_aggregate.ledger_entries or 0),
            active_reservations=active_reservations,
            active_held_micros=active_held_micros,
            reconciliation_needed_reservations=reconciliation_needed,
            total_input_tokens=int(ledger_aggregate.total_input_tokens or 0),
            total_output_tokens=int(ledger_aggregate.total_output_tokens or 0),
            total_cost_usd=Decimal(cost),
            failover_count=int(failover_count or 0),
            gateway_overhead=GatewayOverheadSummary(
                average_ms=float(average_ms) if average_ms is not None else None,
                p50_ms=(
                    float(overhead_aggregate.p50_ms)
                    if overhead_aggregate.p50_ms is not None
                    else None
                ),
                p95_ms=(
                    float(overhead_aggregate.p95_ms)
                    if overhead_aggregate.p95_ms is not None
                    else None
                ),
                p99_ms=(
                    float(overhead_aggregate.p99_ms)
                    if overhead_aggregate.p99_ms is not None
                    else None
                ),
                samples=int(overhead_aggregate.samples or 0),
            ),
        )
