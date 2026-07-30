from fastapi import APIRouter, Depends
from app.api.deps import get_principal
from app.api.schemas.usage import GatewayOverheadResponse, UsageStatsResponse
from app.application.ports.stats_reader import UsageStatsSummary
from app.application.use_cases.get_usage_stats import GetUsageStats
from app.domain.auth import Principal
from app.infrastructure.db.stats_reader import SQLAlchemyStatsReader

router = APIRouter()


def get_usage_stats() -> GetUsageStats:
    return GetUsageStats(SQLAlchemyStatsReader())


def _response(
    scope: str,
    summary: UsageStatsSummary,
) -> UsageStatsResponse:
    return UsageStatsResponse(
        scope=scope,
        total_requests=summary.total_requests,
        settled_requests=summary.settled_requests,
        settled_reservations=summary.settled_reservations,
        provider_attempts=summary.provider_attempts,
        usage_ledger_entries=summary.usage_ledger_entries,
        active_reservations=summary.active_reservations,
        active_held_micros=summary.active_held_micros,
        reconciliation_needed_reservations=(summary.reconciliation_needed_reservations),
        total_input_tokens=summary.total_input_tokens,
        total_output_tokens=summary.total_output_tokens,
        total_cost_usd=f"{summary.total_cost_usd:.6f}",
        failover_count=summary.failover_count,
        gateway_overhead_ms=GatewayOverheadResponse(
            average_ms=summary.gateway_overhead.average_ms,
            p50_ms=summary.gateway_overhead.p50_ms,
            p95_ms=summary.gateway_overhead.p95_ms,
            p99_ms=summary.gateway_overhead.p99_ms,
            samples=summary.gateway_overhead.samples,
        ),
    )


@router.get("/stats", response_model=UsageStatsResponse)
async def tenant_stats(
    principal: Principal = Depends(get_principal),
    use_case: GetUsageStats = Depends(get_usage_stats),
) -> UsageStatsResponse:
    return _response("tenant", await use_case.for_tenant(principal.tenant_id))


@router.get("/stats/me", response_model=UsageStatsResponse)
async def api_key_stats(
    principal: Principal = Depends(get_principal),
    use_case: GetUsageStats = Depends(get_usage_stats),
) -> UsageStatsResponse:
    return _response(
        "api_key",
        await use_case.for_api_key(
            tenant_id=principal.tenant_id,
            api_key_id=principal.api_key_id,
        ),
    )
