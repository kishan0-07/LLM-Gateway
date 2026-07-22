import asyncio
from decimal import Decimal
from app.infrastructure.db.stats_reader import SQLAlchemyStatsReader

async def test_stats_query():
    reader = SQLAlchemyStatsReader()
    # Run read query for non-existent scopes to verify fallback
    summary = await reader.read(tenant_id=99999, api_key_id=99999)
    
    print(f"Stats check for empty tenant: Total={summary.total_requests}, Cost={summary.total_cost_usd}")
    assert summary.total_requests == 0
    assert summary.total_cost_usd == Decimal("0")
    print("  [Pass] Empty stats return zeroes without crash.")

async def main():
    print("=== Running SQLAlchemyStatsReader Tests ===")
    await test_stats_query()
    print("All stats query tests completed successfully!")

if __name__ == "__main__":
    asyncio.run(main())