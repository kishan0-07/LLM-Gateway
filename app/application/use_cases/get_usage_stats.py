from app.application.ports.stats_reader import StatsReader, UsageStatsSummary


class GetUsageStats:
    def __init__(self, reader: StatsReader) -> None:
        self._reader = reader

    async def for_tenant(self, tenant_id: int) -> UsageStatsSummary:
        return await self._reader.read(tenant_id=tenant_id, api_key_id=None)

    async def for_api_key(
        self,
        *,
        tenant_id: int,
        api_key_id: int,
    ) -> UsageStatsSummary:
        return await self._reader.read(
            tenant_id=tenant_id,
            api_key_id=api_key_id,
        )
