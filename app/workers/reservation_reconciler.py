import asyncio
from app.application.ports.budget_store import BudgetStore
from app.core.logging import logger


class ReservationReconciler:
    def __init__(self, budget_store: BudgetStore, *, interval_seconds: int) -> None:
        if interval_seconds <= 0:
            raise ValueError("reconcile interval must be positive")
        self._budget_store = budget_store
        self._interval_seconds = interval_seconds
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run_once(self) -> int:
        # 1. Repair out-of-sync Redis month keys
        repaired_cache_count = await self._budget_store.repair_out_of_sync_caches_once()
        if repaired_cache_count:
            logger.info("reconciler_repaired_out_of_sync_caches", count=repaired_cache_count)

        # 2. Expire or hold stale reservations
        expired_count = await self._budget_store.expire_stale_once()
        if expired_count:
            logger.warning("stale_reservations_expired", expired_count=expired_count)

        return repaired_cache_count + expired_count

    async def run(self) -> None:
        logger.info(
            "reservation_reconciler_started",
            interval_seconds=self._interval_seconds,
        )
        try:
            while not self._stop_event.is_set():
                try:
                    await self.run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception(
                        "reservation_reconciler_iteration_failed",
                        error_type=type(exc).__name__,
                        error=str(exc),
                    )

                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._interval_seconds,
                    )
                except TimeoutError:
                    pass
        finally:
            logger.info("reservation_reconciler_stopped")