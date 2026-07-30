import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest
from httpx import ASGITransport, AsyncClient

import app.main as main_module
from app.api.deps import get_completion_use_cases
from app.main import app


class FakeReconciler:
    def __init__(self, *_args: object, **_kwargs: object) -> None:
        self._stopped = asyncio.Event()

    async def run(self) -> None:
        await self._stopped.wait()

    def stop(self) -> None:
        self._stopped.set()


class FakeCompletionFactory:
    def __init__(self, drain_finalizers: AsyncMock | None) -> None:
        self._drain_finalizers = drain_finalizers

    def cache_info(self) -> SimpleNamespace:
        return SimpleNamespace(currsize=int(self._drain_finalizers is not None))

    def __call__(self) -> SimpleNamespace:
        if self._drain_finalizers is None:
            raise AssertionError("uncached completion factory must not be called")
        return SimpleNamespace(
            stream=SimpleNamespace(drain_finalizers=self._drain_finalizers)
        )


def patch_lifespan_baseline(
    monkeypatch: pytest.MonkeyPatch,
    *,
    drain_finalizers: AsyncMock | None = None,
    shutdown_langfuse: AsyncMock | None = None,
    close_redis: AsyncMock | None = None,
    close_database: AsyncMock | None = None,
) -> tuple[AsyncMock, AsyncMock, AsyncMock]:
    langfuse = shutdown_langfuse or AsyncMock()
    redis = close_redis or AsyncMock()
    database = close_database or AsyncMock()

    monkeypatch.setattr(main_module, "ReservationReconciler", FakeReconciler)
    monkeypatch.setattr(
        main_module,
        "get_completion_use_cases",
        FakeCompletionFactory(drain_finalizers),
    )
    monkeypatch.setattr(main_module, "shutdown_langfuse", langfuse)
    monkeypatch.setattr(main_module, "close_redis", redis)
    monkeypatch.setattr(main_module, "close_database", database)
    return langfuse, redis, database


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lifespan_starts_and_stops_reconciler() -> None:
    finalizer_completed = asyncio.Event()

    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)

        async with AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            response = await client.get("/health")

        assert response.status_code == 200
        task = app.state.reservation_reconciler_task
        assert not task.done()

        async def pending_finalizer() -> None:
            await asyncio.sleep(0.01)
            finalizer_completed.set()

        stream_use_case = get_completion_use_cases().stream
        finalizer_task = asyncio.create_task(pending_finalizer())
        stream_use_case._finalizer_tasks.add(finalizer_task)
        finalizer_task.add_done_callback(stream_use_case._finalizer_tasks.discard)

    assert task.done()
    assert not task.cancelled()
    assert finalizer_completed.is_set()
    assert finalizer_task.done()


@pytest.mark.asyncio
async def test_shutdown_drains_finalizers_before_closing_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    drain = AsyncMock(side_effect=lambda **_kwargs: order.append("finalizers"))
    langfuse = AsyncMock(side_effect=lambda: order.append("langfuse"))
    redis = AsyncMock(side_effect=lambda: order.append("redis"))
    database = AsyncMock(side_effect=lambda: order.append("database"))
    info = Mock(side_effect=lambda event, **_kwargs: order.append(event))

    patch_lifespan_baseline(
        monkeypatch,
        drain_finalizers=drain,
        shutdown_langfuse=langfuse,
        close_redis=redis,
        close_database=database,
    )
    monkeypatch.setattr(main_module.logger, "info", info)

    async with app.router.lifespan_context(app):
        pass

    drain.assert_awaited_once_with(
        timeout_seconds=main_module.settings.shutdown_grace_seconds
    )
    redis.assert_awaited_once()
    database.assert_awaited_once()
    assert order.index("finalizers") < order.index("redis")
    assert order.index("finalizers") < order.index("database")
    assert order.index("langfuse") < order.index("redis")
    assert order.index("langfuse") < order.index("database")
    assert order.index("redis") < order.index("shutdown_complete")
    assert order.index("database") < order.index("shutdown_complete")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failed_dependency", "failure"),
    [
        ("redis", RuntimeError("redis close failed")),
        ("database", OSError("database close failed")),
    ],
)
async def test_dependency_close_exception_is_suppressed_and_reported(
    monkeypatch: pytest.MonkeyPatch,
    failed_dependency: str,
    failure: BaseException,
) -> None:
    redis = AsyncMock(side_effect=failure if failed_dependency == "redis" else None)
    database = AsyncMock(
        side_effect=failure if failed_dependency == "database" else None
    )
    warning = Mock()

    patch_lifespan_baseline(
        monkeypatch,
        close_redis=redis,
        close_database=database,
    )
    monkeypatch.setattr(main_module.logger, "warning", warning)

    async with app.router.lifespan_context(app):
        pass

    redis.assert_awaited_once()
    database.assert_awaited_once()
    warning.assert_has_calls(
        [
            call(
                "dependency_shutdown_failed",
                dependency=failed_dependency,
                error_type=type(failure).__name__,
            )
        ]
    )


@pytest.mark.asyncio
async def test_langfuse_timeout_does_not_prevent_dependency_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    langfuse = AsyncMock(side_effect=TimeoutError)
    warning = Mock()
    _, redis, database = patch_lifespan_baseline(
        monkeypatch,
        shutdown_langfuse=langfuse,
    )
    monkeypatch.setattr(main_module.logger, "warning", warning)

    async with app.router.lifespan_context(app):
        pass

    langfuse.assert_awaited_once()
    redis.assert_awaited_once()
    database.assert_awaited_once()
    warning.assert_any_call("langfuse_shutdown_timed_out")


@pytest.mark.asyncio
async def test_dependency_close_timeout_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    never_finishes = asyncio.Event()

    async def block_until_cancelled() -> None:
        await never_finishes.wait()

    redis = AsyncMock(side_effect=block_until_cancelled)
    database = AsyncMock(side_effect=block_until_cancelled)
    warning = Mock()
    info = Mock()
    patch_lifespan_baseline(
        monkeypatch,
        close_redis=redis,
        close_database=database,
    )
    monkeypatch.setattr(main_module, "DEPENDENCY_SHUTDOWN_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(main_module.logger, "warning", warning)
    monkeypatch.setattr(main_module.logger, "info", info)

    async with app.router.lifespan_context(app):
        pass

    redis.assert_awaited_once()
    database.assert_awaited_once()
    warning.assert_any_call("dependency_shutdown_timed_out")
    info.assert_any_call("shutdown_complete")
