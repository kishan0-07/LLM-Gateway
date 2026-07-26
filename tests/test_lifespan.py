import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from app.api.deps import get_completion_use_cases
from app.main import app


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
