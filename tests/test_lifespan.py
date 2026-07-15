from fastapi.testclient import TestClient
from app.main import app


def test_lifespan_starts_and_stops_reconciler() -> None:
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        task = app.state.reservation_reconciler_task
        assert not task.done()

    assert task.done()
    assert not task.cancelled()