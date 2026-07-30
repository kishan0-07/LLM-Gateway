# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.9 AS uv

FROM python:3.13-slim AS builder
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONDONTWRITEBYTECODE=1

COPY --from=uv /uv /uvx /bin/
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./
COPY scripts/chaos/state_probe.py ./scripts/chaos/state_probe.py
RUN uv sync --frozen --no-dev

FROM python:3.13-slim AS runtime
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --system gateway && useradd --system --gid gateway gateway
COPY --from=builder /app /app
USER gateway
EXPOSE 8000

CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port \"${PORT:-8000}\" --timeout-graceful-shutdown \"${UVICORN_GRACEFUL_SHUTDOWN_SECONDS:-50}\""]
