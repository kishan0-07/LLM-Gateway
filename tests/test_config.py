import pytest
from pydantic import ValidationError

from app.core.config import Settings


def make_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "environment": "development",
        "database_url": "",
        "redis_url": "",
        "groq_api_key": "",
        "openai_api_key": "",
        "langfuse_enabled": False,
        "langfuse_public_key": None,
        "langfuse_secret_key": None,
        "_env_file": None,
    }
    values.update(overrides)
    return Settings(**values)


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            "postgres://user:pass@host:5432/db",
            "postgresql+asyncpg://user:pass@host:5432/db",
        ),
        (
            "postgresql://user:pass@host:5432/db",
            "postgresql+asyncpg://user:pass@host:5432/db",
        ),
        (
            "postgresql+asyncpg://user:pass@host:5432/db",
            "postgresql+asyncpg://user:pass@host:5432/db",
        ),
    ],
)
def test_postgres_url_normalization(source: str, expected: str) -> None:
    assert make_settings(database_url=source).database_url == expected


def test_postgres_url_normalization_preserves_credentials_and_query() -> None:
    source = (
        "postgres://myuser:s3cret%40pass@db.railway.internal:5432/mydb?sslmode=require"
    )

    assert make_settings(database_url=source).database_url == (
        "postgresql+asyncpg://"
        "myuser:s3cret%40pass@db.railway.internal:5432/"
        "mydb?sslmode=require"
    )


def test_unsupported_database_scheme_fails_without_echoing_secret() -> None:
    secret_url = "mysql://user:do-not-echo@example/db"

    with pytest.raises(ValidationError) as exc_info:
        make_settings(database_url=secret_url)

    rendered_error = str(exc_info.value)
    assert "PostgreSQL URL scheme" in rendered_error
    assert "do-not-echo" not in rendered_error


@pytest.mark.parametrize("redis_url", ["redis://redis:6379/0", "rediss://redis/0"])
def test_supported_redis_urls_are_accepted(redis_url: str) -> None:
    assert make_settings(redis_url=redis_url).redis_url == redis_url


def test_unsupported_redis_scheme_fails_without_echoing_secret() -> None:
    secret_url = "http://:do-not-echo@redis.internal"

    with pytest.raises(ValidationError) as exc_info:
        make_settings(redis_url=secret_url)

    rendered_error = str(exc_info.value)
    assert "redis:// or rediss://" in rendered_error
    assert "do-not-echo" not in rendered_error


@pytest.mark.parametrize(
    ("database_url", "redis_url"),
    [
        ("", "redis://redis:6379/0"),
        ("postgresql://user:pass@db/gateway", ""),
    ],
)
def test_production_requires_database_and_redis(
    database_url: str,
    redis_url: str,
) -> None:
    with pytest.raises(ValidationError, match="required in production"):
        make_settings(
            environment="production",
            database_url=database_url,
            redis_url=redis_url,
        )


def test_production_requires_at_least_one_provider() -> None:
    with pytest.raises(ValidationError, match="provider API key"):
        make_settings(
            environment="production",
            database_url="postgresql://user:pass@db/gateway",
            redis_url="redis://redis:6379/0",
        )


@pytest.mark.parametrize(
    ("groq_api_key", "openai_api_key"),
    [
        ("groq-secret", ""),
        ("", "openai-secret"),
    ],
)
def test_each_production_provider_alternative_is_accepted(
    groq_api_key: str,
    openai_api_key: str,
) -> None:
    configured = make_settings(
        environment="production",
        database_url="postgresql://user:pass@db/gateway",
        redis_url="rediss://redis.internal/0",
        groq_api_key=groq_api_key,
        openai_api_key=openai_api_key,
    )

    assert configured.environment == "production"


@pytest.mark.parametrize(
    ("public_key", "secret_key"),
    [(None, None), ("", ""), ("public", ""), ("", "secret")],
)
def test_enabled_langfuse_requires_nonempty_key_pair(
    public_key: str | None,
    secret_key: str | None,
) -> None:
    with pytest.raises(ValidationError, match="LANGFUSE_PUBLIC_KEY"):
        make_settings(
            langfuse_enabled=True,
            langfuse_public_key=public_key,
            langfuse_secret_key=secret_key,
        )
