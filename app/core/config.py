from typing import Literal, Self

from pydantic import SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
    )

    database_url: str = ""
    redis_url: str = ""
    groq_api_key: str = ""
    openai_api_key: str = ""
    environment: Literal["development", "test", "production"] = "development"

    rate_limit_window_seconds: int = 60
    rate_limit_tenant_requests: int = 120
    rate_limit_api_key_requests: int = 60
    rate_limit_redis_failure_mode: Literal["fail_open", "fail_closed"] = "fail_open"
    reservation_reconcile_interval_seconds: int = 60
    shutdown_grace_seconds: int = 10

    langfuse_enabled: bool = False
    langfuse_public_key: SecretStr | None = None
    langfuse_secret_key: SecretStr | None = None
    langfuse_base_url: str = "https://cloud.langfuse.com"

    @field_validator("database_url")
    @classmethod
    def normalize_async_postgres_url(cls, value: str) -> str:
        if not value:
            return value
        if value.startswith("postgres://"):
            return "postgresql+asyncpg://" + value.removeprefix("postgres://")
        if value.startswith("postgresql://"):
            return "postgresql+asyncpg://" + value.removeprefix("postgresql://")
        if value.startswith("postgresql+asyncpg://"):
            return value
        raise ValueError("DATABASE_URL must use a PostgreSQL URL scheme")

    @field_validator("redis_url")
    @classmethod
    def validate_redis_url(cls, value: str) -> str:
        if value and not value.startswith(("redis://", "rediss://")):
            raise ValueError("REDIS_URL must use redis:// or rediss://")
        return value

    @model_validator(mode="after")
    def validate_langfuse(self) -> Self:
        if self.langfuse_enabled and (
            self.langfuse_public_key is None
            or not self.langfuse_public_key.get_secret_value()
            or self.langfuse_secret_key is None
            or not self.langfuse_secret_key.get_secret_value()
        ):
            raise ValueError(
                "LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are required "
                "when LANGFUSE_ENABLED=true"
            )
        return self

    @model_validator(mode="after")
    def validate_production_dependencies(self) -> Self:
        if self.environment == "production" and (
            not self.database_url or not self.redis_url
        ):
            raise ValueError("DATABASE_URL and REDIS_URL are required in production")
        if self.environment == "production" and not (
            self.groq_api_key or self.openai_api_key
        ):
            raise ValueError("at least one provider API key is required in production")
        return self


settings = Settings()
