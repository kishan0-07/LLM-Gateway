from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr, model_validator

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str
    redis_url: str
    groq_api_key: str
    openai_api_key: str

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

    @model_validator(mode="after")
    def validate_langfuse(self):
        if self.langfuse_enabled and (
            self.langfuse_public_key is None or self.langfuse_secret_key is None
        ):
            raise ValueError(
                "LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are required "
                "when LANGFUSE_ENABLED=true"
            )
        return self
    
settings = Settings()