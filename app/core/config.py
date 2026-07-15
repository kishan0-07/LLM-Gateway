from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict

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
    
settings = Settings()