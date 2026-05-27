from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "payment-service"
    api_key: str = Field(default="change-me", min_length=1)
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    outbox_poll_interval_seconds: float = 1.0
    outbox_batch_size: int = 50

    payment_gateway_min_delay_seconds: float = 2.0
    payment_gateway_max_delay_seconds: float = 5.0
    payment_processing_lease_seconds: float = 60.0
    webhook_timeout_seconds: float = 5.0
    webhook_delivery_lease_seconds: float = 60.0
    max_retry_attempts: int = 3

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
