from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProducerSettings(BaseSettings):
    """Configuration from environment variables (prefix PRODUCER_) or a local .env file."""

    model_config = SettingsConfigDict(env_prefix="PRODUCER_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://catalog:catalog_pwd@127.0.0.1:5432/catalog"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "products"
    client_id: str = "product-producer"

    checkpoint_name: str = "products->kafka"
    poll_interval_seconds: float = Field(default=2.0, gt=0)
    batch_size: int = Field(default=500, gt=0)
    settle_delay_ms: int = Field(
        default=1000,
        ge=0,
        description="Rows younger than this are read on the next poll so in-flight "
        "transactions with an earlier updated_at are not skipped.",
    )
    flush_timeout_seconds: float = Field(default=30.0, gt=0)

    log_level: str = "INFO"


@lru_cache
def get_settings() -> ProducerSettings:
    return ProducerSettings()
