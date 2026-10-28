from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConsumerSettings(BaseSettings):
    """Configuration from environment variables (prefix CONSUMER_) or a local .env file."""

    model_config = SettingsConfigDict(env_prefix="CONSUMER_", env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "products"
    kafka_dlq_topic: str = "products.dlq"
    group_id: str = "products-mongo-sync"
    client_id: str = "product-consumer"

    mongo_uri: str = (
        "mongodb://sync_app:sync_app_pwd@127.0.0.1:27017/product_catalog?authSource=product_catalog"
    )
    mongo_database: str = "product_catalog"
    mongo_collection: str = "products"

    retry_initial_seconds: float = Field(default=1.0, gt=0)
    retry_max_seconds: float = Field(default=30.0, gt=0)

    log_level: str = "INFO"


@lru_cache
def get_settings() -> ConsumerSettings:
    return ConsumerSettings()
