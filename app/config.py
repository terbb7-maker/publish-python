import os
from functools import lru_cache
from typing import Literal

from dotenv import load_dotenv
from pydantic import BaseModel, Field, HttpUrl, SecretStr, field_validator


class Settings(BaseModel):
    environment: Literal["local", "staging", "production"] = "local"
    service_name: str = "terbb-python-publisher"
    worker_id: str = "publisher-1"

    supabase_url: HttpUrl
    supabase_service_role_key: SecretStr
    supabase_database_url: SecretStr
    meta_token_encryption_key: SecretStr

    instagram_api_version: str = "v23.0"

    poll_interval_seconds: float = Field(default=2.0, ge=0.2, le=60)
    empty_poll_interval_seconds: float = Field(default=5.0, ge=0.5, le=120)
    batch_size: int = Field(default=100, ge=1, le=100)
    concurrency: int = Field(default=50, ge=1, le=500)
    db_pool_size: int = Field(default=20, ge=2, le=200)

    lease_seconds: int = Field(default=900, ge=60, le=7200)
    heartbeat_interval_seconds: int = Field(default=30, ge=5, le=300)
    publish_timeout_seconds: int = Field(default=900, ge=60, le=7200)
    stale_recovery_limit: int = Field(default=500, ge=1, le=5000)

    max_attempts: int = Field(default=5, ge=1, le=20)
    retry_base_seconds: int = Field(default=60, ge=10, le=86400)
    retry_max_seconds: int = Field(default=3600, ge=60, le=86400)
    retry_jitter_seconds: int = Field(default=30, ge=0, le=3600)

    media_signed_url_ttl_seconds: int = Field(default=3600, ge=60, le=86400)
    http_timeout_seconds: float = Field(default=30.0, ge=5.0, le=120.0)
    polling_initial_seconds: float = Field(default=2.0, ge=0.5, le=60.0)
    polling_max_seconds: float = Field(default=30.0, ge=1.0, le=300.0)
    polling_max_attempts: int = Field(default=20, ge=1, le=200)

    log_level: Literal["debug", "info", "warning", "error"] = "info"
    dry_run: bool = False

    @field_validator("instagram_api_version")
    @classmethod
    def normalize_graph_version(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("INSTAGRAM_API_VERSION cannot be empty")
        return value if value.startswith("v") else f"v{value}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    load_dotenv()

    return Settings(
        environment=os.getenv("PYTHON_PUBLISHER_ENV", "local"),
        service_name=os.getenv("PYTHON_PUBLISHER_SERVICE_NAME", "terbb-python-publisher"),
        worker_id=os.getenv("PYTHON_PUBLISHER_WORKER_ID", "publisher-1"),
        supabase_url=os.environ["SUPABASE_URL"],
        supabase_service_role_key=SecretStr(os.environ["SUPABASE_SERVICE_ROLE_KEY"]),
        supabase_database_url=SecretStr(os.environ["SUPABASE_DATABASE_URL"]),
        meta_token_encryption_key=SecretStr(os.environ["META_TOKEN_ENCRYPTION_KEY"]),
        instagram_api_version=os.getenv(
            "INSTAGRAM_API_VERSION",
            os.getenv("META_GRAPH_API_VERSION", "v23.0"),
        ),
        poll_interval_seconds=float(os.getenv("PYTHON_PUBLISHER_POLL_INTERVAL_SECONDS", "2")),
        empty_poll_interval_seconds=float(
            os.getenv("PYTHON_PUBLISHER_EMPTY_POLL_INTERVAL_SECONDS", "5")
        ),
        batch_size=int(os.getenv("PYTHON_PUBLISHER_BATCH_SIZE", "100")),
        concurrency=int(os.getenv("PYTHON_PUBLISHER_CONCURRENCY", "50")),
        db_pool_size=int(os.getenv("PYTHON_PUBLISHER_DB_POOL_SIZE", "20")),
        lease_seconds=int(os.getenv("PYTHON_PUBLISHER_LEASE_SECONDS", "900")),
        heartbeat_interval_seconds=int(
            os.getenv("PYTHON_PUBLISHER_HEARTBEAT_INTERVAL_SECONDS", "30")
        ),
        publish_timeout_seconds=int(os.getenv("PYTHON_PUBLISHER_TIMEOUT_SECONDS", "900")),
        stale_recovery_limit=int(os.getenv("PYTHON_PUBLISHER_STALE_RECOVERY_LIMIT", "500")),
        max_attempts=int(os.getenv("PYTHON_PUBLISHER_MAX_ATTEMPTS", "5")),
        retry_base_seconds=int(os.getenv("PYTHON_PUBLISHER_RETRY_BASE_SECONDS", "60")),
        retry_max_seconds=int(os.getenv("PYTHON_PUBLISHER_RETRY_MAX_SECONDS", "3600")),
        retry_jitter_seconds=int(os.getenv("PYTHON_PUBLISHER_RETRY_JITTER_SECONDS", "30")),
        media_signed_url_ttl_seconds=int(
            os.getenv("PYTHON_PUBLISHER_MEDIA_SIGNED_URL_TTL_SECONDS", "3600")
        ),
        http_timeout_seconds=float(os.getenv("PYTHON_PUBLISHER_HTTP_TIMEOUT_SECONDS", "30")),
        polling_initial_seconds=float(os.getenv("PYTHON_PUBLISHER_POLLING_INITIAL_SECONDS", "2")),
        polling_max_seconds=float(os.getenv("PYTHON_PUBLISHER_POLLING_MAX_SECONDS", "30")),
        polling_max_attempts=int(os.getenv("PYTHON_PUBLISHER_POLLING_MAX_ATTEMPTS", "20")),
        log_level=os.getenv("PYTHON_PUBLISHER_LOG_LEVEL", "info"),
        dry_run=os.getenv("PYTHON_PUBLISHER_DRY_RUN", "false").lower() in {"1", "true", "yes"},
    )
