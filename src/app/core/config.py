import os
import secrets
import warnings
from typing import Any
from enum import Enum
from pydantic import PostgresDsn, AnyHttpUrl, computed_field, field_validator, model_validator
from pydantic_core.core_schema import FieldValidationInfo
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    MODE: str
    PROJECT_NAME: str
    API_VERSION: str
    API_V1_STR: str

    # Security
    SECRET_KEY: str = secrets.token_urlsafe(32)
    ENCRYPT_KEY: str = secrets.token_urlsafe(32)
    JWT_ALGORITHM: str
    JWT_REFRESH_TOKEN_KEY: str = secrets.token_urlsafe(32)

    ACCESS_TOKENT_EXPIRE_MINUTES: int = 60
    REFRESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 100

    # === Main DB (Application) ===
    DATABASE_USER: str
    DATABASE_PASSWORD: str
    DATABASE_HOST: str
    DATABASE_PORT: int
    DATABASE_NAME: str

    # === Experiment Tracking DB (MLflow etc.) ===
    EXPERIMENT_DB_NAME: str

    # === Redis / Queue ===
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_URL: str = ""

    # === Object Storage (MinIO / S3) ===
    MINIO_ROOT_USER: str
    MINIO_ROOT_PASSWORD: str
    MINIO_URL: str
    MINIO_BUCKET: str

    VAULT_URL: str            # <-- Add this
    VAULT_TOKEN: str         # <-- Add this
    VAULT_DB_MAIN_PATH: str   # Vault path for main DB secrets
    VAULT_DB_EXP_PATH: str    # Vault path for experiment DB
    VAULT_REDIS_PATH: str     # Vault path for Redis
    VAULT_MINIO_PATH: str     # Vault path for MinIO
    VAULT_VECTOR_PATH: str 

    # === Vector DB (Qdrant / Weaviate / Pinecone etc.) ===
    VECTOR_DB_URL: str
    VECTOR_DB_API_KEY: str | None = None

    # === Superuser ===
    FIRST_SUPERUSER_EMAIL: str
    FIRST_SUPERUSER_PASSWORD: str

    # Performance
    DB_POOL_SIZE: int
    WEB_CONCURRENCY: int

    @computed_field
    @property
    def POOL_SIZE(self) -> int:
        return max(self.DB_POOL_SIZE // self.WEB_CONCURRENCY, 5)

    # === Async Main DB URI ===
    ASYNC_DATABASE_URI: PostgresDsn | str = ""

    @field_validator("ASYNC_DATABASE_URI", mode="after")
    @classmethod
    def assemble_async_db_uri(cls, v: str | None, info: FieldValidationInfo) -> Any:
        if not v:
            data = info.data
            db_name = str(data.get("DATABASE_NAME", "")).lstrip("/")
            return PostgresDsn.build(
                scheme="postgresql+asyncpg",
                username=data["DATABASE_USER"],
                password=data["DATABASE_PASSWORD"],
                host=data["DATABASE_HOST"],
                port=data["DATABASE_PORT"],
                path=db_name,
            )
        return v

    # === Sync Experiment DB URI ===
    SYNC_EXPERIMENT_DB_URI: PostgresDsn | str = ""

    @field_validator("SYNC_EXPERIMENT_DB_URI", mode="after")
    @classmethod
    def assemble_sync_experiment_db_uri(cls, v: str | None, info: FieldValidationInfo) -> Any:
        if not v:
            data = info.data
            exp_db = str(data.get("EXPERIMENT_DB_NAME", "")).lstrip("/")
            return PostgresDsn.build(
                scheme="postgresql+psycopg2",
                username=data["DATABASE_USER"],
                password=data["DATABASE_PASSWORD"],
                host=data["DATABASE_HOST"],
                port=data["DATABASE_PORT"],
                path=exp_db,
            )
        return v

    # === Redis URL ===
    @field_validator("REDIS_URL", mode="after")
    @classmethod
    def assemble_redis_url(cls, v: str | None, info: FieldValidationInfo) -> Any:
        if not v:
            data = info.data
            return f"redis://{data['REDIS_HOST']}:{data['REDIS_PORT']}"
        return v

    # === CORS ===
    BACKEND_CORS_ORIGINS: list[str] | list[AnyHttpUrl]

    @field_validator("BACKEND_CORS_ORIGINS", mode="after")
    @classmethod
    def assemble_backend_cors_origins(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",") if origin.strip()]
        elif isinstance(v, list):
            return v
        raise ValueError(f"Invalid cors origins: {v}")

    @computed_field
    @property
    def all_cors_origins(self) -> list[str]:
        return [origin.rstrip("/") for origin in self.BACKEND_CORS_ORIGINS]

    # === Validation ===
    @model_validator(mode="after")
    def check_default_secrete(self) -> "Settings":
        for field in ["SECRET_KEY", "DATABASE_PASSWORD", "DATABASE_USER"]:
            value = getattr(self, field, None)
            if value == "changethis":
                message = f"{field} is set to 'changethis'. Change it in production"
                if self.MODE == "development":
                    warnings.warn(message)
                else:
                    raise ValueError(message)
        return self

    model_config = SettingsConfigDict(
        env_file=os.path.expanduser(".env"),
        case_sensitive=True,
        extra="ignore",
    )


settings = Settings()
