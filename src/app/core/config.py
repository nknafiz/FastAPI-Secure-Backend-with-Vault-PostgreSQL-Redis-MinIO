# secure_settings.py
import os
import warnings
import asyncio
import logging
import functools
from typing import Any, Optional, Callable, Coroutine, Union
from pathlib import Path

import hvac
import asyncpg
import redis.asyncio as redis
from minio import Minio

from pydantic import PostgresDsn, AnyHttpUrl, computed_field, field_validator, model_validator
from pydantic_core.core_schema import FieldValidationInfo
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def async_retry(
    max_attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 5.0,
    allowed_exceptions: tuple = (Exception,),
):
    """Async exponential backoff retry decorator."""
    def decorator(func: Callable[..., Coroutine[Any, Any, Any]]):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except allowed_exceptions:
                    attempt += 1
                    if attempt >= max_attempts:
                        logger.exception("Max attempts reached for %s", func.__name__)
                        raise
                    delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
                    await asyncio.sleep(delay)
        return wrapper
    return decorator


class Settings(BaseSettings):
    # ---------------- General ----------------
    MODE: str = "development"
    PROJECT_NAME: str = "MyProject"
    API_VERSION: str = "v1"
    API_V1_STR: str = "/api/v1"

    # ---------------- Security ----------------
    SECRET_KEY: Optional[str] = None
    ENCRYPT_KEY: Optional[str] = None
    JWT_ALGORITHM: str = "HS256"
    JWT_REFRESH_TOKEN_KEY: Optional[str] = None
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60
    REFRESH_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 100

    # ---------------- Main DB ----------------
    DATABASE_USER: Optional[str] = None
    DATABASE_PASSWORD: Optional[str] = None
    DATABASE_HOST: Optional[str] = None
    DATABASE_PORT: int = 5432
    DATABASE_NAME: Optional[str] = None
    ASYNC_DATABASE_URI: Optional[Union[PostgresDsn, str]] = None
    async_db_pool: Optional[asyncpg.pool.Pool] = None

    # ---------------- Experiment DB ----------------
    EXPERIMENT_DB_NAME: Optional[str] = None
    SYNC_EXPERIMENT_DB_URI: Optional[Union[PostgresDsn, str]] = None

    # ---------------- Redis ----------------
    REDIS_HOST: Optional[str] = None
    REDIS_PORT: int = 6379
    REDIS_URL: Optional[str] = None
    redis_pool: Optional[redis.Redis] = None

    # ---------------- Object Storage ----------------
    MINIO_ROOT_USER: Optional[str] = None
    MINIO_ROOT_PASSWORD: Optional[str] = None
    MINIO_URL: Optional[str] = None
    MINIO_BUCKET: Optional[str] = None
    minio_client: Optional[Minio] = None

    # ---------------- Vault ----------------
    VAULT_URL: Optional[str] = None
    VAULT_TOKEN: Optional[str] = None
    VAULT_DB_MAIN_PATH: Optional[str] = None
    VAULT_DB_EXP_PATH: Optional[str] = None
    VAULT_REDIS_PATH: Optional[str] = None
    VAULT_MINIO_PATH: Optional[str] = None
    VAULT_VECTOR_PATH: Optional[str] = None

    # ---------------- Vector DB ----------------
    VECTOR_DB_URL: Optional[str] = None
    VECTOR_DB_API_KEY: Optional[str] = None

    # ---------------- Superuser ----------------
    FIRST_SUPERUSER_EMAIL: Optional[str] = None
    FIRST_SUPERUSER_PASSWORD: Optional[str] = None

    # ---------------- Performance ----------------
    DB_POOL_SIZE: int = 10
    WEB_CONCURRENCY: int = 2

    # ---------------- Computed ----------------
    @computed_field
    @property
    def POOL_SIZE(self) -> int:
        return max(self.DB_POOL_SIZE // max(1, self.WEB_CONCURRENCY), 2)

    @computed_field
    @property
    def vault_client(self) -> hvac.Client:
        if not self.VAULT_URL or not self.VAULT_TOKEN:
            raise RuntimeError("Vault URL and token must be provided in production")
        client = hvac.Client(url=self.VAULT_URL, token=self.VAULT_TOKEN)
        if not client.is_authenticated():
            raise RuntimeError("Vault authentication failed. Check VAULT_TOKEN/VAULT_URL.")
        return client

    # ---------------- Load Vault Secrets ----------------
    async def fetch_vault_secret_async(self, path: str) -> dict:
        loop = asyncio.get_running_loop()

        @async_retry(max_attempts=4, base_delay=0.5, max_delay=3.0)
        async def _call():
            return await loop.run_in_executor(None, self._fetch_vault_secret_safe, path)

        try:
            return await _call()
        except Exception as e:
            warnings.warn(f"Failed to fetch Vault secret at {path}: {str(e)}")
            logger.warning("Returning empty secret for path %s", path)
            return {}

    def _fetch_vault_secret_safe(self, path: str) -> dict:
        try:
            client = self.vault_client
            secret = client.secrets.kv.v2.read_secret_version(path=path)
            return secret.get("data", {}).get("data", {}) or {}
        except Exception as e:
            logger.exception("Vault read error for path %s", path)
            raise RuntimeError(f"Vault read error for path {path}") from e

    async def load_secrets_from_vault_async(self):
        tasks = []
        if self.VAULT_DB_MAIN_PATH:
            tasks.append(self._load_main_db_secret())
        if self.VAULT_DB_EXP_PATH:
            tasks.append(self._load_experiment_db_secret())
        if self.VAULT_REDIS_PATH:
            tasks.append(self._load_redis_secret())
        if self.VAULT_MINIO_PATH:
            tasks.append(self._load_minio_secret())
        if self.VAULT_VECTOR_PATH:
            tasks.append(self._load_vector_secret())
        if tasks:
            await asyncio.gather(*tasks)

    async def _load_main_db_secret(self):
        db = await self.fetch_vault_secret_async(self.VAULT_DB_MAIN_PATH)
        self.DATABASE_USER = db.get("username") or self.DATABASE_USER
        self.DATABASE_PASSWORD = db.get("password") or self.DATABASE_PASSWORD
        self.DATABASE_HOST = db.get("host") or self.DATABASE_HOST
        port = db.get("port")
        if port:
            try:
                self.DATABASE_PORT = int(port)
            except ValueError:
                logger.warning("Invalid DB port from vault: %s", port)
        self.DATABASE_NAME = db.get("dbname") or self.DATABASE_NAME

    async def _load_experiment_db_secret(self):
        exp = await self.fetch_vault_secret_async(self.VAULT_DB_EXP_PATH)
        self.EXPERIMENT_DB_NAME = exp.get("dbname") or self.EXPERIMENT_DB_NAME

    async def _load_redis_secret(self):
        redis_data = await self.fetch_vault_secret_async(self.VAULT_REDIS_PATH)
        self.REDIS_HOST = redis_data.get("host") or self.REDIS_HOST
        port = redis_data.get("port")
        if port:
            try:
                self.REDIS_PORT = int(port)
            except ValueError:
                logger.warning("Invalid Redis port from vault: %s", port)

    async def _load_minio_secret(self):
        minio_sec = await self.fetch_vault_secret_async(self.VAULT_MINIO_PATH)
        self.MINIO_ROOT_USER = minio_sec.get("username") or self.MINIO_ROOT_USER
        self.MINIO_ROOT_PASSWORD = minio_sec.get("password") or self.MINIO_ROOT_PASSWORD
        self.MINIO_URL = minio_sec.get("url") or self.MINIO_URL
        self.MINIO_BUCKET = minio_sec.get("bucket") or self.MINIO_BUCKET

    async def _load_vector_secret(self):
        vector = await self.fetch_vault_secret_async(self.VAULT_VECTOR_PATH)
        self.VECTOR_DB_URL = vector.get("url") or self.VECTOR_DB_URL
        self.VECTOR_DB_API_KEY = vector.get("api_key") or self.VECTOR_DB_API_KEY

    # ---------------- DB URI Validators ----------------
    @field_validator("ASYNC_DATABASE_URI", mode="after")
    @classmethod
    def assemble_async_db_uri(cls, v: Optional[str], info: FieldValidationInfo) -> str:
        if v:
            return v
        data = info.data
        db_name = str(data.get("DATABASE_NAME", "")).lstrip("/")
        return str( PostgresDsn.build(
            scheme="postgresql", 
            username=data.get("DATABASE_USER") or "",
            password=data.get("DATABASE_PASSWORD") or "",
            host=data.get("DATABASE_HOST") or "localhost",
            port=int(data.get("DATABASE_PORT") or 5432),
            path=f"{db_name}"
        ))

    @field_validator("SYNC_EXPERIMENT_DB_URI", mode="after")
    @classmethod
    def assemble_sync_experiment_db_uri(cls, v: Optional[str], info: FieldValidationInfo) -> str:
        if v:
            return v
        data = info.data
        db_name = str(data.get("EXPERIMENT_DB_NAME", "")).lstrip("/")
        return str ( PostgresDsn.build(
            scheme="postgresql", 
            username=data.get("DATABASE_USER") or "",
            password=data.get("DATABASE_PASSWORD") or "",
            host=data.get("DATABASE_HOST") or "localhost",
            port=int(data.get("DATABASE_PORT") or 5432),
            path=f"{db_name}"
        ))

    # ---------------- Initialize Async Connections ----------------
    @async_retry(max_attempts=4, base_delay=0.5, max_delay=3.0)
    async def init_async_db_pool(self):
        if not self.ASYNC_DATABASE_URI:
            raise RuntimeError("ASYNC_DATABASE_URI is not set")
        self.async_db_pool = await asyncpg.create_pool(
            dsn=self.ASYNC_DATABASE_URI,
            min_size=1,
            max_size=self.POOL_SIZE,
            timeout=60
        )
        logger.info("Initialized asyncpg pool (size=%d)", self.POOL_SIZE)

    @async_retry(max_attempts=4, base_delay=0.5, max_delay=3.0)
    async def init_redis_pool(self):
        url = self.REDIS_URL or f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}"
        self.redis_pool = redis.from_url(
            url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=self.POOL_SIZE
        )
        logger.info("Initialized Redis pool at %s", url)

    @async_retry(max_attempts=3, base_delay=0.3, max_delay=2.0)
    async def init_minio_client(self):
        if not self.MINIO_URL:
            raise RuntimeError("MINIO_URL is not set")
        secure = self.MINIO_URL.startswith("https://")
        endpoint = self.MINIO_URL.replace("https://", "").replace("http://", "")
        self.minio_client = Minio(
            endpoint=endpoint,
            access_key=self.MINIO_ROOT_USER or "",
            secret_key=self.MINIO_ROOT_PASSWORD or "",
            secure=secure
        )
        logger.info("MinIO client initialized (secure=%s)", secure)

    async def init_all_connections(self):
        await asyncio.gather(
            *(c() for c in [
                self.init_async_db_pool if self.ASYNC_DATABASE_URI else None,
                self.init_redis_pool if (self.REDIS_HOST or self.REDIS_URL) else None,
                self.init_minio_client if self.MINIO_URL else None,
            ] if c is not None)
        )
        logger.info("All configured connections initialized.")

    # ---------------- CORS ----------------
    BACKEND_CORS_ORIGINS: list[str] | list[AnyHttpUrl] = []

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
        return [origin.rstrip("/") for origin in self.BACKEND_CORS_ORIGINS or []]

    # ---------------- Validation ----------------
    @model_validator(mode="after")
    def check_required_secrets(self) -> "Settings":
        required = {
            "SECRET_KEY": self.SECRET_KEY,
            "DATABASE_PASSWORD": self.DATABASE_PASSWORD,
            "DATABASE_USER": self.DATABASE_USER,
        }
        for name, value in required.items():
            if self.MODE != "development" and (value is None or value == "" or value == "changethis"):
                raise ValueError(f"{name} is not set or insecure. Update in production!")
            if value is None and self.MODE == "development":
                warnings.warn(f"{name} is not set. Using insecure defaults in development.")
        return self

    # ---------------- Shutdown helpers ----------------
    async def shutdown(self):
        if self.async_db_pool:
            await self.async_db_pool.close()
            logger.info("Closed async DB pool")
        if self.redis_pool:
            try:
                await self.redis_pool.close()
                logger.info("Closed redis pool")
            except Exception:
                logger.exception("Error closing redis pool")

    # ---------------- Model Config ----------------
    model_config = SettingsConfigDict(
        env_file=os.path.expanduser(".env"),
        case_sensitive=True,
        extra="ignore",
    )


# ---------------- Instantiate & Bootstrap ----------------
settings = Settings()

async def bootstrap():
    if settings.VAULT_URL and settings.VAULT_TOKEN:
        logger.info("Loading secrets from Vault")
        await settings.load_secrets_from_vault_async()
    await settings.init_all_connections()


if __name__ == "__main__":
    try:
        asyncio.run(bootstrap())
    except Exception:
        logger.exception("Bootstrap failed")
        raise
