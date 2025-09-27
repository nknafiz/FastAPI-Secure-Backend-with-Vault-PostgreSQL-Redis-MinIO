import os
import secrets
import warnings
import asyncio
from typing import Any, Optional
from pydantic import PostgresDsn, AnyHttpUrl, computed_field, field_validator, model_validator
from pydantic_core.core_schema import FieldValidationInfo
from pydantic_settings import BaseSettings, SettingsConfigDict
import hvac
import asyncpg
import aioredis
from minio import Minio

class Settings(BaseSettings):
    # ---------------- General ----------------
    MODE: str = "development"
    PROJECT_NAME: str
    API_VERSION: str
    API_V1_STR: str

    # ---------------- Security ----------------
    SECRET_KEY: str = secrets.token_urlsafe(32)
    ENCRYPT_KEY: str = secrets.token_urlsafe(32)
    JWT_ALGORITHM: str = "HS256"
    JWT_REFRESH_TOKEN_KEY: str = secrets.token_urlsafe(32)
    ACCESS_TOKENT_EXPIRE_MINUTES: int = 60
    REFRESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 100

    # ---------------- Main DB ----------------
    DATABASE_USER: str = ""
    DATABASE_PASSWORD: str = ""
    DATABASE_HOST: str = ""
    DATABASE_PORT: int = 5432
    DATABASE_NAME: str = ""
    ASYNC_DATABASE_URI: PostgresDsn | str = ""
    async_db_pool: Optional[asyncpg.pool.Pool] = None

    # ---------------- Experiment DB ----------------
    EXPERIMENT_DB_NAME: str = ""
    SYNC_EXPERIMENT_DB_URI: PostgresDsn | str = ""

    # ---------------- Redis ----------------
    REDIS_HOST: str = ""
    REDIS_PORT: int = 6379
    REDIS_URL: str = ""
    redis_pool: Optional[aioredis.Redis] = None

    # ---------------- Object Storage ----------------
    MINIO_ROOT_USER: str = ""
    MINIO_ROOT_PASSWORD: str = ""
    MINIO_URL: str = ""
    MINIO_BUCKET: str = ""
    minio_client: Optional[Minio] = None

    # ---------------- Vault ----------------
    VAULT_URL: str
    VAULT_TOKEN: str
    VAULT_DB_MAIN_PATH: str
    VAULT_DB_EXP_PATH: str
    VAULT_REDIS_PATH: str
    VAULT_MINIO_PATH: str
    VAULT_VECTOR_PATH: str

    # ---------------- Vector DB ----------------
    VECTOR_DB_URL: str = ""
    VECTOR_DB_API_KEY: Optional[str] = None

    # ---------------- Superuser ----------------
    FIRST_SUPERUSER_EMAIL: str
    FIRST_SUPERUSER_PASSWORD: str

    # ---------------- Performance ----------------
    DB_POOL_SIZE: int = 10
    WEB_CONCURRENCY: int = 2

    # ---------------- Computed Fields ----------------
    @computed_field
    @property
    def POOL_SIZE(self) -> int:
        return max(self.DB_POOL_SIZE // self.WEB_CONCURRENCY, 5)

    @computed_field
    @property
    def vault_client(self) -> hvac.Client:
        client = hvac.Client(url=self.VAULT_URL, token=self.VAULT_TOKEN)
        if not client.is_authenticated():
            raise RuntimeError("Vault authentication failed")
        return client

    # ---------------- Async Vault Secret Fetch ----------------
    async def fetch_vault_secret_async(self, path: str) -> dict:
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(None, self._fetch_vault_secret_safe, path)
        except Exception as e:
            warnings.warn(f"Failed to fetch Vault secret at {path}: {str(e)}")
            return {}

    def _fetch_vault_secret_safe(self, path: str) -> dict:
        try:
            secret = self.vault_client.secrets.kv.v2.read_secret_version(path=path)
            return secret.get("data", {}).get("data", {})
        except Exception as e:
            raise RuntimeError(f"Vault read error for path {path}: {e}")

    # ---------------- Load Vault Secrets ----------------
    async def load_secrets_from_vault_async(self):
        await asyncio.gather(
            self._load_main_db_secret(),
            self._load_experiment_db_secret(),
            self._load_redis_secret(),
            self._load_minio_secret(),
            self._load_vector_secret(),
        )

    async def _load_main_db_secret(self):
        db = await self.fetch_vault_secret_async(self.VAULT_DB_MAIN_PATH)
        self.DATABASE_USER = db.get("username", self.DATABASE_USER)
        self.DATABASE_PASSWORD = db.get("password", self.DATABASE_PASSWORD)
        self.DATABASE_HOST = db.get("host", self.DATABASE_HOST)
        self.DATABASE_PORT = int(db.get("port", self.DATABASE_PORT))
        self.DATABASE_NAME = db.get("dbname", self.DATABASE_NAME)

    async def _load_experiment_db_secret(self):
        exp = await self.fetch_vault_secret_async(self.VAULT_DB_EXP_PATH)
        self.EXPERIMENT_DB_NAME = exp.get("dbname", self.EXPERIMENT_DB_NAME)

    async def _load_redis_secret(self):
        redis = await self.fetch_vault_secret_async(self.VAULT_REDIS_PATH)
        self.REDIS_HOST = redis.get("host", self.REDIS_HOST)
        self.REDIS_PORT = int(redis.get("port", self.REDIS_PORT))

    async def _load_minio_secret(self):
        minio_sec = await self.fetch_vault_secret_async(self.VAULT_MINIO_PATH)
        self.MINIO_ROOT_USER = minio_sec.get("username", self.MINIO_ROOT_USER)
        self.MINIO_ROOT_PASSWORD = minio_sec.get("password", self.MINIO_ROOT_PASSWORD)
        self.MINIO_URL = minio_sec.get("url", self.MINIO_URL)
        self.MINIO_BUCKET = minio_sec.get("bucket", self.MINIO_BUCKET)

    async def _load_vector_secret(self):
        vector = await self.fetch_vault_secret_async(self.VAULT_VECTOR_PATH)
        self.VECTOR_DB_URL = vector.get("url", self.VECTOR_DB_URL)
        self.VECTOR_DB_API_KEY = vector.get("api_key", self.VECTOR_DB_API_KEY)

    # ---------------- DB URI Validators ----------------
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

    # ---------------- Initialize Async Connections ----------------
    async def init_async_db_pool(self):
        self.async_db_pool = await asyncpg.create_pool(
            dsn=self.ASYNC_DATABASE_URI,
            min_size=1,
            max_size=self.POOL_SIZE
        )

    async def init_redis_pool(self):
        self.redis_pool = await aioredis.from_url(
            f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}",
            encoding="utf-8",
            decode_responses=True
        )

    async def init_minio_client(self):
        self.minio_client = Minio(
            endpoint=self.MINIO_URL,
            access_key=self.MINIO_ROOT_USER,
            secret_key=self.MINIO_ROOT_PASSWORD,
            secure=self.MINIO_URL.startswith("https://")
        )

    async def init_all_connections(self):
        await asyncio.gather(
            self.init_async_db_pool(),
            self.init_redis_pool(),
            self.init_minio_client()
        )

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
        return [origin.rstrip("/") for origin in self.BACKEND_CORS_ORIGINS]

    # ---------------- Validation ----------------
    @model_validator(mode="after")
    def check_default_secrete(self) -> "Settings":
        for field in ["SECRET_KEY", "DATABASE_PASSWORD", "DATABASE_USER"]:
            value = getattr(self, field, None)
            if value == "changethis" or value in [None, ""]:
                message = f"{field} is not set or insecure. Update in production!"
                if self.MODE == "development":
                    warnings.warn(message)
                else:
                    raise ValueError(message)
        return self

    # ---------------- Model Config ----------------
    model_config = SettingsConfigDict(
        env_file=os.path.expanduser(".env"),
        case_sensitive=True,
        extra="ignore",
    )


# ---------------- Instantiate & Bootstrap ----------------
settings = Settings()

async def bootstrap():
    await settings.load_secrets_from_vault_async()
    await settings.init_all_connections()

# Run bootstrap at startup
asyncio.run(bootstrap())
