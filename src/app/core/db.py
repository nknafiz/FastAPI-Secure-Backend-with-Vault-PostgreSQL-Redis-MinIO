# src/app/core/db.py
import asyncio
import logging
import random
from datetime import datetime, timedelta
from typing import AsyncGenerator, Generator, Optional
from contextlib import suppress

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlmodel import SQLModel
from redis.asyncio import Redis as AsyncRedis
from redis import Redis as SyncRedis
from minio import Minio

from src.app.core.initial_data import init_super_admin
from src.app.core.config import settings


# Logger
logger = logging.getLogger("db_core")
logger.setLevel(logging.DEBUG if settings.MODE == "development" else logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(ch)


# Globals
async_engine: Optional[AsyncEngine] = None
async_session_factory: Optional[sessionmaker] = None
sync_engine: Optional[create_engine] = None
sync_session_factory: Optional[sessionmaker] = None
async_redis: Optional[AsyncRedis] = None
sync_redis: Optional[SyncRedis] = None
minio_client: Optional[Minio] = None
VECTOR_DB_API_KEY: Optional[str] = None
DB_SEMAPHORE: Optional[asyncio.Semaphore] = None
_rotate_task: Optional[asyncio.Task] = None
_secret_cache: dict = {} 


# Vault helpers
async def _vault_read(path: str, key: str, ttl: int = 300) -> str:
    cache_key = f"{path}:{key}"
    now = datetime.utcnow()
    cached = _secret_cache.get(cache_key)
    if cached:
        value, expires = cached
        if now < expires:
            return value

    data = await settings.fetch_vault_secret_async(path)
    value = data.get(key)
    expires_at = now + timedelta(seconds=int(ttl * 0.9))
    _secret_cache[cache_key] = (value, expires_at)
    return value


# Retry decorator
def retry_async(max_attempts: int = 3, base_delay: float = 0.5):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        raise
                    await asyncio.sleep(delay)
                    delay *= 2
        return wrapper
    return decorator


# Async Engine
def _build_async_engine(db_url: str) -> AsyncEngine:
    return create_async_engine(
        db_url,
        echo=settings.MODE == "development",
        pool_size=settings.POOL_SIZE,
        max_overflow=5,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        future=True,
    )

async def _validate_async_engine(engine: AsyncEngine):
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))

@retry_async()
async def create_async_db_engine() -> None:
    global async_engine, async_session_factory, DB_SEMAPHORE

    user = await _vault_read(settings.VAULT_DB_MAIN_PATH, "username")
    password = await _vault_read(settings.VAULT_DB_MAIN_PATH, "password")
    host = settings.DATABASE_HOST
    port = settings.DATABASE_PORT
    db = settings.DATABASE_NAME

    url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}"
    engine = _build_async_engine(url)
    session_factory = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False, autoflush=False, future=True)

    await _validate_async_engine(engine)

    async_engine = engine
    async_session_factory = session_factory

    if DB_SEMAPHORE is None:
        DB_SEMAPHORE = asyncio.Semaphore(settings.POOL_SIZE)


# Sync Engine
def _build_sync_engine(db_url: str):
    return create_engine(
        db_url,
        echo=settings.MODE == "development",
        pool_size=max(settings.POOL_SIZE // 2, 5),
        max_overflow=5,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        future=True,
    )

def create_sync_db_engine():
    global sync_engine, sync_session_factory
    user = settings.DATABASE_USER
    password = settings.DATABASE_PASSWORD
    host = settings.DATABASE_HOST
    port = settings.DATABASE_PORT
    db = settings.EXPERIMENT_DB_NAME or "experiment"

    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = _build_sync_engine(url)
    sync_engine = engine
    sync_session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


# Session Providers
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    if async_session_factory is None:
        await create_async_db_engine()
    assert DB_SEMAPHORE is not None
    async with DB_SEMAPHORE:
        async with async_session_factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

def get_sync_session() -> Generator:
    if sync_session_factory is None:
        create_sync_db_engine()
    with sync_session_factory() as session:
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


# Redis / MinIO / VectorDB
@retry_async()
async def init_services():
    global async_redis, sync_redis, minio_client, VECTOR_DB_API_KEY

    # Redis
    redis_pass = await _vault_read(settings.VAULT_REDIS_PATH, "password")
    url = settings.REDIS_URL or f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}"
    async_redis = AsyncRedis.from_url(url, decode_responses=True, ssl=True, password=redis_pass)
    sync_redis = SyncRedis.from_url(url, decode_responses=True, ssl=True, password=redis_pass)
    logger.info("Redis initialized at %s", url)

    # MinIO
    minio_secret = await _vault_read(settings.VAULT_MINIO_PATH, "secret_key")
    minio_client = Minio(
        endpoint=settings.MINIO_URL,
        access_key=settings.MINIO_ROOT_USER,
        secret_key=minio_secret,
        secure=True
    )
    logger.info("MinIO client initialized")

    # VectorDB
    VECTOR_DB_API_KEY = await _vault_read(settings.VAULT_VECTOR_PATH, "api_key")


# Initialize DB tables
async def init_db():
    if async_engine is None:
        await create_async_db_engine()
        from src.app.models.admin import Admin 
        from src.app.models.user_registry import UserRegistry
    async with async_engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    logger.info("Async DB initialized")

def init_sync_db():
    if sync_engine is None:
        create_sync_db_engine()
    SQLModel.metadata.create_all(bind=sync_engine)
    logger.info("Sync DB initialized")


# Secret rotation
async def rotate_secrets(interval_sec: int = 300):
    consecutive_fail = 0
    backoff = 1.0
    while True:
        jitter = random.uniform(-0.2, 0.2) * interval_sec
        await asyncio.sleep(max(0, interval_sec + jitter))
        try:
            await create_async_db_engine()
            await init_services()
            logger.info("Secrets rotated at %s", datetime.utcnow().isoformat())
            consecutive_fail = 0
            backoff = 1.0
        except asyncio.CancelledError:
            logger.info("Secret rotation cancelled")
            raise
        except Exception as e:
            consecutive_fail += 1
            logger.error("Secret rotation error: %s (consecutive %d)", e, consecutive_fail)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
            if consecutive_fail >= 5:
                logger.critical("Secret rotation failing consecutively: alert ops")
                await asyncio.sleep(30)
                consecutive_fail = 0


# FastAPI startup / shutdown
async def startup():
    await create_async_db_engine()
    create_sync_db_engine()
    await init_services()
    async for session in get_session():
        await init_super_admin(session)

    global _rotate_task
    if _rotate_task is None or _rotate_task.done():
        _rotate_task = asyncio.create_task(rotate_secrets())

async def shutdown():
    global async_engine, async_redis, sync_redis, minio_client, _rotate_task
    if _rotate_task:
        _rotate_task.cancel()
        with suppress(asyncio.CancelledError):
            await _rotate_task
    if async_engine:
        with suppress(Exception):
            await async_engine.dispose()
    if async_redis:
        with suppress(Exception):
            await async_redis.close()
    if sync_redis:
        with suppress(Exception):
            sync_redis.close()
    minio_client = None
    logger.info("Shutdown complete")