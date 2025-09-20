# src/app/core/db.py
import asyncio
import logging
import anyio
import random
from datetime import datetime, timedelta
from typing import AsyncGenerator, Generator, Optional, Callable, Any
from contextlib import suppress

import hvac
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, DBAPIError
from sqlalchemy import text, create_engine
from sqlmodel import SQLModel
from redis.asyncio import Redis as AsyncRedis
from redis import Redis as SyncRedis
from minio import Minio

from src.app.core.config import settings

# -------------------------
# Logger
# -------------------------
logger = logging.getLogger("bank_db")
logger.setLevel(logging.DEBUG if settings.MODE == "development" else logging.INFO)
ch = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
ch.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(ch)

# -------------------------
# Vault client (sync hvac)
# -------------------------
vault_client = hvac.Client(url=settings.VAULT_URL, token=settings.VAULT_TOKEN)

# -------------------------
# Globals
# -------------------------
MAIN_DATABASE_URL: str = ""
EXPERIMENT_DATABASE_URL: str = ""
async_engine: Optional[AsyncEngine] = None
async_session_factory: Optional[sessionmaker] = None
sync_engine: Optional[create_engine] = None
sync_session_factory: Optional[sessionmaker] = None
async_redis: Optional[AsyncRedis] = None
sync_redis: Optional[SyncRedis] = None
minio_client: Optional[Minio] = None
VECTOR_DB_API_KEY: str = ""
_rotate_task: Optional[asyncio.Task] = None
DB_SEMAPHORE: Optional[asyncio.Semaphore] = None

# secret cache
_secret_cache: dict = {}  # key -> (value, expires_at)

# -------------------------
# Helpers: non-blocking vault read + cache
# -------------------------
async def _sync_vault_read(path: str) -> dict:
    """Run hvac read in threadpool (sync)"""
    def read():
        return vault_client.secrets.kv.v2.read_secret_version(path=path)
    return await anyio.to_thread.run_sync(read)

async def get_ephemeral_secret(path: str, key: str, ttl_seconds: int = 300) -> str:
    """
    - Non-blocking vault read
    - Simple in-process cache with safety margin
    """
    cache_key = f"{path}:{key}"
    now = datetime.utcnow()
    cached = _secret_cache.get(cache_key)
    if cached:
        value, expires_at = cached
        if now < expires_at:
            return value

    read = await _sync_vault_read(path)
    value = read["data"]["data"][key]
    # cache with safety margin 10%
    expires_at = now + timedelta(seconds=int(ttl_seconds * 0.9))
    _secret_cache[cache_key] = (value, expires_at)
    return value

# -------------------------
# Retry decorator for async funcs
# -------------------------
def retry_async(max_attempts: int = 3, base_delay: float = 0.5):
    def deco(func: Callable):
        async def wrapper(*a, **kw):
            delay = base_delay
            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*a, **kw)
                except Exception as e:
                    if attempt == max_attempts:
                        raise
                    await asyncio.sleep(delay)
                    delay *= 2
        return wrapper
    return deco

# -------------------------
# Create engines (return new instances, do not swap globals here)
# -------------------------
def _build_async_engine(db_url: str) -> AsyncEngine:
    return create_async_engine(
        url=db_url,
        echo=(settings.MODE == "development"),
        pool_size=settings.POOL_SIZE,
        max_overflow=5,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        future=True,
        connect_args={"sslmode": "require"} if settings.MODE == "production" else {},
    )

def _build_sync_engine(db_url: str):
    return create_engine(
        url=db_url,
        echo=(settings.MODE == "development"),
        pool_size=max(settings.POOL_SIZE // 2, 5),
        max_overflow=5,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        future=True,
        connect_args={"sslmode": "require"} if settings.MODE == "production" else {},
    )

# -------------------------
# Validate engine by a lightweight probe
# -------------------------
async def _validate_async_engine(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))

def _validate_sync_engine(engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

# -------------------------
# Functions that create & atomically swap global engines
# -------------------------
async def create_async_db_engine() -> None:
    """
    Build a new async engine using current Vault secrets, validate it,
    then atomically swap globals. Non-blocking Vault reads used.
    """
    global MAIN_DATABASE_URL, async_engine, async_session_factory, DB_SEMAPHORE

    db_user = await get_ephemeral_secret(settings.VAULT_DB_MAIN_PATH, "username")
    db_pass = await get_ephemeral_secret(settings.VAULT_DB_MAIN_PATH, "password")
    new_url = f"postgresql+asyncpg://{db_user}:{db_pass}@{settings.DATABASE_HOST}:{settings.DATABASE_PORT}/{settings.DATABASE_NAME}"

    new_engine = _build_async_engine(new_url)
    new_session_factory = sessionmaker(
        bind=new_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        future=True,
    )

    # validate before swap
    try:
        await _validate_async_engine(new_engine)
    except Exception:
        await new_engine.dispose()
        logger.exception("Validation failed for new async engine")
        raise

    # atomic swap
    old_engine = async_engine
    async_engine = new_engine
    async_session_factory = new_session_factory
    MAIN_DATABASE_URL = new_url

    # update DB semaphore if not set
    if DB_SEMAPHORE is None:
        DB_SEMAPHORE = asyncio.Semaphore(settings.POOL_SIZE)

    # gracefully dispose old engine
    if old_engine is not None:
        try:
            await old_engine.dispose()
        except Exception:
            logger.exception("Error disposing old async engine")

def create_sync_db_engine() -> None:
    """
    Build & swap sync engine for experiment DB
    """
    global EXPERIMENT_DATABASE_URL, sync_engine, sync_session_factory

    # sync vault reads (these are blocking) -> run in thread if startup called inside eventloop
    read_user = vault_client.secrets.kv.v2.read_secret_version(path=settings.VAULT_DB_MAIN_PATH)
    db_user = read_user["data"]["data"]["username"]
    read_exp = vault_client.secrets.kv.v2.read_secret_version(path=settings.VAULT_DB_EXP_PATH)
    db_pass = read_exp["data"]["data"]["password"]

    new_url = f"postgresql+psycopg2://{db_user}:{db_pass}@{settings.DATABASE_HOST}:{settings.DATABASE_PORT}/{settings.EXPERIMENT_DB_NAME}"
    new_engine = _build_sync_engine(new_url)
    new_session_factory = sessionmaker(
        bind=new_engine,
        autoflush=False,
        autocommit=False,
        future=True,
    )

    # validate
    try:
        _validate_sync_engine(new_engine)
    except Exception:
        with suppress(Exception):
            new_engine.dispose()
        logger.exception("Validation failed for new sync engine")
        raise

    old_engine = sync_engine
    sync_engine = new_engine
    sync_session_factory = new_session_factory
    EXPERIMENT_DATABASE_URL = new_url

    if old_engine is not None:
        with suppress(Exception):
            old_engine.dispose()

# -------------------------
# Session providers
# -------------------------
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    if async_session_factory is None:
        await create_async_db_engine()
    # semaphore for DB concurrency control (per process)
    assert DB_SEMAPHORE is not None, "DB_SEMAPHORE not initialized"
    async with DB_SEMAPHORE:
        async with async_session_factory() as session:
            try:
                yield session
            except (SQLAlchemyError, DBAPIError) as e:
                logger.error("Main DB session error: %s", e)
                await session.rollback()
                raise
            finally:
                await session.close()

def get_experiment_session() -> Generator:
    if sync_session_factory is None:
        create_sync_db_engine()
    with sync_session_factory() as session:
        try:
            yield session
        except (SQLAlchemyError, DBAPIError) as e:
            logger.error("Experiment DB session error: %s", e)
            session.rollback()
            raise
        finally:
            session.close()

# -------------------------
# Services setup (Redis, MinIO, VectorDB)
# -------------------------
async def setup_services() -> None:
    global async_redis, sync_redis, minio_client, VECTOR_DB_API_KEY

    redis_pass = await get_ephemeral_secret(settings.VAULT_REDIS_PATH, "password")
    async_redis = AsyncRedis.from_url(settings.REDIS_URL, decode_responses=True, ssl=True, password=redis_pass)
    sync_redis = SyncRedis.from_url(settings.REDIS_URL, decode_responses=True, ssl=True, password=redis_pass)

    minio_secret = await get_ephemeral_secret(settings.VAULT_MINIO_PATH, "secret_key")
    minio_client = Minio(
        endpoint=settings.MINIO_URL,
        access_key=settings.MINIO_ROOT_USER,
        secret_key=minio_secret,
        secure=True,
    )

    VECTOR_DB_API_KEY = await get_ephemeral_secret(settings.VAULT_VECTOR_PATH, "api_key")

# -------------------------
# Secret rotation: jitter + backoff + circuit-breaker alerting
# -------------------------
async def rotate_secrets_periodically(interval_sec: int = 60, max_consecutive_fail: int = 5):
    """
    Background task: rotates secrets periodically.
    Uses jitter on interval, exponential backoff on failure, and opens a 'circuit'
    (just logs and optionally could notify) if many consecutive failures occur.
    """
    consecutive_fail = 0
    backoff = 1.0

    while True:
        # jittered sleep to avoid thundering herd across instances
        jitter = random.uniform(-0.2, 0.2) * interval_sec
        await asyncio.sleep(max(0.0, interval_sec + jitter))
        try:
            # create engines + services using latest secrets and swap atomically
            await create_async_db_engine()
            # sync engine creation may block; run in thread
            await anyio.to_thread.run_sync(create_sync_db_engine)
            await setup_services()

            logger.info("Ephemeral secrets rotated at %s", datetime.utcnow().isoformat())
            consecutive_fail = 0
            backoff = 1.0
        except asyncio.CancelledError:
            logger.info("rotate_secrets_periodically cancelled")
            raise
        except Exception as e:
            consecutive_fail += 1
            logger.error("Error rotating secrets: %s (consecutive %d)", e, consecutive_fail)
            # exponential backoff on repeated failures
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
            if consecutive_fail >= max_consecutive_fail:
                logger.critical("Secret rotation failing %d times: open circuit & alert ops", consecutive_fail)
                # TODO: integrate with alerting/ops here (PagerDuty/Sentry/email)
                # After alerting we keep trying but with larger backoff
                await asyncio.sleep(30)
                consecutive_fail = 0  # optionally reset after alert

# -------------------------
# DB Initialization helpers
# -------------------------
async def init_main_db() -> None:
    if async_engine is None:
        await create_async_db_engine()
    async with async_engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    logger.info("Main async DB initialized")

def init_experiment_db() -> None:
    if sync_engine is None:
        create_sync_db_engine()
    SQLModel.metadata.create_all(bind=sync_engine)
    logger.info("Experiment DB initialized")

async def init_all_dbs() -> None:
    await init_main_db()
    await anyio.to_thread.run_sync(init_experiment_db)
    logger.info("All databases initialized")

# -------------------------
# Startup / Shutdown helpers for FastAPI
# -------------------------
async def startup_security_tasks() -> None:
    """
    Call this in FastAPI startup event.
    It ensures initial engines/services ready and launches rotation task.
    """
    global _rotate_task, DB_SEMAPHORE

    # initialize engines & services once
    await create_async_db_engine()
    await anyio.to_thread.run_sync(create_sync_db_engine)
    await setup_services()

    # initialize semaphore (per-process)
    if DB_SEMAPHORE is None:
        DB_SEMAPHORE = asyncio.Semaphore(settings.POOL_SIZE)

    # start rotation background task if not running
    if _rotate_task is None or _rotate_task.done():
        _rotate_task = asyncio.create_task(rotate_secrets_periodically(interval_sec=60))

async def shutdown_tasks() -> None:
    """
    Call this in FastAPI shutdown event.
    """
    global _rotate_task, async_engine, sync_engine, async_redis, sync_redis, minio_client

    # cancel rotation task
    if _rotate_task:
        _rotate_task.cancel()
        with suppress(asyncio.CancelledError):
            await _rotate_task

    # dispose engines and close clients
    if async_engine is not None:
        with suppress(Exception):
            await async_engine.dispose()

    if sync_engine is not None:
        with suppress(Exception):
            sync_engine.dispose()

    if async_redis is not None:
        with suppress(Exception):
            await async_redis.close()

    if sync_redis is not None:
        with suppress(Exception):
            sync_redis.close()

    if minio_client is not None:
        # minio client has no close method; rely on GC
        pass

    logger.info("Shutdown complete: DB engines disposed and services closed")
