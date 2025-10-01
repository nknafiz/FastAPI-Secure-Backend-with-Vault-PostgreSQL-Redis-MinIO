# src/app/main.py
from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI
from src.app.core.config import settings
from src.app.core.db import init_db, init_sync_db
from src.app.core.redis_cache import init_async_redis, async_redis

from src.app.api.auth import router as auth_router
from src.app.api.admin import router as admin_router

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        print("Starting server...")

        # Load Vault secrets (async)
        if settings.VAULT_URL and settings.VAULT_TOKEN:
            logger.info("Loading secrets from Vault...")
            await settings.load_secrets_from_vault_async()
            logger.info("Vault secrets loaded")

        #  Initialize DB
        await init_db()
        init_sync_db()
        logger.info("DB initialized")

        # Initialize Redis
        await init_async_redis()
        
        

        yield

    finally:
        # Shutdown logic 
        from src.app.core.db import shutdown
        await shutdown()
        print("Shutting down server...")

# Create FastAPI app 
app = FastAPI(
    title="Protfoliyo",
    version=settings.API_VERSION,
    lifespan=lifespan
)

# Health check route


@app.get("/health")
async def health_check():
    redis_status = "not initialized"
    if settings.redis_pool:
        try:
            pong = await settings.redis_pool.ping()
            redis_status = "alive" if pong else "dead"
        except Exception:
            redis_status = "error"

    return {
        "status": "ok",
        "async_db_uri": str(settings.ASYNC_DATABASE_URI),
        "sync_exp_db_uri": str(settings.SYNC_EXPERIMENT_DB_URI),
        "redis": redis_status
    }





# Include Auth router
app.include_router(auth_router, prefix="/auth")
app.include_router(admin_router, prefix="/admin")
