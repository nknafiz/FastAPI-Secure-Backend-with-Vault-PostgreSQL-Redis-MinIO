# src/app/main.py
from contextlib import asynccontextmanager
import anyio
from fastapi import FastAPI

from src.app.core.db import init_db, init_sync_db
from src.app.core.config import settings
import logging

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        print("Starting your server")

        # 1️⃣ Load Vault secrets (async)
        if settings.VAULT_URL and settings.VAULT_TOKEN:
            logger.info("Loading secrets from Vault...")
            await settings.load_secrets_from_vault_async()
            logger.info("Vault secrets loaded")

        # 2️⃣ Initialize all async connections (DB / Redis / MinIO)
        await settings.init_all_connections()
        print(f"Async DB URI: {settings.ASYNC_DATABASE_URI}")
        print(f"Sync Experiment DB URI: {settings.SYNC_EXPERIMENT_DB_URI}")

        # 3️⃣ Access Vault client (sync, no await)
        vault_client = settings.vault_client
        logger.info("Vault client ready: %s", vault_client.is_authenticated())
        await init_db()
        init_sync_db()
        

        yield  # FastAPI app runs here

    except Exception as e:
        logger.exception("Error during app startup: %s", e)
        raise
    finally:
        # Shutdown connections
        await settings.shutdown()
        print("Shutting down your server")


# ---------------- Create FastAPI app ----------------
app = FastAPI(
    title="Protfoliyo",
    version=settings.API_VERSION,
    lifespan=lifespan
)

# ---------------- Example route ----------------
@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "async_db_uri": settings.ASYNC_DATABASE_URI,
        "sync_exp_db_uri": settings.SYNC_EXPERIMENT_DB_URI
    }
