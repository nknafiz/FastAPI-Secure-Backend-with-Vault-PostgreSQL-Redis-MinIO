# src/app/main.py
from contextlib import asynccontextmanager
import anyio
from fastapi import FastAPI

from src.app.core.db import (
    init_experiment_db,
    init_main_db,
    startup_security_tasks,
    shutdown_tasks,
)
from src.app.core.config import settings

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting your server")
    print(f"Async DB URI: {settings.ASYNC_DATABASE_URI}")
    print(f"Sync Experiment DB URI: {settings.SYNC_EXPERIMENT_DB_URI}")

    # Initialize databases
    await init_main_db()
    await anyio.to_thread.run_sync(init_experiment_db)

    # Initialize engines, services, and background secret rotation
    await startup_security_tasks()

    yield

    # Shutdown tasks: cancel background rotation, close engines/clients
    await shutdown_tasks()
    print("Shutting down your server")

# Create FastAPI app with lifespan context
app = FastAPI(
    title="Protfoliyo",
    version=settings.API_VERSION,
    lifespan=lifespan
)

# Example route
@app.get("/health")
async def health_check():
    return {"status": "ok", "time": anyio.current_time()}
