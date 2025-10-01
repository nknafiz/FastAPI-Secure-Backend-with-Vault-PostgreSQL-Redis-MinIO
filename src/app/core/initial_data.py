# src/app/core/initial_data.py
import logging
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlmodel import SQLModel

from src.app.models.admin import Admin
from src.app.schemas.admin import AdminCreate
from src.app.crud import admin as crud_admin
from src.app.core.config import settings

logger = logging.getLogger(__name__)

async def init_super_admin(session: AsyncSession):

    email = settings.FIRST_SUPERUSER_EMAIL
    password = settings.FIRST_SUPERUSER_PASSWORD

    if not email or not password:
        logger.warning("Superuser credentials not set in .env, skipping superuser creation")
        return None

    # check if super admin exists
    result = await session.execute(select(Admin).where(Admin.email == email))
    existing = result.scalar_one_or_none()
    if existing:
        logger.info("Super Admin already exists: %s", email)
        return existing

    # create super admin
    admin_in = AdminCreate(
        email=email,
        password=password,
        schema_name="public", 
    )
    new_admin = await crud_admin.create_admin(session, admin_in)
    logger.info("Super Admin created: %s", email)
    return new_admin
