#src.app.curd.admin.py

from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select
from src.app.models.admin import Admin
from src.app.schemas.admin import AdminCreate
from src.app.core.config import settings
from src.app.core.security import hash_password

async def get_admin_by_email(session: AsyncSession, email: str) -> Admin | None:
    result = await session.execute(select(Admin).where(Admin.email == email))
    return result.scalar_one_or_none()

async def create_admin(db: AsyncSession, email: str, password: str, schema_name: str = "public"):
    hashed_password = hash_password(password)
    admin = Admin(
        email=email,
        hashed_password=hashed_password,
        is_superuser=True,
        schema_name=schema_name,
        is_active=True,
        created_at=datetime.utcnow()
    )
    db.add(admin)
    await db.commit()
    await db.refresh(admin)
    return admin


async def ensure_superuser(db: AsyncSession):
    result = await db.execute(
        select(Admin).where(Admin.email == settings.FIRST_SUPERUSER_EMAIL)
    )
    admin = result.scalar_one_or_none()
    if not admin:
        await create_admin(
            db,
            email=settings.FIRST_SUPERUSER_EMAIL,
            password=settings.FIRST_SUPERUSER_PASSWORD,
            schema_name="public" 
        )
