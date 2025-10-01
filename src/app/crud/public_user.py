#src.app.crud.public_user.py

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update
from fastapi import HTTPException, status
from src.app.models.user_registry import UserRegistry


async def block_user(db: AsyncSession, email: str):
    result = await db.execute(select(UserRegistry).where(UserRegistry.email == email))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user.is_superuser:
        raise HTTPException(status_code=403, detail="Cannot block superadmin")
    await db.execute(update(UserRegistry).where(UserRegistry.email == email).values(is_active=False))
    await db.commit()


async def unblock_user(db: AsyncSession, email: str):
    result = await db.execute(select(UserRegistry).where(UserRegistry.email == email))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user.is_superuser:
        raise HTTPException(status_code=403, detail="Cannot unblock superadmin")
    await db.execute(update(UserRegistry).where(UserRegistry.email == email).values(is_active=True))
    await db.commit()


async def grant_admin(db: AsyncSession, email: str):
    result = await db.execute(select(UserRegistry).where(UserRegistry.email == email))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user.is_superuser:
        raise HTTPException(status_code=403, detail="Cannot modify superadmin")
    await db.execute(update(UserRegistry).where(UserRegistry.email == email).values(is_superuser=True))
    await db.commit()


async def revoke_admin(db: AsyncSession, email: str):
    result = await db.execute(select(UserRegistry).where(UserRegistry.email == email))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if user.is_superuser:
        raise HTTPException(status_code=403, detail="Cannot modify superadmin")
    await db.execute(update(UserRegistry).where(UserRegistry.email == email).values(is_superuser=False))
    await db.commit()


async def get_all_users(db: AsyncSession):
    result = await db.execute(select(UserRegistry))
    return result.scalars().all()
