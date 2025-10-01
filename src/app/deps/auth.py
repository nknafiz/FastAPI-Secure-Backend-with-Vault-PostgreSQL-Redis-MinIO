#src.app.deps.auth.py

from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from jose import jwt, JWTError

from src.app.core.config import settings
from src.app.core.db import get_session
from src.app.models.user_registry import UserRegistry
from src.app.core.security import decode_token

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/auth/login")


async def get_current_superadmin(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_session)
):
    try:
        payload = await decode_token(token)
        email: str = payload.get("sub")
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")

        result = await db.execute(select(UserRegistry).where(UserRegistry.email == email))
        user = result.scalar_one_or_none()
        if not user:
            raise HTTPException(status_code=401, detail="User not found")

        if not user.is_active:
            raise HTTPException(status_code=403, detail="User is blocked")

        if not user.is_superuser:
            raise HTTPException(status_code=403, detail="Admin access required")

        return user
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
