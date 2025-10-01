# src/app/api/auth.py
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from jose import jwt, JWTError

from src.app.crud.admin import ensure_superuser
from src.app.core.db import get_session
from src.app.schemas.user import create_user_schema
from src.app.core.security import (
    ALGORITHM, AUDIENCE, ISSUER, create_access_token, create_refresh_token, revoke_token, rotate_and_save_refresh,
    verify_password, hash_password, decode_token
)
from src.app.core.redis_cache import (
    add_login_history, set_cache, get_cache, delete_cache,
    is_rate_limited, record_failed_login, is_locked, ip_is_allowed,
    get_refresh_token_for_user, revoke_user_refresh_token, blacklist_token
)
from src.app.core.config import settings

router = APIRouter(prefix="/auth", tags=["auth"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/auth/login")

# rate-limit settings
LOGIN_RATE_LIMIT = getattr(settings, "LOGIN_RATE_LIMIT", 10)
LOGIN_RATE_WINDOW = getattr(settings, "LOGIN_RATE_WINDOW", 60)
FAILED_LOCK_THRESHOLD = getattr(settings, "FAILED_LOCK_THRESHOLD", 5)
FAILED_LOCK_SECONDS = getattr(settings, "FAILED_LOCK_SECONDS", 900)


@router.post("/register")
async def register_user(email: str, password: str, db: AsyncSession = Depends(get_session)):
    schema_name = await create_user_schema(email, db)
    await db.execute(
        text(f'INSERT INTO "{schema_name}".master_user (email, hashed_password) VALUES (:email, :hashed_password)'),
        {"email": email, "hashed_password": hash_password(password)}
    )
    await db.execute(
        text('INSERT INTO public.user_registry (email, schema_name, is_active, is_superuser, created_at) VALUES (:email, :schema, :active, :admin, now())'),
       {"email": email, "schema": schema_name, "active": True, "admin": False}
    )
    await db.commit()
    return {"message": "User registered successfully", "schema": schema_name}




@router.post("/login")
async def login_user(
    form_data: OAuth2PasswordRequestForm = Depends(),
    request: Request = None,
    db: AsyncSession = Depends(get_session),
):
    email = form_data.username
    password = form_data.password
    ip = request.client.host if request else "unknown"

    # Superuser login 
    if email == settings.FIRST_SUPERUSER_EMAIL:
        schema_name = "public"
        table_name = "admin"
        await ensure_superuser(db)
        user_reg = None 
    else:
        # Normal user login 
        res = await db.execute(
            text('SELECT * FROM public.user_registry WHERE email=:email'),
            {"email": email}
        )
        user_reg = res.fetchone()
        if not user_reg:
            raise HTTPException(status_code=404, detail="User not found")
        if not user_reg.is_active:
            raise HTTPException(status_code=403, detail="User account is blocked")

        schema_name = user_reg.schema_name
        table_name = "master_user"

    #  IP allow
    if not await ip_is_allowed(ip):
        raise HTTPException(status_code=403, detail="IP not allowed")

    # Rate limiting
    if await is_rate_limited(f"ip:{ip}", LOGIN_RATE_LIMIT, LOGIN_RATE_WINDOW):
        raise HTTPException(status_code=429, detail="Too many requests from IP")
    if await is_rate_limited(f"user:{email}", LOGIN_RATE_LIMIT, LOGIN_RATE_WINDOW):
        raise HTTPException(status_code=429, detail="Too many requests for user")

    # Check lock
    if await is_locked(email):
        raise HTTPException(status_code=423, detail="Account temporarily locked due to failed logins")

    # Fetch user from schema
    res = await db.execute(
        text(f'SELECT * FROM "{schema_name}".{table_name} WHERE email=:email'),
        {"email": email},
    )
    user = res.fetchone()
    if not user or not verify_password(password, user.hashed_password):
        await record_failed_login(
            email,
            window_seconds=FAILED_LOCK_SECONDS,
            lock_threshold=FAILED_LOCK_THRESHOLD,
            lock_seconds=FAILED_LOCK_SECONDS,
        )
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Clear failed login attempts
    await delete_cache(f"fail:{email}")

    # 8️⃣ Tokens Create
    if email == settings.FIRST_SUPERUSER_EMAIL:
        roles = ["superuser"]
    else:
        roles = ["admin"] if user_reg.is_superuser else ["user"]

    access_token = create_access_token({"sub": email}, roles=roles)
    refresh_token = create_refresh_token({"sub": email})

    # Refresh token save 
    await rotate_and_save_refresh(email, refresh_token)

    # Login history
    await add_login_history(email, ip=ip)
    await set_cache(f"{email}:last_login", {"ip": ip, "time": str(datetime.utcnow())}, expire=86400)

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
    }


# Refresh Token Endpoint

@router.post("/refresh")
async def refresh_token_endpoint(token: str = Depends(oauth2_scheme)):

    try:
        payload = await decode_token(token, refresh=True, allow_expired=True)
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")

    user_email = payload.get("sub")
    if not user_email:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    # New refresh token create & rotate
    new_refresh = await rotate_and_save_refresh(user_email, token)
    new_access = create_access_token({"sub": user_email})

    return {
        "access_token": new_access,
        "refresh_token": new_refresh,
        "token_type": "bearer",
    }

# Logout Endpoint
@router.post("/logout")
async def logout(token: str = Depends(oauth2_scheme)):

    await revoke_token(token)
    return {"detail": "Successfully logged out"}

#forgot-password
@router.post("/forgot-password")
async def forgot_password(email: str):
    reset_token = create_refresh_token({"sub": email}, expires_delta=timedelta(minutes=30))
    await set_cache(f"{email}:reset_password", {"token": reset_token}, expire=1800)
    return {"message": "Password reset link sent (simulated)", "reset_token": reset_token}

#reset-password
@router.post("/reset-password")
async def reset_password(email: str, token: str, new_password: str, db: AsyncSession = Depends(get_session)):
    cache = await get_cache(f"{email}:reset_password")
    if not cache or cache.get("token") != token:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")

    schema_name = f"user_{email.lower().replace('@','_at_').replace('.','_dot_')}"
    await db.execute(
        text(f'UPDATE "{schema_name}".master_user SET hashed_password=:hashed_password WHERE email=:email'),
        {"hashed_password": hash_password(new_password), "email": email}
    )
    await db.commit()
    await delete_cache(f"{email}:reset_password")
    await revoke_user_refresh_token(email)
    return {"message": "Password reset successfully"}


@router.get("/me")
async def get_current_user(token: str = Depends(oauth2_scheme)):
    payload = await decode_token(token, refresh=False, allow_expired=True)

    email = payload.get("sub")
    if not email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token: missing subject",
        )

    login_history = await get_cache(f"{email}:login_history")
    exp = payload.get("exp")
    is_expired = exp < int(datetime.utcnow().timestamp())

    return {
        "email": email,
        "login_history": login_history,
        "expired": is_expired
    }


