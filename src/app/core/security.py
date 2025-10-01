# src/app/core/security.py
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from uuid import uuid4

from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi import HTTPException, status

from src.app.core.config import settings
from src.app.core.redis_cache import (
    is_token_blacklisted,
    save_refresh_token_for_user,
    revoke_user_refresh_token,
    blacklist_token,
)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

ALGORITHM = settings.JWT_ALGORITHM
ACCESS_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES
REFRESH_EXPIRE_MINUTES = settings.REFRESH_TOKEN_EXPIRE_MINUTES
ISSUER = getattr(settings, "JWT_ISSUER", "protfoliyo")
AUDIENCE = getattr(settings, "JWT_AUDIENCE", "protfoliyo_users")


# Password
def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(password: str, hashed: str) -> bool:
    return pwd_context.verify(password, hashed)


def _now_ts() -> int:
    return int(datetime.utcnow().timestamp())



# create_access_token
def create_access_token(
    data: dict,
    expires_delta: Optional[timedelta] = None,
    roles: Optional[List[str]] = None
) -> str:
    if "sub" not in data or not isinstance(data["sub"], str):
        raise ValueError("data must contain 'sub' as string")

    jti = str(uuid4())
    iat = _now_ts()
    exp = iat + int((expires_delta or timedelta(minutes=ACCESS_EXPIRE_MINUTES)).total_seconds())

    payload = {
        **data,
        "jti": jti,
        "iat": iat,
        "exp": exp,
        "iss": ISSUER,
        "aud": AUDIENCE,
        "roles": roles or [],
    }

    return jwt.encode(payload, settings.SECRET_KEY, algorithm=ALGORITHM)

# create_refresh_token
def create_refresh_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    if "sub" not in data or not isinstance(data["sub"], str):
        raise ValueError("data must contain 'sub' as string")

    jti = str(uuid4())
    iat = _now_ts()
    exp = iat + int((expires_delta or timedelta(minutes=REFRESH_EXPIRE_MINUTES)).total_seconds())

    payload = {
        **data,
        "jti": jti,
        "iat": iat,
        "exp": exp,
        "iss": ISSUER,
        "aud": AUDIENCE,
    }

    return jwt.encode(payload, settings.JWT_REFRESH_TOKEN_KEY, algorithm=ALGORITHM)



# decode_token
async def decode_token(token: str, refresh: bool = False, allow_expired: bool = False) -> Dict[str, Any]:
    """
    Decode access/refresh token
    - যদি allow_expired=True হয়, তাহলে expiry যাচাই করবে না
    """
    key = settings.JWT_REFRESH_TOKEN_KEY if refresh else settings.SECRET_KEY
    try:
        payload = jwt.decode(
            token,
            key,
            algorithms=[ALGORITHM],
            issuer=ISSUER,
            options={
                "verify_aud": False,
                "verify_exp": not allow_expired  
            },
        )

        # Blacklist check 
        jti = payload.get("jti")
        if jti and await is_token_blacklisted(jti):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token revoked")

        if not payload.get("sub"):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token missing 'sub'")

        return payload

    except JWTError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=f"Invalid token: {str(e)}")



# Logout / Revoke token
async def revoke_token(token: str, refresh: bool = False):
   
    payload = await decode_token(token, refresh=False, allow_expired=True)

    email = payload.get("sub")
    jti = payload.get("jti")
    exp = payload.get("exp", 0)

    if not jti or not email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Token missing jti or sub claim"
        )

    expire_seconds = max(int(exp - int(datetime.utcnow().timestamp())), 0)

    #  blacklist access token 
    await blacklist_token(jti, expire_seconds)

    #  revoke refresh token
    await revoke_user_refresh_token(email)


# Refresh rotation
async def rotate_and_save_refresh(user_id: str, old_token: Optional[str] = None) -> str:
    if old_token:
        try:
            await revoke_token(old_token, refresh=True)
        except Exception:
            pass

    # new_refresh_token 
    new_refresh = create_refresh_token({"sub": str(user_id)})


    payload = jwt.decode(
        new_refresh,
        settings.JWT_REFRESH_TOKEN_KEY,
        algorithms=[ALGORITHM],
        options={"verify_aud": False},
    )

    jti = payload.get("jti")
    exp = payload.get("exp")
    expire_seconds = max(int(exp - int(datetime.utcnow().timestamp())), 0)

    # Redis_save 
    await save_refresh_token_for_user(user_id, jti, new_refresh, expire_seconds)

    return new_refresh