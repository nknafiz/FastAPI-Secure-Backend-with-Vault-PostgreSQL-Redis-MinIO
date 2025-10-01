# src/app/core/redis_cache.py
import json
from typing import Optional
from datetime import datetime
from src.app.core.config import settings
from redis.asyncio import Redis

async_redis: Optional[Redis] = None

async def init_async_redis():
    global async_redis
    if async_redis is None:
        await settings.init_redis_pool()
        async_redis = settings.redis_pool
        pong = await async_redis.ping()
        print("Redis ping:", pong)


# basic json wrappers
async def set_cache(key: str, value, expire: int = 3600):
    if async_redis is None:
        raise RuntimeError("Redis not initialized")
    await async_redis.set(key, json.dumps(value), ex=expire)

async def get_cache(key: str):
    if async_redis is None:
        raise RuntimeError("Redis not initialized")
    val = await async_redis.get(key)
    if not val:
        return None
    try:
        return json.loads(val)
    except Exception:
        return val

async def delete_cache(key: str):
    if async_redis is None:
        raise RuntimeError("Redis not initialized")
    await async_redis.delete(key)


# refresh token per-user (hash) 
async def save_refresh_token_for_user(email: str, refresh_token: str, jti: str, expire_seconds: int):
    if async_redis is None:
        raise RuntimeError("Redis not initialized")
    key = f"user:{email}:refresh"
    await async_redis.hset(key, mapping={"token": refresh_token, "jti": jti})
    await async_redis.expire(key, expire_seconds)

async def get_refresh_token_for_user(email: str):
    if async_redis is None:
        raise RuntimeError("Redis not initialized")
    key = f"user:{email}:refresh"
    data = await async_redis.hgetall(key)
    if not data:
        return None
    return {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v for k, v in data.items()}

async def revoke_user_refresh_token(email: str):
    if async_redis is None:
        raise RuntimeError("Redis not initialized")
    key = f"user:{email}:refresh"
    await async_redis.delete(key)

async def rotate_and_save_refresh(email: str, new_refresh_token: str, jti: str = None, expire_seconds: int = 604800):
    from uuid import uuid4

    if not jti:
        jti = str(uuid4())

    await save_refresh_token_for_user(email, new_refresh_token, jti, expire_seconds)
    return jti

#  blacklist helpers
async def blacklist_token(jti: str, expire_seconds: int):
    if async_redis is None:
        raise RuntimeError("Redis not initialized")
    if not jti:
        return
    key = f"blacklist:{jti}"
    await async_redis.setex(key, expire_seconds, "1")

async def is_token_blacklisted(jti: str) -> bool:
    if async_redis is None:
        raise RuntimeError("Redis not initialized")
    if not jti:
        return False
    return await async_redis.exists(f"blacklist:{jti}") == 1


#login history 
async def add_login_history(email: str, ip: Optional[str] = None, limit: int = 50):
    key = f"{email}:login_history"
    entry = {"ip": ip, "time": str(datetime.utcnow())}
    existing = await get_cache(key) or []
    if not isinstance(existing, list):
        existing = []
    existing.insert(0, entry)
    existing = existing[:limit]
    await set_cache(key, existing, expire=86400)


#rate limiting & failed attempts
async def is_rate_limited(key_prefix: str, limit: int, window_seconds: int) -> bool:
    if async_redis is None:
        raise RuntimeError("Redis not initialized")
    key = f"rl:{key_prefix}"
    cur = await async_redis.incr(key)
    if cur == 1:
        await async_redis.expire(key, window_seconds)
    return cur > limit

async def record_failed_login(email: str, window_seconds: int = 300, lock_threshold: int = 5, lock_seconds: int = 900):
    if async_redis is None:
        raise RuntimeError("Redis not initialized")
    key = f"fail:{email}"
    cur = await async_redis.incr(key)
    if cur == 1:
        await async_redis.expire(key, window_seconds)
    if cur >= lock_threshold:
        await async_redis.setex(f"lock:{email}", lock_seconds, "1")
    return cur

async def is_locked(email: str) -> bool:
    if async_redis is None:
        raise RuntimeError("Redis not initialized")
    return await async_redis.exists(f"lock:{email}") == 1


# IP whitelist 
async def ip_is_allowed(ip: str) -> bool:
    wl = getattr(settings, "IP_WHITELIST", []) or []
    if wl and ip in wl:
        return True
    bw = getattr(settings, "IP_BLOCKLIST", []) or []
    if ip in bw:
        return False
    allowed_countries = getattr(settings, "ALLOWED_COUNTRIES", []) or []
    if not allowed_countries:
        return True
    return True  
