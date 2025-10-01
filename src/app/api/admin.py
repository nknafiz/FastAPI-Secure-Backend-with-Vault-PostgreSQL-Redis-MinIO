# src/app/api/admin.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from src.app.core.db import get_session
from src.app.crud.public_user import block_user, unblock_user, grant_admin, revoke_admin, get_all_users
from src.app.deps.auth import get_current_superadmin

router = APIRouter(prefix="/admin", tags=["admin"])

#all users
@router.get("/users")
async def list_users(
    db: AsyncSession = Depends(get_session),
    current_admin = Depends(get_current_superadmin)
):
    users = await get_all_users(db)
    return {"status": "success", "data": users}

#user block
@router.post("/block/{email}")
async def api_block_user(email: str, db: AsyncSession = Depends(get_session), current_admin = Depends(get_current_superadmin)):
    await block_user(db, email)
    return {"status": "success", "message": f"{email} blocked successfully"}

#user unblock
@router.post("/unblock/{email}")
async def api_unblock_user(email: str, db: AsyncSession = Depends(get_session), current_admin = Depends(get_current_superadmin)):
    await unblock_user(db, email)
    return {"status": "success", "message": f"{email} unblocked successfully"}

#admin access user
@router.post("/grant_admin/{email}")
async def api_grant_admin(email: str, db: AsyncSession = Depends(get_session), current_admin = Depends(get_current_superadmin)):
    await grant_admin(db, email)
    return {"status": "success", "message": f"{email} granted admin rights"}

#admin delete access 
@router.post("/revoke_admin/{email}")
async def api_revoke_admin(email: str, db: AsyncSession = Depends(get_session), current_admin = Depends(get_current_superadmin)):
    await revoke_admin(db, email)
    return {"status": "success", "message": f"{email} revoked admin rights"}
