#src.app.schemas.admin.py

from typing import Optional
from pydantic import BaseModel, EmailStr

class AdminBase(BaseModel):
    email: EmailStr
    is_active: bool = True
    schema_name: str

class AdminCreate(AdminBase):
    password: str

class AdminRead(AdminBase):
    id: int

    class Config:
        orm_mode = True
