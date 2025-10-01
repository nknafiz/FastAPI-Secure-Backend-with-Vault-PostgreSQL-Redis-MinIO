#src.app.models.admin.py

from sqlmodel import SQLModel, Field
from datetime import datetime

class Admin(SQLModel, table=True, schema="public"):
    id: int | None = Field(default=None, primary_key=True)
    email: str = Field(nullable=False, unique=True, index=True)
    hashed_password: str = Field(nullable=False)
    is_active: bool = Field(default=True)
    is_superuser: bool = Field(default=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
