#src.app.models.user_registry.py

from sqlmodel import SQLModel, Field
from datetime import datetime
from typing import Optional

class UserRegistry(SQLModel, table=True):
    __tablename__ = "user_registry"
    __table_args__ = {"schema": "public"}  

    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(index=True, unique=True, nullable=False)
    schema_name: str = Field(nullable=False)
    is_active: bool = Field(default=True)      
    is_superuser: bool = Field(default=False)   
    created_at: datetime = Field(default_factory=datetime.utcnow)
