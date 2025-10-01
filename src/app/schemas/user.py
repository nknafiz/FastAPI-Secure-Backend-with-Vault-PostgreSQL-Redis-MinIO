#src.app.schemas.user.py

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

async def create_user_schema(email: str, db: AsyncSession) -> str:
    schema_name = f"user_{email.lower().replace('@','_at_').replace('.','_dot_')}"
    
    # create schema
    await db.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
    
    # master_user table
    await db.execute(text(f'''
        CREATE TABLE IF NOT EXISTS "{schema_name}".master_user (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            hashed_password VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    '''))
    
    
    await db.commit()
    return schema_name
