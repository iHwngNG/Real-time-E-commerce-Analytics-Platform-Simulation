import asyncpg
from typing import Optional
from config import settings


class PostgresService:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        # Convert sql urls if necessary, asyncpg requires postgresql://
        db_url = settings.DATABASE_URL.replace(
            "postgresql+psycopg2://", "postgresql://"
        )
        self.pool = await asyncpg.create_pool(dsn=db_url)

    async def disconnect(self):
        if self.pool:
            await self.pool.close()

    async def fetch(self, query: str, *args):
        if not self.pool:
            raise Exception("Database not connected")
        async with self.pool.acquire() as conn:
            records = await conn.fetch(query, *args)
            return [dict(record) for record in records]

    async def execute(self, query: str, *args):
        if not self.pool:
            raise Exception("Database not connected")
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)


postgres_db = PostgresService()
