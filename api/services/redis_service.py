import redis.asyncio as redis
from typing import Optional
from config import settings


class RedisService:
    def __init__(self):
        self.client: Optional[redis.Redis] = None

    async def connect(self):
        self.client = redis.from_url(settings.REDIS_URL, decode_responses=True)

    async def disconnect(self):
        if self.client:
            await self.client.aclose()

    def get_client(self) -> redis.Redis:
        if not self.client:
            raise Exception("Redis not connected")
        return self.client


redis_db = RedisService()
