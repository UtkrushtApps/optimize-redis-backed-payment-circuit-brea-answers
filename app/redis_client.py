import os
from typing import Optional

from redis.asyncio import Redis


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

_redis_client: Optional[Redis] = None


async def get_redis() -> Redis:
    """Return a singleton async Redis client.

    Using a shared client avoids reconnect overhead and lets us pipeline
    commands efficiently from the FastAPI endpoints.
    """
    global _redis_client
    if _redis_client is None:
        _redis_client = Redis.from_url(
            REDIS_URL,
            encoding="utf-8",
            decode_responses=True,  # return str instead of bytes
        )
    return _redis_client


async def close_redis() -> None:
    """Close the global Redis client on application shutdown."""
    global _redis_client
    if _redis_client is not None:
        await _redis_client.close()
        _redis_client = None
