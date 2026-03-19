"""Provider status caching utilities.

Optimizations compared to naïve implementations:

* A stable cache key per provider (no timestamp in the key name) so that
  repeated requests can benefit from the same Redis entry.
* A reasonable TTL (e.g. 10 seconds) instead of 1 second, drastically
  increasing cache hit rates.
* A simple Redis-based lock (SET NX + short EX) to avoid cache stampede
  when many clients hit the same uncached provider concurrently.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from redis.asyncio import Redis

from .circuit_breaker import get_current_circuit_state
from .models import ProviderStatus


STATUS_CACHE_TTL_SECONDS = 10
STATUS_LOCK_TTL_SECONDS = 5


def _status_key(provider_id: str) -> str:
    return f"provider:{provider_id}:status"


def _lock_key(provider_id: str) -> str:
    return f"provider:{provider_id}:status:lock"


async def _compute_fresh_status(redis: Redis, provider_id: str) -> ProviderStatus:
    """Compute the provider status without using the cache.

    In this example we derive `is_available` directly from the circuit
    breaker state, which is a reasonable proxy for provider health.
    """
    state = await get_current_circuit_state(redis, provider_id)
    is_available = state != "open"
    now = datetime.now(timezone.utc)
    return ProviderStatus(
        provider_id=provider_id,
        is_available=is_available,
        last_checked_at=now,
    )


async def get_provider_status(redis: Redis, provider_id: str) -> ProviderStatus:
    """Get provider status with Redis-backed caching and stampede protection.

    Algorithm:
    1. Try a direct GET on the stable cache key.
    2. On hit: deserialize and return.
    3. On miss: acquire a short-lived lock using SET NX EX.
       * If the lock is acquired: compute the status, cache it, release lock.
       * If lock is not acquired: briefly wait and re-check the cache a
         handful of times;
           - If it appears, return it.
           - Otherwise, compute once and return (without caching again).
    """
    key = _status_key(provider_id)
    lock = _lock_key(provider_id)

    cached = await redis.get(key)
    if cached is not None:
        return ProviderStatus.parse_raw(cached)

    # Attempt to acquire the rebuild lock
    got_lock = await redis.set(lock, "1", ex=STATUS_LOCK_TTL_SECONDS, nx=True)

    if got_lock:
        # This worker recomputes and refreshes the cache
        status = await _compute_fresh_status(redis, provider_id)
        # Single SET with TTL; value is Pydantic JSON string
        await redis.set(key, status.json(), ex=STATUS_CACHE_TTL_SECONDS)
        # Best-effort lock release (it's fine if it already expired)
        await redis.delete(lock)
        return status

    # Another worker holds the lock; wait briefly and try to read the cache
    for _ in range(3):
        await asyncio.sleep(0.05)
        cached = await redis.get(key)
        if cached is not None:
            return ProviderStatus.parse_raw(cached)

    # Fallback: compute and return, but don't fight over caching
    return await _compute_fresh_status(redis, provider_id)
