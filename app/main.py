from __future__ import annotations

from fastapi import FastAPI, Depends, Query
from redis.asyncio import Redis

from .cache import get_provider_status
from .circuit_breaker import process_payment
from .models import (
    PaymentRequest,
    PaymentResponse,
    ProviderStatus,
    PopularProvider,
    PopularProvidersResponse,
)
from .redis_client import get_redis, close_redis


app = FastAPI(title="Optimized Payment Service with Redis Circuit Breaker")


@app.on_event("shutdown")
async def _shutdown_redis() -> None:
    await close_redis()


# ----------------------------- Payments API ------------------------------


@app.post("/api/payments", response_model=PaymentResponse)
async def create_payment(
    payload: PaymentRequest,
    redis: Redis = Depends(get_redis),
) -> PaymentResponse:
    """Simulate a payment against an external provider.

    The underlying implementation uses a Redis-backed circuit breaker to
    decide whether to attempt the provider call or reject quickly, and it
    updates metrics using a single hash and pipelined commands.
    """
    return await process_payment(redis, payload)


# -------------------------- Provider status API --------------------------


@app.get("/api/providers/{provider_id}/status", response_model=ProviderStatus)
async def provider_status(
    provider_id: str,
    redis: Redis = Depends(get_redis),
) -> ProviderStatus:
    """Return provider availability, cached efficiently in Redis.

    This endpoint uses a stable per-provider key with a moderate TTL and a
    lightweight lock to avoid cache stampede under load.
    """
    return await get_provider_status(redis, provider_id)


# ------------------------- Popular providers API -------------------------


@app.get("/api/providers/popular", response_model=PopularProvidersResponse)
async def popular_providers(
    limit: int = Query(10, gt=0, le=100, description="Maximum number of providers to return"),
    redis: Redis = Depends(get_redis),
) -> PopularProvidersResponse:
    """Return providers ordered by number of successful payments.

    Instead of scanning Redis with KEYS and individual GETs, this uses a
    single sorted set (ZSET) maintained by the payment logic and a single
    ZREVRANGE call to read the top providers.
    """
    # ZREVRANGE with scores gives us (member, score) pairs ordered
    raw = await redis.zrevrange("providers:popular", 0, limit - 1, withscores=True)

    providers = [
        PopularProvider(provider_id=pid, successful_payments=int(score))
        for pid, score in raw
    ]

    return PopularProvidersResponse(providers=providers)
