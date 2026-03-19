"""Redis-backed circuit breaker for payment providers.

This module implements an efficient, hash-based circuit breaker using Redis.
All state for a provider lives under a *single* Redis hash key, which keeps
Redis calls per request low and makes counters consistent under concurrency.
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Dict, Optional, Tuple

from redis.asyncio import Redis

from .models import PaymentRequest, PaymentResponse


# Circuit breaker configuration (tunable as needed)
FAILURE_THRESHOLD = 5               # Failures before opening the circuit
OPEN_TIMEOUT_SECONDS = 60           # How long to stay open before half-open
HALF_OPEN_MAX_TRIALS = 1            # How many requests allowed in half-open


def _cb_key(provider_id: str) -> str:
    """Return the Redis hash key storing circuit breaker state."""
    return f"provider:{provider_id}:cb"


async def _simulate_provider_call(request: PaymentRequest) -> bool:
    """Simulate a real payment provider call.

    This function intentionally introduces a small random latency and a
    random failure rate to exercise the circuit breaker and caching logic.
    """
    # Simulated network / provider latency
    await asyncio.sleep(random.uniform(0.05, 0.2))

    # Simulated success probability (80% success rate)
    return random.random() > 0.2


async def evaluate_circuit(
    redis: Redis, provider_id: str
) -> Tuple[bool, str, Optional[str], Dict[str, str]]:
    """Evaluate whether a request for a provider should be allowed.

    Returns a tuple: (allowed, current_state, rejection_reason, raw_state_dict)

    * `allowed`: whether the payment may proceed to the provider
    * `current_state`: one of "closed", "open", "half-open"
    * `rejection_reason`: if not allowed, a short reason string
    * `raw_state_dict`: current circuit breaker hash fields (strings)

    This performs a single HGETALL on the provider hash. Any necessary
    state transition from OPEN -> HALF-OPEN is also executed here via a
    single HSET, keeping calls minimal and atomic per field.
    """
    key = _cb_key(provider_id)
    state_dict: Dict[str, str] = await redis.hgetall(key)

    now = int(time.time())
    state = state_dict.get("state", "closed")

    # Parse numeric fields safely
    opened_at_ts = int(state_dict.get("opened_at_ts", 0) or 0)
    half_open_trial_count = int(state_dict.get("half_open_trial_count", 0) or 0)

    # Handle OPEN -> potential HALF-OPEN transition
    if state == "open":
        if now - opened_at_ts >= OPEN_TIMEOUT_SECONDS:
            # Move to half-open after timeout, allow limited probes
            state = "half-open"
            state_dict["state"] = "half-open"
            state_dict["half_open_trial_count"] = "0"
            # One atomic update for the transition
            await redis.hset(key, mapping={"state": "half-open", "half_open_trial_count": 0})
        else:
            return False, "open", "circuit_open", state_dict

    # In HALF-OPEN, allow only a limited number of concurrent trials
    if state == "half-open":
        if half_open_trial_count >= HALF_OPEN_MAX_TRIALS:
            # Too many probe requests in half-open; reject quickly
            return False, "half-open", "half_open_trial_limit_reached", state_dict
        # Otherwise allow and increment the trial counter later in record_payment_result
        return True, "half-open", None, state_dict

    # CLOSED: request is allowed
    return True, "closed", None, state_dict


async def record_circuit_rejection(
    redis: Redis,
    provider_id: str,
    state: str,
    reason: str,
) -> None:
    """Record a request rejected by the circuit breaker (no provider call).

    This keeps metrics consistent without affecting failure counters.
    """
    key = _cb_key(provider_id)
    now = int(time.time())

    pipe = redis.pipeline(transaction=True)
    pipe.hincrby(key, "total_requests", 1)
    pipe.hincrby(key, "rejected_requests", 1)
    pipe.hset(key, mapping={"last_rejected_ts": now, "state": state})
    await pipe.execute()


async def record_payment_result(
    redis: Redis,
    provider_id: str,
    success: bool,
    prev_state: str,
    state_dict: Dict[str, str],
) -> None:
    """Update circuit breaker and metrics after a provider call.

    All updates are performed against a single Redis hash using a pipeline,
    so counters are incremented atomically and efficiently.
    """
    key = _cb_key(provider_id)
    now = int(time.time())

    failure_count = int(state_dict.get("failure_count", 0) or 0)
    half_open_trial_count = int(state_dict.get("half_open_trial_count", 0) or 0)

    mapping: Dict[str, object] = {"last_updated_ts": now}

    pipe = redis.pipeline(transaction=True)
    pipe.hincrby(key, "total_requests", 1)

    if success:
        # Successful call
        pipe.hincrby(key, "success_count", 1)
        mapping["last_success_ts"] = now

        # Any prior failures are reset, and circuit is closed
        if prev_state in ("half-open", "open") or failure_count > 0:
            mapping.update({
                "state": "closed",
                "failure_count": 0,
                "half_open_trial_count": 0,
            })

        # Track popularity of providers via a sorted set
        pipe.zincrby("providers:popular", 1, provider_id)

    else:
        # Failed call
        new_failure_count = failure_count + 1
        mapping["last_failure_ts"] = now

        pipe.hincrby(key, "failure_count", 1)
        pipe.hincrby(key, "total_failures", 1)

        if prev_state == "half-open":
            # Half-open probe failed: immediately re-open the circuit
            mapping.update({
                "state": "open",
                "opened_at_ts": now,
                "half_open_trial_count": 0,
            })
        elif prev_state == "closed" and new_failure_count >= FAILURE_THRESHOLD:
            # Too many failures in closed state: open the circuit
            mapping.update({
                "state": "open",
                "opened_at_ts": now,
            })

    # Every time we actually perform a half-open trial, bump the counter
    if prev_state == "half-open":
        mapping["half_open_trial_count"] = half_open_trial_count + 1

    pipe.hset(key, mapping=mapping)
    await pipe.execute()


async def process_payment(
    redis: Redis,
    request: PaymentRequest,
) -> PaymentResponse:
    """High level function used by the FastAPI endpoint.

    It evaluates the circuit breaker, optionally calls the provider, then
    writes back results and metrics in an optimized way.
    """
    allowed, state, rejection_reason, state_dict = await evaluate_circuit(
        redis, request.provider_id
    )

    if not allowed:
        # Record rejection and return immediately
        await record_circuit_rejection(redis, request.provider_id, state, rejection_reason or "rejected")
        return PaymentResponse(
            provider_id=request.provider_id,
            amount=request.amount,
            status="rejected",
            reason=rejection_reason,
            circuit_state=state,
        )

    # Circuit allows the call: simulate external provider
    success = await _simulate_provider_call(request)

    # Update circuit breaker state and metrics
    await record_payment_result(redis, request.provider_id, success, state, state_dict)

    return PaymentResponse(
        provider_id=request.provider_id,
        amount=request.amount,
        status="success" if success else "failed",
        reason=None if success else "provider_error",
        circuit_state=("closed" if success else ("open" if state == "open" else state)),
    )


async def get_current_circuit_state(redis: Redis, provider_id: str) -> str:
    """Lightweight helper for other modules to read the provider state.

    Uses a single HGET instead of HGETALL for efficiency when only the
    state is needed (e.g. for provider status calculation).
    """
    key = _cb_key(provider_id)
    state = await redis.hget(key, "state")
    return state or "closed"
