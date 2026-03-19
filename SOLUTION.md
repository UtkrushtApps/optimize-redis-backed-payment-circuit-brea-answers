# Solution Steps

1. Create a shared async Redis client so all FastAPI endpoints reuse a single connection pool instead of creating a new client per request:
- Add redis.asyncio to the project and implement app/redis_client.py.
- Expose `get_redis()` as a FastAPI dependency and `close_redis()` for shutdown cleanup.
- Configure the client from REDIS_URL and enable `decode_responses=True` to work with strings instead of bytes.

2. Consolidate circuit breaker state into a single Redis hash per provider instead of multiple keys:
- Define a helper `_cb_key(provider_id)` that returns a key like `provider:{provider_id}:cb`.
- Decide on fields to store in the hash: `state`, `failure_count`, `success_count`, `opened_at_ts`, `half_open_trial_count`, `total_requests`, `total_failures`, timestamps for last events, etc.
- This design ensures all circuit breaker information is fetched via one HGETALL and updated via pipelined HINCRBY/HSET calls.

3. Implement the circuit breaker evaluation logic with minimal Redis round-trips in app/circuit_breaker.py:
- Write `evaluate_circuit(redis, provider_id)` to perform a single HGETALL and derive the current state (default to `closed` if missing).
- If state is `open`, check `opened_at_ts` against the current time; if the open timeout has expired, transition to `half-open` using a single HSET, otherwise reject the request.
- If state is `half-open`, allow only if `half_open_trial_count` is below `HALF_OPEN_MAX_TRIALS`; otherwise reject to avoid overload.
- Return `(allowed, state, rejection_reason, state_dict)` for use by the payment endpoint.

4. Implement efficient recording of circuit breaker metrics and transitions using Redis pipelines:
- Add `record_circuit_rejection(redis, provider_id, state, reason)` to increment `total_requests` and `rejected_requests` in the hash and store `last_rejected_ts`, using a single pipelined operation.
- Add `record_payment_result(redis, provider_id, success, prev_state, state_dict)` that:
  - Increments `total_requests` for every attempted provider call.
  - On success: increments `success_count`, updates `last_success_ts`, resets failures and closes the circuit if it wasn’t already `closed`, and `ZINCRBY providers:popular` to track popularity.
  - On failure: increments `failure_count` and `total_failures`, updates `last_failure_ts`, opens the circuit (`state=open`, `opened_at_ts=now`) if thresholds are exceeded or if a half-open probe fails.
  - For half-open trials, increments `half_open_trial_count` in the same hash.
- Execute all hash updates and the ZINCRBY via a single pipeline (`pipeline.execute()`) to minimize round-trips and keep counters consistent.

5. Implement the high-level payment processing function used by the FastAPI endpoint:
- In `process_payment(redis, request)` call `evaluate_circuit`.
- If `allowed` is False, call `record_circuit_rejection` and return a `PaymentResponse` with status `rejected` and the current circuit state.
- If allowed, call `_simulate_provider_call(request)` to introduce latency and random failures, then call `record_payment_result` with the outcome.
- Build and return a `PaymentResponse` with status `success` or `failed` and the effective circuit state.
- Add `get_current_circuit_state(redis, provider_id)` that uses a single HGET to read just the `state` field for other modules (like status caching).

6. Optimize provider status caching in app/cache.py with a stable key and stampede protection:
- Choose a stable cache key pattern like `provider:{provider_id}:status` and a separate lock key `provider:{provider_id}:status:lock`.
- Implement `_compute_fresh_status(redis, provider_id)` that derives `is_available` from the current circuit breaker state (state != `open`) and stamps `last_checked_at` with the current UTC time.
- Implement `get_provider_status(redis, provider_id)`:
  - Try `GET` on the status key; on hit, `ProviderStatus.parse_raw` and return.
  - On miss, attempt `SET lock NX EX STATUS_LOCK_TTL_SECONDS`.
    - If lock acquired: compute status, `SET` the cache value with `ex=STATUS_CACHE_TTL_SECONDS`, then `DEL` the lock and return the status.
    - If lock not acquired: loop a few times (`asyncio.sleep(0.05)`), retry `GET`, and return if it appears.
    - If still missing, compute and return the status without writing to Redis (to avoid more contention).
- Use a TTL like 10 seconds to drastically improve hit rates vs the original 1-second timestamped keys.

7. Replace KEYS-based provider popularity discovery with a sorted set in Redis:
- In `record_payment_result`, after a successful provider call, update a sorted set `providers:popular` with `ZINCRBY providers:popular 1 {provider_id}`.
- Implement `/api/providers/popular` endpoint in main.py to call `ZREVRANGE providers:popular 0 limit-1 WITHSCORES` and build a list of `PopularProvider` models.
- Wrap this list into a `PopularProvidersResponse` to return via the API.
- This removes the need for scanning `provider_status:*` keys with KEYS and for issuing individual GETs per key on every request.

8. Wire everything into FastAPI in app/main.py:
- Create the FastAPI app and register a shutdown hook that calls `close_redis()`.
- Add the `/api/payments` POST endpoint:
  - Inject the Redis client via `Depends(get_redis)`.
  - Call `process_payment(redis, payload)` and return its `PaymentResponse`.
- Add the `/api/providers/{provider_id}/status` GET endpoint:
  - Inject Redis and delegate to `get_provider_status(redis, provider_id)`.
- Add the `/api/providers/popular` GET endpoint:
  - Accept a `limit` query param, default 10.
  - Read top providers from the `providers:popular` ZSET and return them as `PopularProvidersResponse`.

9. Validate behavior and performance:
- Run Redis locally and start the FastAPI app.
- Hit `/api/payments` repeatedly for a few provider IDs and observe that:
  - The circuit transitions from closed to open after enough failures and respects the open timeout.
  - Redis holds a single hash per provider (`HGETALL provider:{id}:cb`) with consistent counters.
- Hit `/api/providers/{id}/status` concurrently (e.g. via a load tool) and confirm:
  - The same status key is reused (no timestamp in the key) and TTL ~10 seconds.
  - Under load, only one worker at a time acquires the lock to rebuild the cache, avoiding a cache stampede.
- Hit `/api/providers/popular` and confirm that:
  - It uses ZREVRANGE on `providers:popular` (no KEYS scans).
  - It returns providers sorted by successful payment count.
- Optionally, use Redis MONITOR or command stats to verify significantly fewer commands per request compared to a naïve multi-key / KEYS-based implementation.

