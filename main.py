"""
main.py — FastAPI application with per-IP Token Bucket rate limiting.

Configuration (adjust at module level):
    BUCKET_CAPACITY  — max burst size per client
    REFILL_RATE      — sustained token refill in tokens/second
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from limiter import TokenBucket

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rate-limit configuration
# ---------------------------------------------------------------------------

BUCKET_CAPACITY: int = 10          # burst: up to 10 requests at once
REFILL_RATE: float = 2.0           # sustained: 2 new tokens per second

# ---------------------------------------------------------------------------
# In-memory client registry
#
# Maps  client_ip (str) → TokenBucket
#
# Access pattern: reads dominate; new-client writes happen once per IP.
# Because FastAPI's default Uvicorn runner is single-process / single-loop,
# plain dict access between coroutines is safe — the asyncio scheduler
# cannot context-switch between two coroutines unless one is awaiting.
# Guarded by _registry_lock for correctness even if a multi-threaded
# executor submits work on the same loop.
# ---------------------------------------------------------------------------

_registry: Dict[str, TokenBucket] = {}
_registry_lock: asyncio.Lock = asyncio.Lock()


async def _get_or_create_bucket(client_ip: str) -> TokenBucket:
    """
    Return the existing bucket for `client_ip`, or atomically create one.

    Using a lock around the check-then-set pattern eliminates the TOCTOU
    race that would arise if two coroutines simultaneously discover that
    the same IP is missing from the registry.
    """
    # Fast-path: bucket already exists (no lock needed — dict lookup is atomic)
    if client_ip in _registry:
        return _registry[client_ip]

    # Slow-path: first request from this IP
    async with _registry_lock:
        # Double-checked locking: re-test after acquiring
        if client_ip not in _registry:
            _registry[client_ip] = TokenBucket(
                capacity=BUCKET_CAPACITY,
                refill_rate=REFILL_RATE,
            )
            log.info("New bucket created for client %s", client_ip)
        return _registry[client_ip]


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Token Bucket Rate Limiter",
    description="Per-IP rate limiting via the Token Bucket algorithm.",
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get(
    "/api/resource",
    summary="Protected resource",
    responses={
        200: {"description": "Request allowed"},
        429: {"description": "Rate limit exceeded"},
    },
)
async def protected_resource(request: Request) -> JSONResponse:
    """
    Dummy protected endpoint.

    Rate-limiting decision:
      • consume() → True  → 200 OK
      • consume() → False → 429 Too Many Requests
    """
    client_ip: str = (
        request.client.host if request.client else "unknown"
    )

    bucket = await _get_or_create_bucket(client_ip)
    allowed = await bucket.consume()

    if allowed:
        remaining = await bucket.available_tokens()
        log.debug("ALLOW  %s | tokens_remaining=%.2f", client_ip, remaining)
        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "message": "Request successful.",
                "client": client_ip,
                "tokens_remaining": remaining,
                "timestamp": time.time(),
            },
        )

    log.warning("BLOCK  %s | bucket exhausted", client_ip)
    raise HTTPException(
        status_code=429,
        detail={
            "error": "Too Many Requests",
            "message": (
                f"Rate limit exceeded. Bucket refills at "
                f"{REFILL_RATE} token(s)/s (capacity={BUCKET_CAPACITY})."
            ),
            "client": client_ip,
        },
    )


# ---------------------------------------------------------------------------
# Health / observability
# ---------------------------------------------------------------------------

@app.get("/health", include_in_schema=False)
async def health() -> dict:
    return {
        "status": "healthy",
        "active_clients": len(_registry),
        "bucket_config": {
            "capacity": BUCKET_CAPACITY,
            "refill_rate_per_second": REFILL_RATE,
        },
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8000,
        log_level="info",
    )
