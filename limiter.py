"""
limiter.py — Token Bucket Rate Limiter
Production-grade, fully async, strictly thread-safe.
"""

import asyncio
import time


class TokenBucket:
    """
    Token Bucket implementation for per-client rate limiting.

    Tokens are replenished continuously at `refill_rate` tokens/second,
    capped at `capacity`. Each request consumes one token.

    All state mutation is guarded by an asyncio.Lock, making this
    safe under arbitrary concurrency within a single event loop.
    """

    __slots__ = ("_capacity", "_refill_rate", "_tokens", "_last_refill", "_lock")

    def __init__(self, capacity: int, refill_rate: float) -> None:
        """
        Args:
            capacity:    Maximum token capacity (burst ceiling).
            refill_rate: Tokens added per second (sustained throughput).
        """
        if capacity <= 0:
            raise ValueError(f"capacity must be > 0, got {capacity}")
        if refill_rate <= 0.0:
            raise ValueError(f"refill_rate must be > 0.0, got {refill_rate}")

        self._capacity: int = capacity
        self._refill_rate: float = refill_rate
        self._tokens: float = float(capacity)   # start full
        self._last_refill: float = time.time()
        self._lock: asyncio.Lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Internal helpers (called only while holding _lock)
    # ------------------------------------------------------------------

    def _refill(self) -> None:
        """
        Recompute token count based on wall-clock elapsed time.

        Must be called exclusively while the caller holds `_lock`.
        Uses high-resolution float timestamps so sub-second intervals
        are reflected accurately, avoiding token starvation under
        low-rate bursts.
        """
        now: float = time.time()
        elapsed: float = now - self._last_refill

        if elapsed > 0.0:
            replenishment: float = elapsed * self._refill_rate
            self._tokens = min(float(self._capacity), self._tokens + replenishment)
            self._last_refill = now

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def consume(self, tokens: int = 1) -> bool:
        """
        Attempt to consume `tokens` from the bucket.

        Acquires the asyncio lock, refills based on elapsed time,
        then atomically deducts tokens if available.

        Returns:
            True  — tokens were available and consumed (request allowed).
            False — insufficient tokens (request should be rejected 429).
        """
        async with self._lock:
            self._refill()

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True

            return False

    # ------------------------------------------------------------------
    # Introspection helpers (for observability / health endpoints)
    # ------------------------------------------------------------------

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def refill_rate(self) -> float:
        return self._refill_rate

    async def available_tokens(self) -> float:
        """Thread-safe snapshot of current token count (for diagnostics)."""
        async with self._lock:
            self._refill()
            return round(self._tokens, 4)

    def __repr__(self) -> str:  # noqa: D105
        return (
            f"TokenBucket(capacity={self._capacity}, "
            f"refill_rate={self._refill_rate} tok/s)"
        )
