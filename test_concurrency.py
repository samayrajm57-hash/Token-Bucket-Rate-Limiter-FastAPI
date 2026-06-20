"""
test_concurrency.py — Concurrency stress-test for the Token Bucket rate limiter.

Fires N requests as simultaneously as possible using asyncio.gather(),
then prints a structured terminal summary proving that the lock enforces
the bucket's capacity ceiling exactly.

Usage:
    # Terminal 1 — start the server
    uvicorn main:app --host 127.0.0.1 --port 8000

    # Terminal 2 — run this script
    python test_concurrency.py
"""

from __future__ import annotations

import asyncio
import sys
import time
from dataclasses import dataclass, field
from typing import List

import httpx

# ---------------------------------------------------------------------------
# Test configuration
# ---------------------------------------------------------------------------

TARGET_URL: str       = "http://127.0.0.1:8000/api/resource"
TOTAL_REQUESTS: int   = 50          # total concurrent requests to fire
BUCKET_CAPACITY: int  = 10          # must match main.py BUCKET_CAPACITY
REQUEST_TIMEOUT: float = 10.0       # seconds before a request is an error

# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class Result:
    index: int
    status_code: int
    elapsed_ms: float
    detail: str = ""


@dataclass
class Summary:
    results: List[Result] = field(default_factory=list)

    @property
    def allowed(self) -> List[Result]:
        return [r for r in self.results if r.status_code == 200]

    @property
    def blocked(self) -> List[Result]:
        return [r for r in self.results if r.status_code == 429]

    @property
    def errors(self) -> List[Result]:
        return [r for r in self.results if r.status_code not in (200, 429)]

    @property
    def min_ms(self) -> float:
        return min((r.elapsed_ms for r in self.results), default=0.0)

    @property
    def max_ms(self) -> float:
        return max((r.elapsed_ms for r in self.results), default=0.0)

    @property
    def avg_ms(self) -> float:
        if not self.results:
            return 0.0
        return sum(r.elapsed_ms for r in self.results) / len(self.results)


# ---------------------------------------------------------------------------
# Single request coroutine
# ---------------------------------------------------------------------------

async def fire_request(
    client: httpx.AsyncClient,
    index: int,
    barrier: asyncio.Event,
) -> Result:
    """
    Wait for the shared barrier event, then immediately fire the request.
    All coroutines block on `barrier.wait()`, so they are released
    simultaneously — maximising concurrency overlap.
    """
    await barrier.wait()                     # synchronise with all peers

    t0 = time.perf_counter()
    try:
        response = await client.get(TARGET_URL)
        elapsed = (time.perf_counter() - t0) * 1000
        detail = response.json().get("message", "")
        return Result(
            index=index,
            status_code=response.status_code,
            elapsed_ms=round(elapsed, 2),
            detail=detail,
        )
    except httpx.HTTPStatusError as exc:
        elapsed = (time.perf_counter() - t0) * 1000
        return Result(
            index=index,
            status_code=exc.response.status_code,
            elapsed_ms=round(elapsed, 2),
            detail=str(exc),
        )
    except Exception as exc:                 # network error, timeout, etc.
        elapsed = (time.perf_counter() - t0) * 1000
        return Result(
            index=index,
            status_code=-1,
            elapsed_ms=round(elapsed, 2),
            detail=str(exc),
        )


# ---------------------------------------------------------------------------
# Pretty printer
# ---------------------------------------------------------------------------

RESET  = "\033[0m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[2m"


def _c(text: str, *codes: str) -> str:
    return "".join(codes) + text + RESET


def print_summary(summary: Summary) -> None:
    n_ok    = len(summary.allowed)
    n_429   = len(summary.blocked)
    n_err   = len(summary.errors)
    n_total = len(summary.results)

    # ── header ──────────────────────────────────────────────────────────
    print()
    print(_c("╔══════════════════════════════════════════════════╗", BOLD, CYAN))
    print(_c("║     Token Bucket — Concurrency Test Results      ║", BOLD, CYAN))
    print(_c("╚══════════════════════════════════════════════════╝", BOLD, CYAN))
    print()

    # ── request breakdown ───────────────────────────────────────────────
    print(_c("  Request Breakdown", BOLD))
    print(f"  {'─'*44}")
    print(
        f"  {_c('✅  200 OK   (allowed)', GREEN):<38}"
        f"{_c(str(n_ok).rjust(4), BOLD, GREEN)}"
    )
    print(
        f"  {_c('🚫  429 TMR  (rate-limited)', YELLOW):<38}"
        f"{_c(str(n_429).rjust(4), BOLD, YELLOW)}"
    )
    if n_err:
        print(
            f"  {_c('❌  Error   (network/timeout)', RED):<38}"
            f"{_c(str(n_err).rjust(4), BOLD, RED)}"
        )
    print(f"  {'─'*44}")
    print(f"  {'Total requests fired':<38}{str(n_total).rjust(4)}")
    print()

    # ── latency ─────────────────────────────────────────────────────────
    print(_c("  Latency (ms)", BOLD))
    print(f"  {'─'*44}")
    print(f"  {'Min':<38}{summary.min_ms:>8.2f}")
    print(f"  {'Avg':<38}{summary.avg_ms:>8.2f}")
    print(f"  {'Max':<38}{summary.max_ms:>8.2f}")
    print()

    # ── correctness verdict ─────────────────────────────────────────────
    print(_c("  Correctness Verdict", BOLD))
    print(f"  {'─'*44}")

    # Allowed requests should be ≤ BUCKET_CAPACITY (≥ is impossible if lock works)
    lock_verdict = n_ok <= BUCKET_CAPACITY
    lock_msg = (
        f"200s ({n_ok}) ≤ capacity ({BUCKET_CAPACITY})"
        if lock_verdict
        else f"RACE CONDITION: {n_ok} > capacity ({BUCKET_CAPACITY})"
    )
    lock_icon = _c("✅ PASS", BOLD, GREEN) if lock_verdict else _c("❌ FAIL", BOLD, RED)
    print(f"  Lock enforcement  →  {lock_icon}")
    print(f"  {_c(lock_msg, DIM)}")
    print()

    # All requests accounted for (no silent drops)
    accounting_ok = (n_ok + n_429 + n_err) == n_total
    acct_icon = _c("✅ PASS", BOLD, GREEN) if accounting_ok else _c("❌ FAIL", BOLD, RED)
    print(f"  Request accounting → {acct_icon}")
    print(f"  {_c(f'ok({n_ok}) + 429({n_429}) + err({n_err}) = {n_ok+n_429+n_err}/{n_total}', DIM)}")
    print()

    print(_c("╚══════════════════════════════════════════════════╝", DIM, CYAN))
    print()


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

async def main() -> int:
    print()
    print(_c(f"  Preparing {TOTAL_REQUESTS} concurrent tasks …", DIM))

    barrier = asyncio.Event()
    summary = Summary()

    # Use a single shared httpx.AsyncClient (connection-pool reuse)
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        tasks = [
            asyncio.create_task(fire_request(client, i, barrier))
            for i in range(TOTAL_REQUESTS)
        ]

        # Give all tasks a moment to reach barrier.wait()
        await asyncio.sleep(0.05)

        t_start = time.perf_counter()
        barrier.set()          # release the herd — all tasks unblock at once

        results: list[Result] = await asyncio.gather(*tasks)
        t_total = (time.perf_counter() - t_start) * 1000

    summary.results = sorted(results, key=lambda r: r.index)

    print_summary(summary)
    print(_c(f"  Wall-clock time for all {TOTAL_REQUESTS} requests: {t_total:.1f} ms", DIM))
    print()

    # Exit 1 if lock enforcement failed
    return 0 if len(summary.allowed) <= BUCKET_CAPACITY else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
