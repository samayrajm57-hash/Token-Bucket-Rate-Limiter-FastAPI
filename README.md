# Token-Bucket-Rate-Limiter-FastAPI

Production-grade per-IP rate limiting using the **Token Bucket** algorithm,
built with **Python 3.11+**, **FastAPI**, and `asyncio.Lock` for strict
thread-safety under high concurrency.

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Incoming Request                                        │
│      │                                                   │
│      ▼                                                   │
│  Extract client IP  (request.client.host)                │
│      │                                                   │
│      ▼                                                   │
│  _registry[ip]  ──(miss)──▶  create TokenBucket         │
│      │                             │                     │
│      └─────────────────────────────┘                     │
│      │                                                   │
│      ▼                                                   │
│  bucket.consume()                                        │
│   ┌── asyncio.Lock acquired ──────────────────────────┐  │
│   │   _refill()  ← elapsed × refill_rate              │  │
│   │   tokens >= 1 ?                                   │  │
│   │     Yes → tokens -= 1 → return True               │  │
│   │     No  →               return False              │  │
│   └───────────────────────────────────────────────────┘  │
│      │                                                   │
│      ├── True  → 200 OK                                  │
│      └── False → 429 Too Many Requests                   │
└──────────────────────────────────────────────────────────┘
```

### Files

| File | Role |
|------|------|
| `limiter.py` | `TokenBucket` class — pure algorithm, zero dependencies |
| `main.py` | FastAPI app — per-IP registry, endpoint, health check |
| `test_concurrency.py` | Async stress-test — barrier-synchronised concurrent blast |

---

## Quick Start

### 1. Install dependencies

```bash
pip install fastapi uvicorn[standard] httpx
```

### 2. Start the server

```bash
uvicorn main:app --host 127.0.0.1 --port 8000 --log-level info
```

You should see:
```
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

### 3. Run the concurrency test

Open a **second terminal**:

```bash
python test_concurrency.py
```

### Expected output

```
╔══════════════════════════════════════════════════╗
║     Token Bucket — Concurrency Test Results      ║
╚══════════════════════════════════════════════════╝

  Request Breakdown
  ────────────────────────────────────────────────
  ✅  200 OK   (allowed)                         10
  🚫  429 TMR  (rate-limited)                    40
  ────────────────────────────────────────────────
  Total requests fired                           50

  Latency (ms)
  ────────────────────────────────────────────────
  Min                                          8.24
  Avg                                         12.91
  Max                                         31.05

  Correctness Verdict
  ────────────────────────────────────────────────
  Lock enforcement  →  ✅ PASS
  200s (10) ≤ capacity (10)

  Request accounting → ✅ PASS
  ok(10) + 429(40) + err(0) = 50/50
```

The **200 count will never exceed `BUCKET_CAPACITY`** — this mathematically
proves the `asyncio.Lock` is eliminating race conditions.

### 4. Hit the health endpoint (optional)

```bash
curl http://127.0.0.1:8000/health
```

```json
{
  "status": "healthy",
  "active_clients": 1,
  "bucket_config": {
    "capacity": 10,
    "refill_rate_per_second": 2.0
  }
}
```

---

## Configuration

All tuneable constants live at the top of `main.py`:

| Constant | Default | Meaning |
|----------|---------|---------|
| `BUCKET_CAPACITY` | `10` | Max burst — tokens available at startup |
| `REFILL_RATE` | `2.0` | Tokens replenished per second (sustained rate) |

When you change `BUCKET_CAPACITY`, update the matching constant in
`test_concurrency.py` so the verdict logic remains accurate.

---

## Why `asyncio.Lock` and not a threading lock?

FastAPI on Uvicorn runs a **single-threaded asyncio event loop**. Race
conditions still arise because the scheduler can switch between coroutines
at every `await` point. `asyncio.Lock` suspends competing coroutines at
the exact point of contention and resumes them one at a time, making
the `_refill → check → deduct` sequence **atomic** with respect to all
other coroutines sharing the same loop.

A `threading.Lock` would work too, but it blocks the entire OS thread
(defeating async concurrency). `asyncio.Lock` yields control correctly.

---

## Running with multiple workers (multi-process)

For a multi-worker deployment (`uvicorn --workers 4`), the in-memory
registry is **per-process** and each worker maintains independent state.
To share rate-limit state across workers, replace `_registry` with a
Redis backend (e.g., `redis-py` with Lua-atomic scripts) or a shared
memory structure. The `TokenBucket` interface remains unchanged.
