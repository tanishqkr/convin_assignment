# Webhook Ingest — Solution Overview

## What Was Broken, and Why

1. **Concurrent Duplicate Webhooks (Racy Check-Then-Act)**
   `Service.Ingest` checked `store.EventExists` before inserting events and updating stats as separate database statements. `events.event_id` lacked a `UNIQUE` database constraint. Under concurrent deliveries of the same stable `event_id`, multiple goroutines passed the existence check, causing duplicate event insertions and inflated `account_stats`.

2. **Unprocessed Call Recordings (Request Context Cancellation)**
   `processRecording` ran in an async goroutine inheriting `r.Context()`. Go's `http.Server` cancels request contexts immediately after sending the HTTP response. `MarkRecordingProcessed` failed with `context.Canceled`, and the error was silently swallowed at `// TODO: handle`.

3. **Lost In-Flight Work on Shutdown (Unmanaged Goroutines)**
   Recording work goroutines were launched without lifecycle tracking. Process shutdown (`SIGTERM`) terminated the service without waiting for active recording tasks to finish.

4. **Stats Cache Concurrency & Restart State Loss (Unsynchronized Map Access)**
   `stats.Cache.Record` mutated `c.m` without acquiring `c.mu` (unlike `Get` which acquired `RLock`), producing data races under concurrent requests. Additionally, service restarts initialized an empty cache without restoring durable totals from Postgres.

---

## Why This Deduplication Strategy

- **Postgres UNIQUE Constraint + Transactional `ON CONFLICT DO NOTHING` (Chosen)**
  Postgres is the durable correctness authority for idempotency: the `UNIQUE` constraint and atomic `INSERT ... ON CONFLICT DO NOTHING` prevent concurrent deliveries from being accepted more than once, while the surrounding transaction keeps the associated durable side effects consistent. If `INSERT INTO events ... ON CONFLICT DO NOTHING` returns no row, duplicate deliveries immediately short-circuit without mutating `calls` or `account_stats`.

- **Redis `SETNX`-Only (Rejected as Sole Correctness Authority)**
  While fast, Redis lacks durable persistence guarantees in typical setups. Relying on `SETNX` alone means a Redis restart, eviction, or memory pressure would silently open the door to duplicate ingestion into Postgres.

- **Application-Level Check-Then-Act (Rejected)**
  Inherently vulnerable to race conditions across concurrent application instances and threads. Database-level constraints are the only race-free correctness authority.

---

## What Changes at 10,000 Webhooks/Second

1. **Postgres Connection & Hot-Row Optimization**
   Direct updates to single `account_stats` rows for popular accounts will cause severe DB row lock contention and saturate the connection pool (`DBMaxConns=20`). Transition to an append-only event stream or staging buffer, aggregating stats asynchronously in micro-batches.

2. **Redis as a Best-Effort Pre-Check Filter (Not a Correctness Gate)**
   Use Redis `SETNX` with a short TTL as a high-throughput edge filter to shed duplicate request traffic before hitting Postgres pool limits. However, Redis serves only as a load-shedding optimization; Postgres retains sole authority over durable event acceptance so Redis evictions never cause dropped webhooks.

3. **Durable Distributed Worker Queues**
   Replace in-process goroutines with a persistent distributed queue (e.g., Redis Streams, RabbitMQ, or NATS). This ensures recording processing survives process crashes (beyond graceful SIGTERM stops), supports exponential backoff retries, and enforces backpressure downstream.
