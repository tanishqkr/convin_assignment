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
  Postgres is the single source of truth for durable state. Atomic insertion inside `IngestEventTx` guarantees strict ACID consistency. If `INSERT INTO events ... ON CONFLICT DO NOTHING` returns no row, duplicate deliveries immediately short-circuit without mutating `calls` or `account_stats`.

- **Redis `SETNX`-Only (Rejected as Sole Correctness Authority)**
  While fast, Redis in this architecture lacks persistent disk guarantees. A Redis restart or key eviction would silently allow duplicate events into Postgres.

- **Application-Level Check-Then-Act (Rejected)**
  Inherently vulnerable to race conditions across concurrent application instances and threads. Database-level constraints are the only race-free correctness authority.

---

## What Changes at 10,000 Webhooks/Second

1. **Postgres Connection & Hot-Row Optimization**
   Direct updates to single `account_stats` rows will suffer heavy lock contention. Shift to an append-only event stream or staging buffer (e.g. TimescaleDB/Postgres partition) and aggregate stats asynchronously in batches.

2. **Redis as a Fast Pre-Check Edge Filter**
   Use Redis `SETNX` with TTL in front of Postgres to filter duplicate requests early and shed load before hitting Postgres pool limits.

3. **Durable Asynchronous Job Queue**
   Replace in-process goroutines with a persistent worker queue (Redis Streams, RabbitMQ, or NATS). This ensures recording processing survives process crashes, supports exponential retries, and enforces backpressure downstream.
