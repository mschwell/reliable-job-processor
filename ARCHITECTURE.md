# Architecture & Failure Modes

## Scope

One API and one in-process hosted worker use a common PostgreSQL database. Multiple instances can run simultaneously. Jobs are small, synthetic, and bounded. This is designed for review by a senior-engineering interviewer, not deployment as a multi-tenant SaaS.

## Invariants

1. `queued` jobs are eligible only after `available_at <= now()`.
2. A worker claims a job in a single PostgreSQL statement using `FOR UPDATE SKIP LOCKED`; no two transactions can claim the same queued row simultaneously.
3. Every claim atomically increments the job attempt number and inserts one `job_attempts` row.
4. All acknowledgments require a matching lease token and unexpired lease.
5. A failed or expired job can retry only while `attempts < max_attempts`.
6. A unique producer idempotency key resolves to exactly one stored request or yields a conflict.
7. The demonstration side-effect sink has a unique `(job_id)` key, so a replay cannot store the same logical effect twice.

## Why this is at-least-once

There is an unavoidable window between executing a side effect and acknowledging the job. For example:

1. Worker A claims a job.
2. Worker A commits a payment or calls an external service.
3. Worker A crashes before setting `status='succeeded'`.
4. After its lease expires, Worker B reclaims the job.

The database can prevent two **live valid leases** from being issued simultaneously for a queued row, but cannot roll back an already completed external call. For safe external work, pass the stable job UUID as a downstream idempotency key, or use an outbox and a consumer that deduplicates stable event IDs. When a worker loses a lease after pausing, the old process can still run code; token-based ack checks protect our DB job state but do not magically fence external systems.

## Recovery details

A worker periodically scans for `running` jobs whose leases expired. It uses `FOR UPDATE SKIP LOCKED` so recovery workers do not conflict. In one statement, it marks the previous attempt `lease_expired` and returns the job to `queued` with exponential backoff, or `dead` if no retry budget remains.

A reclaimer may run while the original worker is slow, e.g. due to a stop-the-world pause. The original worker's old token is rejected at completion. The sample's bounded 5-second handlers reduce lease risk; real workers need lease extension and/or job-level fencing at the side-effect target.

## Concurrency & ordering

The queue has no strict FIFO guarantee: `SKIP LOCKED` prioritizes throughput over global ordering, and scheduled jobs and retries naturally move around. It does not guarantee one active job per customer/account. Such a requirement would need additional per-entity concurrency keys or partitioning.

## Backoff

After attempt N, delay = `min(60 seconds, 2^N seconds)`. For example: 2, 4, 8, 16, 32, 60 seconds. No jitter is included; production retries would often use jitter to avoid a thundering herd. Expired leases use the same exponent, calculated in SQL.

## Data integrity

The `jobs`, `job_attempts`, and `job_effects` tables are separate. The audit trail is never erased by manual retry. The claim and audit insert occur in one SQL statement, and the terminal state and audit update likewise occur in one SQL statement. The worker's synthetic effect is stored separately from completion, deliberately exposing the crash window and need for effect idempotency.

## Operations discussion

For a production deployment, consider:

- Versioned migrations and transaction-level deployment compatibility
- Worker-level heartbeats, cancellation, throttling, tracing and metrics
- Poison-message dashboards, alerting and retention/compaction for old jobs
- Multi-tenant authorization and data isolation
- Circuit breakers and rate limits for downstream APIs
- Outbox/inbox patterns for cross-system side effects
- Horizontal scaling, adaptive polling or message broker integration
- Separate API and worker deployment processes

## Interview prompts

**Why not `Task.Run` when the HTTP request arrives?** It isn't durable and runs within a process lifetime. A persisted job record preserves work across restarts and gives operational visibility.

**Why not a message broker?** A broker is a strong option at high throughput or when fan-out and delivery guarantees are more important. PostgreSQL fits a small, transaction-oriented service that already depends on Postgres, and it can keep enqueuing and business writes in one transaction (a future extension).

**How do you guarantee exactly once?** I don't claim universal exactly-once execution. I use at-least-once delivery, idempotency keys on submission, lease fencing for job state transitions, and idempotent effects when possible. External systems must participate in deduplication.

**How do you test failure recovery?** A PostgreSQL integration test forces lease expiry, attempts an old-token completion (must fail), reclaims the job and checks that audit history records both the expired and successful attempts.
