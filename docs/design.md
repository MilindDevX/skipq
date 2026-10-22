# skipq design

skipq is a durable background-job queue for Go that stores jobs in Postgres.
This document says what skipq promises, how the data is laid out, how a job
moves through its life, what happens when things break, and how skipq compares
with existing tools.

Terms used below:

- **Job**: one unit of work, such as "send this email". A row in `skipq_jobs`.
- **Worker**: a goroutine (a lightweight thread managed by the Go runtime) that
  claims a job, runs the handler for it, and records the result.
- **Pool**: a group of workers inside one process that share a database
  connection pool.
- **Lease** (also called a visibility timeout): a time limit attached to a
  claimed job. If the worker has not finished the job when the lease runs out,
  the job becomes claimable again.
- **Handler**: the user's Go function that does the actual work for a job kind.

## 1. Guarantees and non-guarantees

### Guarantees

1. **Durable enqueue.** `Enqueue` returns only after the job row is committed.
   Once it returns, the job survives a crash of every application process (it
   does not survive losing the database itself; see section 4.3).
2. **At-least-once execution.** Every job that is enqueued is run until it
   succeeds or runs out of attempts. A job may run more than once; it is never
   silently dropped.
3. **No double claim while a lease is valid.** Two workers never hold a valid
   lease on the same job at the same time. This is enforced by Postgres row
   locks (`SELECT ... FOR UPDATE SKIP LOCKED`), not by application code.
4. **Fenced completion.** A worker whose lease has expired cannot mark the job
   as succeeded or failed. Every claim increments `attempt`, and ack/fail only
   apply when `attempt` still matches the value the worker claimed. A stale
   worker gets `ErrLeaseLost`.
5. **Bounded retries.** A failing job is retried with exponential backoff and
   jitter up to `max_attempts`, then moved to `skipq_dead_jobs`. Attempts are
   counted when a job is *claimed*, so a job that crashes the worker process
   (out-of-memory, segfault) also uses up attempts and eventually dead-letters.
6. **Idempotent enqueue.** If a job with the same `idempotency_key` already
   exists in `skipq_jobs`, `Enqueue` returns that job (with `Duplicate: true`)
   instead of inserting a second one, even when the duplicates race.
7. **Transactional enqueue.** `EnqueueTx` inserts the job inside the caller's
   transaction, so "write my row and enqueue a job about it" commits or rolls
   back together (the transactional outbox pattern).
8. **One clock.** Every time comparison (`run_at`, lease expiry) uses the
   database's `now()`, so clock drift between worker machines cannot cause a
   job to run early or a lease to expire early.

### Non-guarantees

1. **Not exactly-once.** A job runs twice if a worker finishes the work but
   crashes before the ack commits, or if the handler takes longer than its
   lease. Handlers must be idempotent (safe to run twice), for example by
   using the job ID as a dedupe key in their own writes.
2. **No strict ordering.** Jobs are claimed by `priority DESC, run_at, id`, but
   with many workers a later job can finish before an earlier one, and a retried
   job goes to the back of the line.
3. **No exact scheduling.** A job with `run_at = 10:00:00` runs at the first
   poll at or after 10:00:00, so up to one poll interval late (1 s by default).
4. **Idempotency window is the row's lifetime.** The key is unique only while
   the row is in `skipq_jobs`. After a job is dead-lettered (and, from M2, after
   retention deletes old succeeded jobs), the same key can be enqueued again.
5. **Not durable against losing committed writes.** If the Postgres primary
   fails over to an asynchronous replica, the last few committed transactions
   can be lost: an enqueued job can disappear, or an acked job can reappear
   and run again. Use synchronous replication if that matters.
6. **Handlers are cancelled cooperatively.** On shutdown, `JobTimeout` or a
   lost lease skipq cancels the handler's `context.Context`. A handler that
   ignores its context keeps running; skipq stops renewing its lease, and the
   job may be re-run elsewhere while the first copy is still going.

## 2. Schema

Migrations live in `migrations/` and are embedded in the binary with Go's
`embed` package. `skipq.Migrate` applies them in one transaction guarded by a
Postgres advisory lock (a named lock that is not tied to a row), so several
processes can call it at startup without racing.

```sql
CREATE TABLE skipq_jobs (
    id               bigserial PRIMARY KEY,
    queue            text        NOT NULL DEFAULT 'default',
    kind             text        NOT NULL,
    payload          jsonb       NOT NULL DEFAULT '{}',
    state            text        NOT NULL DEFAULT 'available'
                     CHECK (state IN ('available', 'running', 'succeeded')),
    priority         integer     NOT NULL DEFAULT 0,      -- higher runs first
    attempt          integer     NOT NULL DEFAULT 0,      -- incremented per claim
    max_attempts     integer     NOT NULL DEFAULT 25 CHECK (max_attempts > 0),
    run_at           timestamptz NOT NULL DEFAULT now(),  -- not before this time
    lease_expires_at timestamptz,                         -- set while running
    locked_by        text,                                -- worker ID, for debugging
    last_error       text,
    idempotency_key  text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz
);

-- The claim query reads this index in order and stops after LIMIT rows.
CREATE INDEX skipq_jobs_claim_idx ON skipq_jobs (queue, priority DESC, run_at, id)
    WHERE state = 'available';
-- The rescuer finds expired leases without scanning the table.
CREATE INDEX skipq_jobs_lease_idx ON skipq_jobs (lease_expires_at)
    WHERE state = 'running';
CREATE UNIQUE INDEX skipq_jobs_idempotency_key_idx ON skipq_jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE skipq_dead_jobs (
    id              bigint PRIMARY KEY,  -- same ID the job had in skipq_jobs
    queue, kind, payload, priority, attempt, max_attempts,
    last_error, idempotency_key, created_at,
    dead_at         timestamptz NOT NULL DEFAULT now()
);
```

Why partial indexes: most rows in a busy queue are `succeeded`. Indexing only
`available` (and only `running` for leases) keeps the hot indexes small, so the
claim query stays fast as history grows.

Why a separate dead-letter table instead of a `dead` state: dead jobs need a
human. Keeping them apart makes "what is broken?" one `SELECT * FROM
skipq_dead_jobs`, keeps them out of the claim index, and lets an operator retry
one by moving it back (`RetryDead`, or `POST /v1/dead-jobs/{id}/retry`).

## 3. State machine

```
                 Enqueue (run_at may be in the future)
                              │
                              ▼
   ┌──────────────────►  available  ◄──────────────────────────┐
   │                          │                                 │
   │                          │ Claim: FOR UPDATE SKIP LOCKED,   │
   │                          │ attempt += 1, lease set          │
   │                          ▼                                 │
   │  Release            running ──── lease expires, attempts ──┘
   │  (graceful          │  │  │       left (Rescue)
   │   shutdown) ◄───────┘  │  │
   │                        │  └──── handler error, attempts left:
   │                        │        run_at = now() + backoff ──► available
   │                        │
   │                Ack     │       handler error with no attempts left,
   │                        │       Permanent(err), unknown kind, or lease
   │                        ▼       expired with no attempts left
   │                   succeeded          │
   │                                      ▼
   └─ RetryDead (operator) ◄──────── skipq_dead_jobs
```

| From | Event | To | Condition |
|---|---|---|---|
| — | `Enqueue` | available | new key, or no key |
| — | `Enqueue` | (existing job) | key already in `skipq_jobs` |
| available | `Claim` | running | `run_at <= now()`, row not locked by another claimer |
| running | `Ack` | succeeded | `attempt` matches |
| running | `Fail` | available | `attempt < max_attempts`, error not permanent |
| running | `Fail` | dead | `attempt >= max_attempts`, or error is permanent |
| running | `Release` | available | graceful shutdown hit its timeout; attempt still counts |
| running | `Rescue` | available | `lease_expires_at < now()`, `attempt < max_attempts` |
| running | `Rescue` | dead | `lease_expires_at < now()`, `attempt >= max_attempts` |
| running | `ExtendLease` (heartbeat) | running | `attempt` matches; lease moves to `now() + Lease` |
| dead | `RetryDead` | available | idempotency key not taken by a newer job; `attempt` reset to 0 |

Each transition is a single SQL statement, so it is atomic without an explicit
transaction. The claim is:

```sql
WITH next AS (
    SELECT id FROM skipq_jobs
    WHERE state = 'available' AND queue = $1 AND run_at <= now()
    ORDER BY priority DESC, run_at, id
    LIMIT $2
    FOR UPDATE SKIP LOCKED      -- skip rows another claimer has locked
)
UPDATE skipq_jobs j
SET state = 'running', attempt = j.attempt + 1, locked_by = $3,
    lease_expires_at = now() + $4 * interval '1 microsecond', updated_at = now()
FROM next WHERE j.id = next.id
RETURNING j.*;
```

`FOR UPDATE` locks the selected rows; `SKIP LOCKED` makes a second claimer
step over locked rows instead of waiting. That is how many workers can claim
different jobs at once without blocking each other or taking the same one.

## 4. Failure modes

### 4.1 Worker crash (process killed, machine lost)

The job stays `running` with a lease in the future. Nothing else touches it
until the lease expires. Every pool runs a rescuer every `RescueInterval`
(default 30 s) that moves expired jobs back to `available` (or to dead if they
used all attempts). Recovery time is roughly lease length plus one rescue
interval.

The lease no longer limits how long a job may run. While a handler runs, the
pool renews the lease every `Lease/3` (a heartbeat), so a healthy job keeps it
however long it takes, and a crashed worker's job comes back within about one
lease (default 1 min) plus one rescue interval. How long a handler may run is a
separate setting, `JobTimeout` (default 30 min), after which its context is
cancelled and the run counts as a failed attempt.

### 4.2 Worker finishes but the ack is lost

Network blip or crash between "handler returned" and "ack committed". The job
is rescued after its lease and runs again. This is the main reason delivery is
at-least-once, and why handlers must be idempotent.

### 4.3 Database failover or restart

- While Postgres is down, claims, acks and enqueues fail with errors. Workers
  log the error and keep polling with their normal interval; pgx's pool
  reconnects when the database is back.
- A handler that finished while the database was down cannot ack. Its job is
  rescued after the lease and runs again.
- Heartbeats fail too. If the pool cannot renew a lease before it would
  expire, it cancels the handler with `ErrLeaseLost` as the context's cause,
  because another worker may legitimately claim the job once the database is
  back.
- With asynchronous replication, failover can lose recently committed writes
  (see non-guarantee 5). Lost acks mean re-runs (safe under at-least-once);
  lost enqueues mean lost jobs (not safe). The fix is synchronous replication
  or checking for the job after failover; skipq cannot fix this alone.

### 4.4 Poison jobs (a job that always fails)

- A handler error or panic is caught (panics are recovered and turned into
  errors with a stack trace) and counted as a failed attempt.
- After `max_attempts` the job moves to `skipq_dead_jobs` with its last error.
  With the default 25 attempts and a 1 h backoff cap, that takes about a day,
  so a fix deployed in that time lets the job succeed.
- A handler can return `skipq.Permanent(err)` to dead-letter immediately when
  retrying cannot help (bad payload, deleted user).
- A job of a kind no handler knows is dead-lettered at once with
  `no handler registered for kind "x"`.
- A job that crashes the whole worker process still uses up attempts because
  `attempt` is incremented at claim time, and the rescuer dead-letters it.

### 4.5 A worker loses its lease while the handler runs

With heartbeats this needs a pause longer than a whole lease: a long
stop-the-world pause, a laptop going to sleep, or the database unreachable for
that long. The rescuer re-queues the job and another worker may run it while
the first copy is still running. When the first worker's next heartbeat finds
the job reclaimed, it cancels the handler's context with cause `ErrLeaseLost`.
If the handler ignores that and later returns, the fenced ack or fail affects
no rows, so the job's recorded state is never overwritten by a stale worker.
Side effects of both runs still happen, which is why idempotent handlers
matter.

### 4.6 Graceful shutdown

Cancelling the context passed to `Pool.Run`:

1. Workers stop claiming new jobs. A claim that was already in flight and
   returns a job after shutdown began releases that job instead of running it.
2. In-flight handlers keep running until they finish or `ShutdownTimeout`
   (default 30 s) passes.
3. After the timeout, handler contexts are cancelled. Jobs whose handler
   returns because of that cancellation are *released*: put back to
   `available` with `run_at = now()` so another process picks them up straight
   away instead of waiting for the lease. The interrupted attempt still counts.
4. `Run` returns after every worker goroutine has exited.

### 4.7 Thundering herd and idle load

In M1 each worker polls independently (one cheap indexed query per worker per
poll interval, with jitter). With 100 idle workers and a 1 s interval that is
100 queries per second. M2 adds LISTEN/NOTIFY wake-ups and a single fetcher
per pool that claims in batches, which removes most idle polling.

## 5. Comparison

| | skipq | River | asynq | Plain LISTEN/NOTIFY |
|---|---|---|---|---|
| Storage | Postgres | Postgres | Redis | none (notifications are not stored) |
| Claim method | `FOR UPDATE SKIP LOCKED` | `FOR UPDATE SKIP LOCKED` | Redis Lua scripts moving task IDs between lists | — |
| Enqueue in your DB transaction | yes (`EnqueueTx`) | yes | no (different database) | `NOTIFY` is sent on commit, but the message is lost if nobody is listening |
| Delivery | at-least-once | at-least-once | at-least-once | at-most-once |
| Extra infrastructure | none beyond Postgres | none beyond Postgres | Redis | none |
| Maturity | learning project | production-grade, web UI, many features | production-grade, web UI | building block |

**River** is the closest relative and the one to read for ideas. It has more
job states (scheduled, retryable, cancelled, discarded), leader election for
maintenance tasks, unique-job options, periodic jobs, and a web UI. skipq is
deliberately smaller: it exists to make the core mechanisms (locking, leases,
fencing, backoff, shutdown) small enough to read in one sitting and to measure
them (M3).

**asynq** keeps jobs in Redis. Redis is fast, but the job cannot be committed
atomically with your Postgres data, so you either enqueue after commit (and
lose the job if the process dies in between) or before commit (and run a job
for data that rolled back). Durability depends on Redis persistence settings.
A Postgres queue avoids a second datastore and gets transactional enqueue for
free, at the cost of lower peak throughput and table bloat (dead row versions
from updates) that autovacuum must clean.

**LISTEN/NOTIFY alone is not a queue.** A notification goes only to sessions
that are listening at that moment; if a worker is down, the message is gone.
Payloads are limited to 8000 bytes by default. It is useful as a *wake-up
signal* on top of a jobs table ("there is new work, go and poll now"), which
is how skipq will use it in M2.

## 6. HTTP API and metrics

`cmd/skipq` serves the `server` package: `POST /v1/jobs`, `GET /v1/jobs/{id}`,
`GET /v1/dead-jobs`, `POST /v1/dead-jobs/{id}/retry`, `GET /healthz` and
`GET /metrics`. Request bodies are capped at 1 MiB; queue, kind and key at 200
bytes. With `SKIPQ_API_TOKEN` set, `/v1/` needs a bearer token (compared in
constant time); health and metrics stay open. Without a token the API trusts
anyone who can reach it, so the default listen address is loopback only.

Metrics come from two places (`metrics` package):

| Metric | Type | Source |
|---|---|---|
| `skipq_jobs{queue,state}` | gauge | database, per scrape: available, scheduled, running, dead |
| `skipq_queue_lag_seconds{queue}` | gauge | database: age of the oldest ready, unclaimed job |
| `skipq_jobs_enqueued_total{queue,duplicate}` | counter | API process |
| `skipq_job_runs_total{queue,kind,outcome}` | counter | worker process (Pool observer) |
| `skipq_job_wait_seconds{queue,kind}` | histogram | worker: run_at to claim, on the database clock |
| `skipq_job_run_seconds{queue,kind}` | histogram | worker: handler duration |

Database gauges are read at scrape time, so they are right across every
process; counters and histograms are per process, so each worker process
exposes its own `/metrics`. Jobs whose kind has no handler are labelled
`_unknown` so arbitrary kinds from callers cannot create unbounded series.
Queue names still become label values: keep them to a small fixed set.

## 7. Out of scope (for now)

Workflows and job dependencies, cron-style periodic jobs, cross-queue global
rate limits (M3 adds per-queue limits), multi-tenant fairness, and a web UI.
