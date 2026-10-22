# skipq

A durable background-job queue for Go, backed by Postgres.

Workers claim jobs with `SELECT … FOR UPDATE SKIP LOCKED`, so many workers can
take different jobs at the same time without blocking each other or taking the
same job twice. If a worker crashes, its lease runs out and the job comes back.

- **At-least-once delivery** with leases and fenced acks (a worker whose lease
  was taken over cannot overwrite the result)
- **Retries** with exponential backoff and jitter, then a **dead-letter table**
- **Idempotency keys**: enqueueing the same key twice returns the existing job
- **Transactional enqueue**: `EnqueueTx` commits the job with your own writes
- **Priorities** and **delayed jobs** (`RunAt`)
- **Worker pool** with lease heartbeats, job timeouts, and graceful shutdown
  that finishes or releases in-flight jobs
- **HTTP API** (`cmd/skipq`) to enqueue, inspect, and retry dead jobs
- **Prometheus metrics**: queue depth and lag, runs by outcome, wait and run
  time histograms

Status: milestones 1–2 of 4 done (core queue, service). Benchmarks and chaos
tests are next; see [docs/roadmap.md](docs/roadmap.md). The
guarantees, schema, state machine and failure modes are in
[docs/design.md](docs/design.md).

## Quickstart

Needs Go 1.26+ and Docker.

```bash
git clone https://github.com/MilindDevX/skipq.git
cd skipq
docker compose up -d --wait     # Postgres 17 on localhost:54329
go test -race ./...             # integration tests against that Postgres
go run ./examples/basic         # enqueue 20 jobs and work them
```

The example prints each job as it runs; every fifth job fails once and is
retried. Run it a second time and it reports `0 jobs (20 already existed)`
because each job has an idempotency key.

Run the HTTP API too (builds a small image; listens on `127.0.0.1:8080`):

```bash
docker compose --profile server up -d --build --wait
curl -s -X POST localhost:8080/v1/jobs -H 'Idempotency-Key: demo-1' \
     -d '{"kind":"email.send","payload":{"to":"a@example.com"}}'
curl -s localhost:8080/v1/jobs/1    # use the "id" from the response above
curl -s localhost:8080/metrics | grep '^skipq_'
```

Stop everything and delete the data with `docker compose --profile server down -v`.

Tests use `postgres://skipq:skipq@localhost:54329/skipq` by default. Point
them elsewhere with `SKIPQ_TEST_DATABASE_URL`. Each test creates and drops its
own schema, so they run in parallel and leave nothing behind.

## Usage

```go
db, _ := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
if err := skipq.Migrate(ctx, db); err != nil { ... }
client := skipq.New(db)

// Enqueue. Same key again returns the existing job with res.Duplicate == true.
res, err := client.Enqueue(ctx, skipq.EnqueueParams{
    Kind:           "email.send",
    Payload:        map[string]string{"to": "a@example.com"},
    IdempotencyKey: "welcome-email-user-42",
    RunAt:          time.Now().Add(10 * time.Minute), // optional delay
})

// Work. metrics.New(reg) is optional; it implements skipq.Observer.
pool, err := client.NewPool(skipq.PoolConfig{
    Workers:  10,
    Observer: metrics.New(prometheus.DefaultRegisterer),
    Handlers: map[string]skipq.Handler{
        "email.send": func(ctx context.Context, job *skipq.Job) error {
            // return nil to succeed, an error to retry,
            // or skipq.Permanent(err) to dead-letter now
            return sendEmail(ctx, job.Payload)
        },
    },
})
err = pool.Run(ctx) // blocks; cancel ctx for graceful shutdown

// Check status (works for dead-lettered jobs too).
job, err := client.Get(ctx, res.Job.ID)
```

Handlers can run more than once (for example if a worker crashes after doing
the work but before recording it), so make them idempotent. A handler's
context is cancelled at `JobTimeout` (default 30 min), at shutdown, or when its
lease is lost (`context.Cause(ctx)` is then `skipq.ErrLeaseLost`).

## HTTP API

| Request | Result |
|---|---|
| `POST /v1/jobs` | `201` new job, or `200` + `Idempotent-Replayed: true` if the `Idempotency-Key` header (or `idempotency_key` field) matches an existing job |
| `GET /v1/jobs/{id}` | `200` job (any state, including `dead`), `404` |
| `GET /v1/dead-jobs?queue=&limit=&before_id=` | `200` `{"jobs":[…],"next_before_id":N}`, newest first |
| `POST /v1/dead-jobs/{id}/retry` | `200` job back in the queue with fresh attempts, `404`, `409` if its key was reused |
| `GET /healthz` | `200`, or `503` if Postgres is unreachable |
| `GET /metrics` | Prometheus metrics |

Enqueue body: `kind` (required), `queue`, `payload` (any JSON), `priority`,
`max_attempts`, `run_at` (RFC 3339), `idempotency_key`. Bodies over 1 MiB get
`413`; unknown fields get `400`.

Server settings are environment variables: `DATABASE_URL`, `SKIPQ_ADDR`
(default `127.0.0.1:8080`), `SKIPQ_API_TOKEN` (if set, `/v1/` needs
`Authorization: Bearer <token>`), `SKIPQ_MIGRATE` (default `true`). There is no
other authentication, so set a token before exposing the API beyond your
machine. The metrics are listed in [docs/design.md](docs/design.md#6-http-api-and-metrics).

## Layout

| Path | What it is |
|---|---|
| `skipq.go` | `Client`, `Job`, `Get` |
| `enqueue.go` | `Enqueue`, `EnqueueTx`, idempotency keys |
| `lifecycle.go` | `Claim`, `Ack`, `Fail`, `ExtendLease`, `Release`, `Rescue`, `Permanent` |
| `deadjobs.go` | `ListDead`, `RetryDead` |
| `backoff.go` | exponential backoff with jitter |
| `pool.go`, `observer.go` | worker pool, heartbeats, graceful shutdown, events |
| `metrics/` | Prometheus observer and queue collector |
| `server/`, `cmd/skipq/` | HTTP API and its binary; `Dockerfile` builds it |
| `migrate.go`, `migrations/` | embedded schema migrations |
| `*_test.go`, `internal/testdb` | integration tests against real Postgres |
| `examples/basic` | runnable demo |

## License

MIT
