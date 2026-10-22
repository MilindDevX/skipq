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
- **Worker pool** with graceful shutdown that finishes or releases in-flight jobs

Status: milestone 1 of 4 (core queue). HTTP API, Prometheus metrics, benchmarks
and chaos tests are next; see [docs/roadmap.md](docs/roadmap.md). The
guarantees, schema, state machine and failure modes are in
[docs/design.md](docs/design.md).

## Quickstart

Needs Go 1.25+ and Docker.

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

Stop Postgres and delete its data with `docker compose down -v`.

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

// Work.
pool, err := client.NewPool(skipq.PoolConfig{
    Workers: 10,
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
the work but before recording it), so make them idempotent.

## Layout

| Path | What it is |
|---|---|
| `skipq.go` | `Client`, `Job`, `Get` |
| `enqueue.go` | `Enqueue`, `EnqueueTx`, idempotency keys |
| `lifecycle.go` | `Claim`, `Ack`, `Fail`, `Release`, `Rescue`, `Permanent` |
| `backoff.go` | exponential backoff with jitter |
| `pool.go` | worker pool and graceful shutdown |
| `migrate.go`, `migrations/` | embedded schema migrations |
| `*_test.go` | integration tests against real Postgres |
| `examples/basic` | runnable demo |

## License

MIT
