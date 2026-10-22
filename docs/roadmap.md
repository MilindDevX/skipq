# skipq roadmap

Budget: 15–20 hours per week, around coursework. Target: résumé-ready by
**13 December 2026**. Each milestone has a "must" list and a "cut if late"
list, so a heavy exam week shrinks a milestone instead of sliding the date.

Mid-sem recovery comes first. If an exam week leaves under 10 hours, do only
the "must" items and move the rest to the next milestone.

## M1 — Core queue (22 Oct – 8 Nov, ~2.5 weeks)

The code for M1 already exists. Most of the time here is understanding it well
enough to explain every line in an interview.

Must:
- [x] Migrations embedded in the binary, applied under an advisory lock
- [x] `Enqueue` / `EnqueueTx` with priority, delayed `RunAt`, idempotency key
- [x] `Claim` with `FOR UPDATE SKIP LOCKED` and a lease
- [x] `Ack`, `Fail` (exponential backoff with jitter), dead-letter table
- [x] Fencing: stale workers get `ErrLeaseLost`
- [x] `Rescue` for expired leases
- [x] Worker pool: goroutines, context cancellation, graceful shutdown that
      releases unfinished jobs, panic recovery
- [x] Integration tests on real Postgres, including the N-worker
      no-double-run test, all passing with `-race`
- [ ] Milind: read every file, run the tests, break something on purpose and
      watch which test fails (~8 h)
- [ ] Milind: write a short "how a job flows" note in your own words (~2 h)
- [x] GitHub Actions CI workflow written (`.github/workflows/ci.yml`)
- [ ] CI: confirm it passes on the first push (~1 h if it needs fixes)

## M2 — Service shape (9 Nov – 22 Nov, 2 weeks)

Must:
- `cmd/skipq`: a server binary with a small HTTP API
  - `POST /v1/jobs` (enqueue; honours `Idempotency-Key` header)
  - `GET /v1/jobs/{id}` (status, including dead jobs)
  - `GET /v1/dead-jobs`, `POST /v1/dead-jobs/{id}/retry`
- Prometheus metrics at `/metrics`: jobs enqueued, running, succeeded, failed,
  dead (counters/gauges per queue and kind), and a histogram of
  enqueue-to-start latency and run duration
- Lease heartbeat: extend the lease while a handler is still running
- Dockerfile for the server (multi-stage, small image)

Cut if late:
- LISTEN/NOTIFY wake-ups plus one batched fetcher per pool (replaces
  per-worker polling)
- Retention job that deletes succeeded jobs older than N days

## M3 — Proof under load and failure (23 Nov – 6 Dec, 2 weeks)

Must:
- Load-test harness (`cmd/skipq-bench`): enqueue N jobs, run M workers,
  report throughput (jobs/s) and p50/p95/p99 enqueue-to-start latency.
  Run on the laptop with fixed settings and record the numbers and the
  hardware in the README. Keep it within 8 GB RAM.
- Chaos test: start worker *processes*, `SIGKILL` (force-kill) some of them
  mid-job, and assert every job ends `succeeded` or dead, none lost, and the
  only duplicates are jobs that were in flight on a killed worker.

Cut if late:
- Optional Redis token-bucket rate limit per queue (a "token bucket" refills
  N tokens per second; each job takes one token, so a queue cannot exceed
  N jobs/s across all workers). Off unless `REDIS_URL` is set.

## M4 — Real use and polish (7 Dec – 13 Dec, 1 week)

Must:
- FeedbackOS integration example: FeedbackOS (Next.js) calls
  `POST /v1/jobs` with an idempotency key for each feedback item's
  classification, and a Go worker runs the classifier. Lives under
  `examples/feedbackos/`; no FeedbackOS production changes without review.
- README: architecture diagram, benchmark table, "guarantees" summary,
  design-doc link. Tag `v0.1.0`.
- Résumé bullets drafted from real numbers (throughput, p99, chaos results).

Cut if late:
- Go client package for the HTTP API
- Short blog post: "What `SKIP LOCKED` actually does"

## Interview talking points this should give you

- Why `SKIP LOCKED` and what happens without it (workers queue behind one lock)
- At-least-once vs exactly-once, and why idempotent handlers are the answer
- Fencing tokens (the `attempt` check) and the stale-worker problem
- Backoff with jitter and why jitter prevents synchronized retry storms
- Graceful shutdown with `context` cancellation in Go
- Postgres vs Redis for queues: transactional enqueue vs raw throughput
