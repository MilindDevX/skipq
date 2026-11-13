# skipq roadmap

Milestones 1 and 2 are done. Milestones 3 and 4 are planned and not started.

## M1: Core queue (done)

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
- [x] CI on GitHub Actions: gofmt, vet, `go test -race` with Postgres 17

## M2: Service (done)

- [x] `cmd/skipq`: a server binary with a small HTTP API
  - `POST /v1/jobs` (enqueue; honours the `Idempotency-Key` header)
  - `GET /v1/jobs/{id}` (status, including dead jobs)
  - `GET /v1/dead-jobs`, `POST /v1/dead-jobs/{id}/retry`
  - optional bearer token, 1 MiB body cap, `/healthz`
- [x] Prometheus metrics at `/metrics`: job counts per queue and state, queue
      lag, enqueue counter, runs by outcome, wait and run-time histograms
- [x] Lease heartbeat, plus a separate `JobTimeout`
- [x] Dockerfile for the server (multi-stage, distroless, non-root)
- [x] Fixed a shutdown race found as a flaky test: a claim that finished after
      shutdown began could start a new job

## M3: Proof under load and failure (planned)

- [ ] Load-test harness (`cmd/skipq-bench`): enqueue N jobs, run M workers,
      report throughput (jobs/s) and p50/p95/p99 enqueue-to-start latency,
      with the settings and hardware recorded in the README
- [ ] Chaos test: start worker processes, `SIGKILL` some of them mid-job, and
      assert that every job ends `succeeded` or dead, none is lost, and the
      only duplicates are jobs that were in flight on a killed worker

## M4: Real use and polish (planned)

- [ ] An example that enqueues from a Next.js app over the HTTP API with an
      idempotency key per item and works the jobs in Go
- [ ] README: architecture diagram, benchmark table, guarantees summary
- [ ] Tag `v0.1.0`

## Maybe later

- LISTEN/NOTIFY wake-ups with one batched fetcher per pool, replacing
  per-worker polling
- A retention job that deletes succeeded jobs older than N days
- An optional Redis token-bucket rate limit per queue
- A Go client package for the HTTP API
