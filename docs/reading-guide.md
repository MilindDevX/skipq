# Reading guide

Read the code in this order. Each file is short.

1. `migrations/0001_init.sql`: the tables. Everything else is SQL against these.
2. `skipq.go`: the `Job` type, `Client`, `Get`.
3. `enqueue.go`: inserting jobs, and how idempotency keys survive races.
4. `lifecycle.go`: claim, ack, fail, release, rescue. The heart of the queue.
5. `backoff.go`: retry delays.
6. `pool.go`: goroutines, shutdown. The hardest file; read it last.
7. Tests in the same order. `concurrency_test.go` is the headline test.

Try this: delete the line `FOR UPDATE SKIP LOCKED` in `Claim` and run
`go test -run TestNoJobRunsTwice ./...`. Jobs run several times each. Put it
back. Then remove `AND attempt = $2` from `Ack` and watch
`TestStaleWorkerCannotRecordResultAfterReclaim` fail.

## Go idioms used here

- **`context.Context`**: a value passed as the first argument that carries a
  cancellation signal and an optional deadline. `ctx.Done()` is a channel that
  closes when the context is cancelled. `context.WithTimeout` makes a child
  that cancels itself after a duration. `context.WithoutCancel` makes a child
  that ignores the parent's cancellation; the pool uses it so database writes
  still happen during shutdown.
- **goroutine** (`go f()`): runs `f` concurrently. Cheap (a few KB each), so one
  per worker is normal.
- **channel** (`make(chan error, 1)`): a typed pipe between goroutines. The `1`
  is a buffer, so the sender does not block if nobody is receiving yet.
- **`select`**: waits on several channel operations and runs whichever is ready
  first. Used for "job finished OR shutdown timeout OR context cancelled".
- **`sync.WaitGroup`**: counts running goroutines; `wg.Go(f)` starts one and
  `wg.Wait()` blocks until all have returned.
- **`defer`**: runs a call when the surrounding function returns, used for
  cleanup (`defer rows.Close()`). `recover()` inside a deferred function
  catches a panic (Go's crash-style error) so one bad job cannot kill the process.
- **Errors are values**: functions return `error` as the last result. `errors.Is`
  checks whether an error is (or wraps) a specific one, like `ErrLeaseLost`.
  `errors.As` checks for an error *type*, used by `IsPermanent`. `fmt.Errorf`
  with `%w` wraps an error so `Is`/`As` still see the original.
- **`cmp.Or(a, b)`**: returns the first argument that is not the zero value;
  used for defaults (`cmp.Or(p.Queue, "default")`).
- **`//go:embed`**: a compiler directive that bakes files (the SQL migrations)
  into the binary, so there is nothing extra to deploy.
- **`_test.go` files with `package skipq_test`**: tests compiled as a separate
  package, so they can only use the public API, the same as a real user.
- **`t.Parallel()`**: lets tests run at the same time. Safe here because each
  test has its own Postgres schema.
- **`-race`**: builds with Go's race detector, which reports two goroutines
  touching the same memory without synchronisation.
