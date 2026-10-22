package skipq

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"sync"
	"time"
)

// Handler does the work for one job. Return nil to mark the job succeeded,
// an error to retry it (with backoff) or Permanent(err) to dead-letter it.
//
// ctx is cancelled when the job's lease ends or when graceful shutdown runs
// out of time; long-running handlers should watch ctx.Done(). Because a job
// can run more than once (see docs/design.md), handlers must be idempotent.
type Handler func(ctx context.Context, job *Job) error

// PoolConfig configures a worker pool. Only Handlers is required.
type PoolConfig struct {
	Queue    string             // queue to work; defaults to DefaultQueue
	Workers  int                // concurrent jobs; defaults to 10
	Handlers map[string]Handler // job kind -> handler

	Lease           time.Duration // how long a worker owns a claimed job; defaults to 5m
	PollInterval    time.Duration // idle wait between claim attempts; defaults to 1s
	RescueInterval  time.Duration // how often to rescue expired leases; defaults to 30s
	ShutdownTimeout time.Duration // how long shutdown waits for in-flight jobs; defaults to 30s

	// Backoff returns the retry delay after the given attempt fails.
	// Defaults to DefaultBackoff.
	Backoff func(attempt int) time.Duration
	Logger  *slog.Logger // defaults to slog.Default()
	// WorkerID prefixes the locked_by value of claimed jobs.
	// Defaults to "<hostname>-<pid>-<random>".
	WorkerID string
}

// Pool runs jobs from one queue on a fixed number of worker goroutines.
type Pool struct {
	client *Client
	cfg    PoolConfig
}

// abandonGrace is how long a worker waits, after cancelling a handler's
// context at shutdown, for the handler to return before releasing its job and
// leaving the handler's goroutine behind.
const abandonGrace = time.Second

// dbTimeout bounds the bookkeeping queries (claim, ack, fail, release). They
// run on a context that survives shutdown so a result is never dropped just
// because the pool is stopping.
const dbTimeout = 10 * time.Second

// errAbandoned is recorded when a handler ignored cancellation at shutdown.
var errAbandoned = errors.New("skipq: handler did not return after cancellation")

// NewPool returns a pool that runs jobs with the given handlers.
func (c *Client) NewPool(cfg PoolConfig) (*Pool, error) {
	if len(cfg.Handlers) == 0 {
		return nil, errors.New("skipq: PoolConfig.Handlers must not be empty")
	}
	cfg.Queue = cmp.Or(cfg.Queue, DefaultQueue)
	cfg.Workers = cmp.Or(cfg.Workers, 10)
	cfg.Lease = cmp.Or(cfg.Lease, 5*time.Minute)
	cfg.PollInterval = cmp.Or(cfg.PollInterval, time.Second)
	cfg.RescueInterval = cmp.Or(cfg.RescueInterval, 30*time.Second)
	cfg.ShutdownTimeout = cmp.Or(cfg.ShutdownTimeout, 30*time.Second)
	if cfg.Backoff == nil {
		cfg.Backoff = DefaultBackoff
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.WorkerID == "" {
		host, _ := os.Hostname()
		cfg.WorkerID = fmt.Sprintf("%s-%d-%04x", host, os.Getpid(), rand.IntN(0x10000))
	}
	if cfg.Workers < 0 || cfg.Lease < 0 || cfg.PollInterval < 0 || cfg.RescueInterval < 0 || cfg.ShutdownTimeout < 0 {
		return nil, errors.New("skipq: PoolConfig values must not be negative")
	}
	return &Pool{client: c, cfg: cfg}, nil
}

// Run works jobs until ctx is cancelled, then shuts down gracefully:
//
//  1. workers stop claiming new jobs;
//  2. in-flight handlers get up to ShutdownTimeout to finish;
//  3. after that their contexts are cancelled, and jobs that do not complete
//     are released so another process can pick them up immediately.
//
// Run returns once every worker has stopped.
func (p *Pool) Run(ctx context.Context) error {
	// handlerCtx is the parent of every handler's context. It is deliberately
	// not derived from ctx, so cancelling ctx (the shutdown signal) lets
	// in-flight handlers keep running until stopHandlers is called.
	handlerCtx, stopHandlers := context.WithCancel(context.WithoutCancel(ctx))
	defer stopHandlers()

	var wg sync.WaitGroup
	for i := range p.cfg.Workers {
		workerID := fmt.Sprintf("%s/%d", p.cfg.WorkerID, i)
		wg.Go(func() { p.work(ctx, handlerCtx, workerID) })
	}
	wg.Go(func() { p.rescueLoop(ctx) })

	<-ctx.Done()
	p.cfg.Logger.Info("skipq: shutting down", "queue", p.cfg.Queue, "timeout", p.cfg.ShutdownTimeout)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(p.cfg.ShutdownTimeout):
		p.cfg.Logger.Warn("skipq: shutdown timeout reached; cancelling in-flight jobs", "queue", p.cfg.Queue)
		stopHandlers()
		<-done
	}
	return nil
}

// work is one worker goroutine: claim a job, run it, record the result, repeat.
func (p *Pool) work(ctx, handlerCtx context.Context, workerID string) {
	for ctx.Err() == nil {
		// Claim on a context that shutdown does not cancel: if the claim
		// commits on the server, we must get the job back and run or release
		// it rather than leave it locked until its lease expires.
		claimCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dbTimeout)
		jobs, err := p.client.Claim(claimCtx, ClaimParams{
			Queue: p.cfg.Queue, Limit: 1, WorkerID: workerID, Lease: p.cfg.Lease,
		})
		cancel()
		if err != nil {
			p.cfg.Logger.Error("skipq: claim failed", "queue", p.cfg.Queue, "err", err)
		}
		if len(jobs) == 0 {
			sleep(ctx, jitter(p.cfg.PollInterval))
			continue
		}
		p.runJob(handlerCtx, jobs[0])
	}
}

func (p *Pool) runJob(handlerCtx context.Context, job *Job) {
	jobCtx, cancel := context.WithTimeout(handlerCtx, p.cfg.Lease)
	defer cancel()

	result := make(chan error, 1) // buffered so an abandoned handler can still send and exit
	go func() { result <- p.callHandler(jobCtx, job) }()

	var err error
	select {
	case err = <-result:
	case <-handlerCtx.Done(): // shutdown timeout: jobCtx is now cancelled too
		select {
		case err = <-result:
		case <-time.After(abandonGrace):
			err = errAbandoned
		}
	}

	dbCtx, cancelDB := context.WithTimeout(context.WithoutCancel(handlerCtx), dbTimeout)
	defer cancelDB()
	log := p.cfg.Logger.With("job_id", job.ID, "kind", job.Kind, "attempt", job.Attempt)

	switch {
	case err == nil:
		if ackErr := p.client.Ack(dbCtx, job); ackErr != nil {
			log.Error("skipq: ack failed; job will run again after its lease", "err", ackErr)
		}
	case handlerCtx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, errAbandoned)):
		// Interrupted by shutdown, not a real failure: hand it back.
		if relErr := p.client.Release(dbCtx, job); relErr != nil {
			log.Error("skipq: release failed; job will run again after its lease", "err", relErr)
		} else {
			log.Info("skipq: released unfinished job at shutdown")
		}
	default:
		state, failErr := p.client.Fail(dbCtx, job, err, p.cfg.Backoff(job.Attempt))
		switch {
		case failErr != nil:
			log.Error("skipq: recording failure failed", "err", failErr, "job_err", err)
		case state == StateDead:
			log.Error("skipq: job dead-lettered", "err", err)
		default:
			log.Warn("skipq: job failed; will retry", "err", err)
		}
	}
}

// callHandler runs the handler for job.Kind, turning a panic into an error so
// one bad job cannot crash the whole process.
func (p *Pool) callHandler(ctx context.Context, job *Job) (err error) {
	h, ok := p.cfg.Handlers[job.Kind]
	if !ok {
		return Permanent(fmt.Errorf("no handler registered for kind %q", job.Kind))
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, job)
}

func (p *Pool) rescueLoop(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.RescueInterval)
	defer ticker.Stop()
	for {
		res, err := p.client.Rescue(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			p.cfg.Logger.Error("skipq: rescue failed", "err", err)
		case res.Retried > 0 || res.Dead > 0:
			p.cfg.Logger.Warn("skipq: rescued jobs with expired leases", "retried", res.Retried, "dead", res.Dead)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// jitter spreads d over [d/2, 3d/2) so idle workers do not poll in lockstep.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d/2 + rand.N(d)
}

// sleep waits for d or until ctx is cancelled, whichever comes first.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
