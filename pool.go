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
// ctx is cancelled when JobTimeout passes, when the job's lease is lost
// (context.Cause(ctx) is then ErrLeaseLost), or when graceful shutdown runs
// out of time; long-running handlers should watch ctx.Done(). Because a job
// can run more than once (see docs/design.md), handlers must be idempotent.
type Handler func(ctx context.Context, job *Job) error

// PoolConfig configures a worker pool. Only Handlers is required.
type PoolConfig struct {
	Queue    string             // queue to work; defaults to DefaultQueue
	Workers  int                // concurrent jobs; defaults to 10
	Handlers map[string]Handler // job kind -> handler

	// Lease is how long a claimed job stays owned without a heartbeat. The
	// pool renews it every Lease/3 while the handler runs, so this bounds how
	// quickly a crashed worker's jobs come back, not how long a job may run.
	// Defaults to 1m.
	Lease time.Duration
	// JobTimeout is the longest a handler may run before its context is
	// cancelled. Defaults to 30m.
	JobTimeout      time.Duration
	PollInterval    time.Duration // idle wait between claim attempts; defaults to 1s
	RescueInterval  time.Duration // how often to rescue expired leases; defaults to 30s
	ShutdownTimeout time.Duration // how long shutdown waits for in-flight jobs; defaults to 30s

	// Backoff returns the retry delay after the given attempt fails.
	// Defaults to DefaultBackoff.
	Backoff  func(attempt int) time.Duration
	Observer Observer     // receives job events, e.g. for metrics; optional
	Logger   *slog.Logger // defaults to slog.Default()
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
	cfg.Lease = cmp.Or(cfg.Lease, time.Minute)
	cfg.JobTimeout = cmp.Or(cfg.JobTimeout, 30*time.Minute)
	cfg.PollInterval = cmp.Or(cfg.PollInterval, time.Second)
	cfg.RescueInterval = cmp.Or(cfg.RescueInterval, 30*time.Second)
	cfg.ShutdownTimeout = cmp.Or(cfg.ShutdownTimeout, 30*time.Second)
	if cfg.Backoff == nil {
		cfg.Backoff = DefaultBackoff
	}
	if cfg.Observer == nil {
		cfg.Observer = nopObserver{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.WorkerID == "" {
		host, _ := os.Hostname()
		cfg.WorkerID = fmt.Sprintf("%s-%d-%04x", host, os.Getpid(), rand.IntN(0x10000))
	}
	if cfg.Workers < 0 || cfg.JobTimeout < 0 || cfg.PollInterval < 0 || cfg.RescueInterval < 0 || cfg.ShutdownTimeout < 0 {
		return nil, errors.New("skipq: PoolConfig values must not be negative")
	}
	if cfg.Lease < 3*time.Millisecond {
		return nil, errors.New("skipq: PoolConfig.Lease must be at least 3ms")
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
		claimedAt := time.Now()
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
		if ctx.Err() != nil {
			// Shutdown began while the claim was in flight. Hand the job
			// back instead of starting new work.
			p.release(handlerCtx, jobs[0])
			return
		}
		p.runJob(handlerCtx, jobs[0], claimedAt)
	}
}

func (p *Pool) runJob(handlerCtx context.Context, job *Job, claimedAt time.Time) {
	handler, known := p.cfg.Handlers[job.Kind]
	event := JobEvent{
		Queue:   job.Queue,
		Kind:    job.Kind,
		JobID:   job.ID,
		Attempt: job.Attempt,
		Wait:    max(job.UpdatedAt.Sub(job.RunAt), 0), // both set by the database at claim time
	}
	if !known {
		event.Kind = UnknownKind
	}
	p.cfg.Observer.JobStarted(event)

	// jobCtx ends on shutdown timeout (via handlerCtx), JobTimeout, or a lost
	// lease (cancelLease, with ErrLeaseLost as the cause).
	leaseCtx, cancelLease := context.WithCancelCause(handlerCtx)
	defer cancelLease(nil)
	jobCtx, cancelTimeout := context.WithTimeout(leaseCtx, p.cfg.JobTimeout)
	defer cancelTimeout()
	stopHeartbeat := p.heartbeat(jobCtx, cancelLease, job, claimedAt)

	// The handler gets its own copy of the job, so nothing it does to the
	// struct can affect the heartbeat or the result we record.
	handlerJob := *job
	start := time.Now()
	result := make(chan error, 1) // buffered so an abandoned handler can still send and exit
	go func() { result <- p.callHandler(jobCtx, handler, &handlerJob) }()

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
	stopHeartbeat()

	event.Duration = time.Since(start)
	event.Err = err
	event.Outcome = p.record(handlerCtx, leaseCtx, job, err)
	p.cfg.Observer.JobFinished(event)
}

// record writes the handler's result to the database and reports the outcome.
func (p *Pool) record(handlerCtx, leaseCtx context.Context, job *Job, err error) Outcome {
	log := p.cfg.Logger.With("job_id", job.ID, "kind", job.Kind, "attempt", job.Attempt)
	if errors.Is(context.Cause(leaseCtx), ErrLeaseLost) {
		log.Warn("skipq: lease lost while running; result not recorded", "err", err)
		return OutcomeLeaseLost
	}
	if handlerCtx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, errAbandoned)) {
		// Interrupted by shutdown, not a real failure: hand it back.
		p.release(handlerCtx, job)
		return OutcomeReleased
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(handlerCtx), dbTimeout)
	defer cancel()

	if err == nil {
		switch ackErr := p.client.Ack(ctx, job); {
		case errors.Is(ackErr, ErrLeaseLost):
			log.Warn("skipq: job finished but its lease was lost; another worker owns it")
			return OutcomeLeaseLost
		case ackErr != nil:
			log.Error("skipq: ack failed; job will run again after its lease", "err", ackErr)
			return OutcomeRecordFailed
		}
		return OutcomeSucceeded
	}

	state, failErr := p.client.Fail(ctx, job, err, p.cfg.Backoff(job.Attempt))
	switch {
	case errors.Is(failErr, ErrLeaseLost):
		log.Warn("skipq: job failed but its lease was lost; another worker owns it", "job_err", err)
		return OutcomeLeaseLost
	case failErr != nil:
		log.Error("skipq: recording failure failed", "err", failErr, "job_err", err)
		return OutcomeRecordFailed
	case state == StateDead:
		log.Error("skipq: job dead-lettered", "err", err)
		return OutcomeDead
	default:
		log.Warn("skipq: job failed; will retry", "err", err)
		return OutcomeRetried
	}
}

// heartbeat renews job's lease every Lease/3 until stop is called or jobCtx
// ends. If the job turns out to be reclaimed, or the lease runs out while the
// database is unreachable, it cancels the job with ErrLeaseLost so the
// handler stops instead of racing another worker.
func (p *Pool) heartbeat(jobCtx context.Context, cancelJob context.CancelCauseFunc, job *Job, claimedAt time.Time) (stop func()) {
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		interval := p.cfg.Lease / 3
		timer := time.NewTimer(interval)
		defer timer.Stop()
		// The database set the lease to (its now) + Lease, and its now came
		// after we sent the request, so local send time + Lease is a safe
		// lower bound for when the lease ends, whatever the clocks say.
		leaseEnd := claimedAt.Add(p.cfg.Lease)
		for {
			select {
			case <-done:
				return
			case <-jobCtx.Done():
				return
			case <-timer.C:
			}
			// A renewal may take as long as the lease is still valid: a slow
			// database is not a reason to give up a lease we still hold.
			sent := time.Now()
			ctx, cancel := context.WithDeadline(context.WithoutCancel(jobCtx), leaseEnd)
			_, err := p.client.ExtendLease(ctx, job, p.cfg.Lease)
			cancel()
			switch {
			case err == nil:
				leaseEnd = sent.Add(p.cfg.Lease)
				timer.Reset(interval)
			case errors.Is(err, ErrLeaseLost):
				cancelJob(ErrLeaseLost)
				return
			case !time.Now().Before(leaseEnd):
				cancelJob(fmt.Errorf("%w: could not renew before it expired: %v", ErrLeaseLost, err))
				return
			default:
				// Still inside the lease: retry soon rather than a full interval later.
				p.cfg.Logger.Warn("skipq: lease renewal failed; will retry", "job_id", job.ID, "err", err)
				timer.Reset(min(interval/4, time.Until(leaseEnd)))
			}
		}
	}()
	return func() {
		close(done)
		<-exited
	}
}

// release hands a job back at shutdown so another process can claim it now.
func (p *Pool) release(handlerCtx context.Context, job *Job) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(handlerCtx), dbTimeout)
	defer cancel()
	log := p.cfg.Logger.With("job_id", job.ID, "kind", job.Kind, "attempt", job.Attempt)
	if err := p.client.Release(ctx, job); err != nil {
		log.Error("skipq: release failed; job will run again after its lease", "err", err)
		return
	}
	log.Info("skipq: released unfinished job at shutdown")
}

// callHandler runs handler, turning a panic into an error so one bad job
// cannot crash the whole process. A nil handler means the kind is unknown.
func (p *Pool) callHandler(ctx context.Context, handler Handler, job *Job) (err error) {
	if handler == nil {
		return Permanent(fmt.Errorf("no handler registered for kind %q", job.Kind))
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	return handler(ctx, job)
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
