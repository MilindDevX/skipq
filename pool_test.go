package skipq_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MilindDevX/skipq"
)

// startPool runs a pool in the background and returns a stop function that
// cancels it and waits for Run to return.
func startPool(t *testing.T, c *skipq.Client, cfg skipq.PoolConfig) (stop func()) {
	t.Helper()
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 10 * time.Millisecond
	}
	if cfg.Backoff == nil {
		cfg.Backoff = func(int) time.Duration { return 0 }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	pool, err := c.NewPool(cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pool.Run(ctx) }()

	var stopped bool
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("pool.Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("pool did not shut down within 10s")
		}
	}
	t.Cleanup(stop)
	return stop
}

func waitForState(t *testing.T, c *skipq.Client, id int64, want skipq.JobState) *skipq.Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		job, err := c.Get(context.Background(), id)
		if err == nil && job.State == want {
			return job
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %d never reached %s; last: %+v (err %v)", id, want, job, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNewPoolRequiresHandlers(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	if _, err := c.NewPool(skipq.PoolConfig{}); err == nil {
		t.Fatal("expected an error for a pool with no handlers")
	}
}

func TestPoolRunsJobsAndAcksThem(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	var runs atomic.Int32
	startPool(t, c, skipq.PoolConfig{
		Workers: 3,
		Handlers: map[string]skipq.Handler{
			"greet": func(ctx context.Context, job *skipq.Job) error {
				runs.Add(1)
				return nil
			},
		},
	})

	var ids []int64
	for range 10 {
		ids = append(ids, mustEnqueue(t, c, skipq.EnqueueParams{Kind: "greet"}).ID)
	}
	for _, id := range ids {
		waitForState(t, c, id, skipq.StateSucceeded)
	}
	if n := runs.Load(); n != 10 {
		t.Fatalf("handler ran %d times, want 10", n)
	}
}

func TestPoolRetriesFailedJobUntilItSucceeds(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	startPool(t, c, skipq.PoolConfig{
		Handlers: map[string]skipq.Handler{
			"flaky": func(ctx context.Context, job *skipq.Job) error {
				if job.Attempt < 3 {
					return errors.New("not yet")
				}
				return nil
			},
		},
	})

	job := waitForState(t, c, mustEnqueue(t, c, skipq.EnqueueParams{Kind: "flaky"}).ID, skipq.StateSucceeded)
	if job.Attempt != 3 || job.LastError != "not yet" {
		t.Fatalf("unexpected job: %+v", job)
	}
}

func TestPoolDeadLettersPanickingJob(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	startPool(t, c, skipq.PoolConfig{
		Handlers: map[string]skipq.Handler{
			"poison": func(ctx context.Context, job *skipq.Job) error { panic("nil map write") },
		},
	})

	enqueued := mustEnqueue(t, c, skipq.EnqueueParams{Kind: "poison", MaxAttempts: 2})
	job := waitForState(t, c, enqueued.ID, skipq.StateDead)
	if job.Attempt != 2 || !strings.Contains(job.LastError, "panic: nil map write") {
		t.Fatalf("unexpected dead job: %+v", job)
	}
}

func TestPoolDeadLettersUnknownKindAndPermanentErrors(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	startPool(t, c, skipq.PoolConfig{
		Handlers: map[string]skipq.Handler{
			"bad-payload": func(ctx context.Context, job *skipq.Job) error {
				return skipq.Permanent(errors.New("payload missing user_id"))
			},
		},
	})

	unknown := waitForState(t, c, mustEnqueue(t, c, skipq.EnqueueParams{Kind: "nobody-handles-this"}).ID, skipq.StateDead)
	if unknown.Attempt != 1 || !strings.Contains(unknown.LastError, `no handler registered for kind "nobody-handles-this"`) {
		t.Fatalf("unknown kind: %+v", unknown)
	}
	permanent := waitForState(t, c, mustEnqueue(t, c, skipq.EnqueueParams{Kind: "bad-payload"}).ID, skipq.StateDead)
	if permanent.Attempt != 1 || permanent.LastError != "payload missing user_id" {
		t.Fatalf("permanent error: %+v", permanent)
	}
}

func TestPoolRescuesJobFromCrashedWorker(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	enqueued := mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})
	claimOne(t, c, 50*time.Millisecond) // a worker claims it and then "crashes"

	startPool(t, c, skipq.PoolConfig{
		RescueInterval: 20 * time.Millisecond,
		Handlers:       map[string]skipq.Handler{"k": func(context.Context, *skipq.Job) error { return nil }},
	})

	job := waitForState(t, c, enqueued.ID, skipq.StateSucceeded)
	if job.Attempt != 2 {
		t.Fatalf("expected the rescued job to run as attempt 2, got %+v", job)
	}
}

func TestPoolHandlerContextEndsAtJobTimeout(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	startPool(t, c, skipq.PoolConfig{
		JobTimeout: 50 * time.Millisecond,
		Handlers: map[string]skipq.Handler{
			"slow": func(ctx context.Context, job *skipq.Job) error {
				<-ctx.Done()
				return skipq.Permanent(ctx.Err())
			},
		},
	})

	job := waitForState(t, c, mustEnqueue(t, c, skipq.EnqueueParams{Kind: "slow"}).ID, skipq.StateDead)
	if job.LastError != context.DeadlineExceeded.Error() {
		t.Fatalf("expected JobTimeout to cancel the handler, got %q", job.LastError)
	}
}

func TestHeartbeatKeepsLongJobFromBeingRescued(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	var runs atomic.Int32
	startPool(t, c, skipq.PoolConfig{
		Lease:          time.Second,           // heartbeat about every 330ms
		RescueInterval: 20 * time.Millisecond, // rescuer is eager to steal it
		Handlers: map[string]skipq.Handler{
			"long": func(ctx context.Context, job *skipq.Job) error {
				runs.Add(1)
				select {
				case <-time.After(3 * time.Second): // 3x the lease
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		},
	})

	job := waitForState(t, c, mustEnqueue(t, c, skipq.EnqueueParams{Kind: "long"}).ID, skipq.StateSucceeded)
	if job.Attempt != 1 || runs.Load() != 1 {
		t.Fatalf("job should run once on attempt 1; attempt=%d runs=%d", job.Attempt, runs.Load())
	}
}

func TestHandlerIsCancelledWhenLeaseIsLost(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	c := skipq.New(db)
	started := make(chan int64, 1)
	cause := make(chan error, 1)
	startPool(t, c, skipq.PoolConfig{
		Lease: 150 * time.Millisecond,
		Handlers: map[string]skipq.Handler{
			"k": func(ctx context.Context, job *skipq.Job) error {
				started <- job.ID
				<-ctx.Done()
				cause <- context.Cause(ctx)
				return ctx.Err()
			},
		},
	})
	mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})

	id := <-started
	// Simulate another worker reclaiming the job: bump the fencing token.
	if _, err := db.Exec(context.Background(), "UPDATE skipq_jobs SET attempt = attempt + 1 WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-cause:
		if !errors.Is(err, skipq.ErrLeaseLost) {
			t.Fatalf("context cause = %v, want ErrLeaseLost", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler was not cancelled after its lease was lost")
	}
}

type recordingObserver struct {
	mu       sync.Mutex
	started  []skipq.JobEvent
	finished []skipq.JobEvent
}

func (o *recordingObserver) JobStarted(e skipq.JobEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.started = append(o.started, e)
}

func (o *recordingObserver) JobFinished(e skipq.JobEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.finished = append(o.finished, e)
}

func (o *recordingObserver) outcomes() map[string][]skipq.Outcome {
	o.mu.Lock()
	defer o.mu.Unlock()
	m := map[string][]skipq.Outcome{}
	for _, e := range o.finished {
		m[e.Kind] = append(m[e.Kind], e.Outcome)
	}
	return m
}

func TestPoolReportsEventsToObserver(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	obs := &recordingObserver{}
	startPool(t, c, skipq.PoolConfig{
		Observer: obs,
		Handlers: map[string]skipq.Handler{
			"ok": func(context.Context, *skipq.Job) error { return nil },
			"flaky": func(ctx context.Context, job *skipq.Job) error {
				if job.Attempt == 1 {
					return errors.New("first try fails")
				}
				return nil
			},
			"bad": func(context.Context, *skipq.Job) error { return skipq.Permanent(errors.New("no")) },
		},
	})
	ids := []int64{
		mustEnqueue(t, c, skipq.EnqueueParams{Kind: "ok"}).ID,
		mustEnqueue(t, c, skipq.EnqueueParams{Kind: "flaky"}).ID,
		mustEnqueue(t, c, skipq.EnqueueParams{Kind: "bad"}).ID,
		mustEnqueue(t, c, skipq.EnqueueParams{Kind: "who-knows"}).ID,
	}
	waitForState(t, c, ids[0], skipq.StateSucceeded)
	waitForState(t, c, ids[1], skipq.StateSucceeded)
	waitForState(t, c, ids[2], skipq.StateDead)
	waitForState(t, c, ids[3], skipq.StateDead)

	got := obs.outcomes()
	want := map[string][]skipq.Outcome{
		"ok":       {skipq.OutcomeSucceeded},
		"flaky":    {skipq.OutcomeRetried, skipq.OutcomeSucceeded},
		"bad":      {skipq.OutcomeDead},
		"_unknown": {skipq.OutcomeDead}, // unregistered kinds share one label
	}
	for kind, w := range want {
		if fmt.Sprint(got[kind]) != fmt.Sprint(w) {
			t.Errorf("kind %s: outcomes %v, want %v", kind, got[kind], w)
		}
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.started) != 5 {
		t.Errorf("started events = %d, want 5", len(obs.started))
	}
	for _, e := range obs.started {
		if e.Queue != "default" || e.Wait < 0 || e.Wait > 10*time.Second {
			t.Errorf("bad started event: %+v", e)
		}
	}
}

func TestGracefulShutdownFinishesInFlightJob(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	started := make(chan struct{})
	stop := startPool(t, c, skipq.PoolConfig{
		ShutdownTimeout: 5 * time.Second,
		Handlers: map[string]skipq.Handler{
			"report": func(ctx context.Context, job *skipq.Job) error {
				close(started)
				time.Sleep(200 * time.Millisecond) // keeps working after shutdown begins
				return ctx.Err()                   // nil: shutdown did not cancel it
			},
		},
	})
	enqueued := mustEnqueue(t, c, skipq.EnqueueParams{Kind: "report"})

	<-started
	stop()

	if job := mustGet(t, c, enqueued.ID); job.State != skipq.StateSucceeded {
		t.Fatalf("in-flight job should finish during shutdown, got %+v", job)
	}
}

func TestShutdownTimeoutReleasesUnfinishedJob(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	started := make(chan struct{})
	stop := startPool(t, c, skipq.PoolConfig{
		ShutdownTimeout: 100 * time.Millisecond,
		Handlers: map[string]skipq.Handler{
			"long": func(ctx context.Context, job *skipq.Job) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			},
		},
	})
	enqueued := mustEnqueue(t, c, skipq.EnqueueParams{Kind: "long"})

	<-started
	begin := time.Now()
	stop()
	if took := time.Since(begin); took > 3*time.Second {
		t.Fatalf("shutdown took %v; it should stop soon after the 100ms timeout", took)
	}

	job := mustGet(t, c, enqueued.ID)
	if job.State != skipq.StateAvailable || job.Attempt != 1 || job.LastError != "" || time.Until(job.RunAt) > 0 {
		t.Fatalf("job should be released for immediate pickup, got %+v", job)
	}
}

func TestShutdownDoesNotWaitForeverOnHandlerThatIgnoresContext(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	started := make(chan struct{})
	never := make(chan struct{})
	t.Cleanup(func() { close(never) }) // let the abandoned goroutine exit
	stop := startPool(t, c, skipq.PoolConfig{
		ShutdownTimeout: 50 * time.Millisecond,
		Handlers: map[string]skipq.Handler{
			"stuck": func(ctx context.Context, job *skipq.Job) error {
				close(started)
				<-never
				return nil
			},
		},
	})
	enqueued := mustEnqueue(t, c, skipq.EnqueueParams{Kind: "stuck"})

	<-started
	stop() // fails the test if Run does not return within 10s

	if job := mustGet(t, c, enqueued.ID); job.State != skipq.StateAvailable {
		t.Fatalf("abandoned job should be released, got %+v", job)
	}
}
