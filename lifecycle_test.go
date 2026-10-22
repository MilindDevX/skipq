package skipq_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MilindDevX/skipq"
)

func claimOne(t *testing.T, c *skipq.Client, lease time.Duration) *skipq.Job {
	t.Helper()
	jobs, err := c.Claim(context.Background(), skipq.ClaimParams{WorkerID: "test-worker", Lease: lease})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 claimed job, got %d", len(jobs))
	}
	return jobs[0]
}

func claimNone(t *testing.T, c *skipq.Client) {
	t.Helper()
	jobs, err := c.Claim(context.Background(), skipq.ClaimParams{WorkerID: "test-worker", Lease: time.Minute})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected no claimable jobs, got %+v", jobs[0])
	}
}

func countDead(t *testing.T, db *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := db.QueryRow(context.Background(), "SELECT count(*) FROM skipq_dead_jobs").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestClaimMarksJobRunningWithLease(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	enqueued := mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})

	job := claimOne(t, c, time.Minute)

	if job.ID != enqueued.ID || job.State != skipq.StateRunning || job.Attempt != 1 || job.LockedBy != "test-worker" {
		t.Fatalf("unexpected claimed job: %+v", job)
	}
	if job.LeaseExpiresAt == nil || time.Until(*job.LeaseExpiresAt) < 50*time.Second {
		t.Fatalf("lease should be about a minute out, got %v", job.LeaseExpiresAt)
	}
	claimNone(t, c) // a running job cannot be claimed again
}

func TestClaimOrdersByPriorityThenAge(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	low := mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k", Priority: -1})
	mid1 := mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})
	high := mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k", Priority: 10})
	mid2 := mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})

	for _, want := range []int64{high.ID, mid1.ID, mid2.ID, low.ID} {
		if got := claimOne(t, c, time.Minute).ID; got != want {
			t.Fatalf("claimed job %d, want %d", got, want)
		}
	}
}

func TestClaimSkipsFutureJobsAndOtherQueues(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k", RunAt: time.Now().Add(time.Hour)})
	mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k", Queue: "other"})
	claimNone(t, c)

	past := mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k", RunAt: time.Now().Add(-time.Second)})
	if got := claimOne(t, c, time.Minute).ID; got != past.ID {
		t.Fatalf("claimed %d, want the past-due job %d", got, past.ID)
	}

	jobs, err := c.Claim(context.Background(), skipq.ClaimParams{Queue: "other", WorkerID: "w", Lease: time.Minute})
	if err != nil || len(jobs) != 1 || jobs[0].Queue != "other" {
		t.Fatalf("claim from queue other: %v, %v", jobs, err)
	}
}

func TestClaimRespectsLimit(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	for range 5 {
		mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})
	}
	jobs, err := c.Claim(context.Background(), skipq.ClaimParams{Limit: 3, WorkerID: "w", Lease: time.Minute})
	if err != nil || len(jobs) != 3 {
		t.Fatalf("expected 3 jobs, got %d (%v)", len(jobs), err)
	}
}

func TestAckMarksJobSucceeded(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})
	job := claimOne(t, c, time.Minute)

	if err := c.Ack(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	got := mustGet(t, c, job.ID)
	if got.State != skipq.StateSucceeded || got.FinishedAt == nil || got.LeaseExpiresAt != nil {
		t.Fatalf("unexpected job after ack: %+v", got)
	}
	if err := c.Ack(context.Background(), job); !errors.Is(err, skipq.ErrLeaseLost) {
		t.Fatalf("second ack should fail with ErrLeaseLost, got %v", err)
	}
}

func TestFailWithAttemptsLeftSchedulesRetry(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})
	job := claimOne(t, c, time.Minute)

	state, err := c.Fail(context.Background(), job, errors.New("smtp timeout"), time.Hour)
	if err != nil || state != skipq.StateAvailable {
		t.Fatalf("fail: state=%s err=%v", state, err)
	}

	got := mustGet(t, c, job.ID)
	if got.State != skipq.StateAvailable || got.LastError != "smtp timeout" || got.Attempt != 1 {
		t.Fatalf("unexpected job after fail: %+v", got)
	}
	if time.Until(got.RunAt) < 59*time.Minute {
		t.Fatalf("retry should be about an hour out, got run_at %v", got.RunAt)
	}
	claimNone(t, c)
}

func TestFailOnLastAttemptMovesJobToDeadLetter(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	c := skipq.New(db)
	mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k", MaxAttempts: 2, IdempotencyKey: "poison"})

	for attempt := 1; attempt <= 2; attempt++ {
		job := claimOne(t, c, time.Minute)
		if job.Attempt != attempt {
			t.Fatalf("attempt = %d, want %d", job.Attempt, attempt)
		}
		want := skipq.StateAvailable
		if attempt == 2 {
			want = skipq.StateDead
		}
		state, err := c.Fail(context.Background(), job, errors.New("boom"), 0)
		if err != nil || state != want {
			t.Fatalf("attempt %d: state=%s err=%v, want %s", attempt, state, err, want)
		}
	}

	claimNone(t, c)
	if n := countDead(t, db); n != 1 {
		t.Fatalf("dead jobs = %d, want 1", n)
	}
	var deadID int64
	if err := db.QueryRow(context.Background(), "SELECT id FROM skipq_dead_jobs").Scan(&deadID); err != nil {
		t.Fatal(err)
	}
	if dead := mustGet(t, c, deadID); dead.State != skipq.StateDead || dead.LastError != "boom" || dead.Attempt != 2 {
		t.Fatalf("unexpected dead job: %+v", dead)
	}

	// The idempotency key is only reserved while the job is in skipq_jobs.
	if res, err := c.Enqueue(context.Background(), skipq.EnqueueParams{Kind: "k", IdempotencyKey: "poison"}); err != nil || res.Duplicate {
		t.Fatalf("key should be free after dead-lettering: %+v %v", res, err)
	}
}

func TestPermanentErrorDeadLettersImmediately(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	c := skipq.New(db)
	mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})
	job := claimOne(t, c, time.Minute)

	state, err := c.Fail(context.Background(), job, skipq.Permanent(errors.New("user deleted")), 0)
	if err != nil || state != skipq.StateDead {
		t.Fatalf("state=%s err=%v", state, err)
	}
	if got := mustGet(t, c, job.ID); got.State != skipq.StateDead || got.LastError != "user deleted" {
		t.Fatalf("unexpected job: %+v", got)
	}
}

func TestRescueReturnsExpiredLeaseToQueue(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})
	claimOne(t, c, 20*time.Millisecond) // this "worker" crashes and never acks
	time.Sleep(60 * time.Millisecond)

	res, err := c.Rescue(context.Background())
	if err != nil || res.Retried != 1 || res.Dead != 0 {
		t.Fatalf("rescue: %+v %v", res, err)
	}

	job := claimOne(t, c, time.Minute)
	if job.Attempt != 2 || !strings.Contains(job.LastError, "lease expired") {
		t.Fatalf("rescued job: %+v", job)
	}
}

func TestRescueDoesNotTouchValidLeases(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})
	claimOne(t, c, time.Minute)

	res, err := c.Rescue(context.Background())
	if err != nil || res.Retried != 0 || res.Dead != 0 {
		t.Fatalf("rescue: %+v %v", res, err)
	}
}

func TestRescueDeadLettersExpiredJobWithNoAttemptsLeft(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	c := skipq.New(db)
	enqueued := mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k", MaxAttempts: 1})
	claimOne(t, c, 20*time.Millisecond)
	time.Sleep(60 * time.Millisecond)

	res, err := c.Rescue(context.Background())
	if err != nil || res.Retried != 0 || res.Dead != 1 {
		t.Fatalf("rescue: %+v %v", res, err)
	}
	if got := mustGet(t, c, enqueued.ID); got.State != skipq.StateDead {
		t.Fatalf("job should be dead, got %+v", got)
	}
}

func TestStaleWorkerCannotRecordResultAfterReclaim(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	ctx := context.Background()
	mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})

	stale := claimOne(t, c, 20*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	if _, err := c.Rescue(ctx); err != nil {
		t.Fatal(err)
	}
	fresh := claimOne(t, c, time.Minute)

	if err := c.Ack(ctx, stale); !errors.Is(err, skipq.ErrLeaseLost) {
		t.Fatalf("stale ack: want ErrLeaseLost, got %v", err)
	}
	if _, err := c.Fail(ctx, stale, errors.New("x"), 0); !errors.Is(err, skipq.ErrLeaseLost) {
		t.Fatalf("stale fail: want ErrLeaseLost, got %v", err)
	}
	if err := c.Release(ctx, stale); !errors.Is(err, skipq.ErrLeaseLost) {
		t.Fatalf("stale release: want ErrLeaseLost, got %v", err)
	}
	if err := c.Ack(ctx, fresh); err != nil {
		t.Fatalf("fresh ack: %v", err)
	}
}

func TestReleaseMakesJobClaimableImmediately(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})
	job := claimOne(t, c, time.Hour)

	if err := c.Release(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	again := claimOne(t, c, time.Minute)
	if again.ID != job.ID || again.Attempt != 2 {
		t.Fatalf("released job: %+v", again)
	}
}

func TestExtendLeaseMovesExpiryForward(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	ctx := context.Background()
	mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})
	job := claimOne(t, c, 50*time.Millisecond)

	expires, err := c.ExtendLease(ctx, job, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if time.Until(expires) < 59*time.Minute {
		t.Fatalf("lease should now end in about an hour, got %v", expires)
	}

	time.Sleep(100 * time.Millisecond) // past the original 50ms lease
	if res, err := c.Rescue(ctx); err != nil || res.Retried != 0 {
		t.Fatalf("extended job must not be rescued: %+v %v", res, err)
	}
}

func TestExtendLeaseFailsAfterReclaim(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	ctx := context.Background()
	mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k"})
	stale := claimOne(t, c, 20*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	if _, err := c.Rescue(ctx); err != nil {
		t.Fatal(err)
	}
	claimOne(t, c, time.Minute)

	if _, err := c.ExtendLease(ctx, stale, time.Hour); !errors.Is(err, skipq.ErrLeaseLost) {
		t.Fatalf("want ErrLeaseLost, got %v", err)
	}
}
