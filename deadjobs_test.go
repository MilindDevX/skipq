package skipq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MilindDevX/skipq"
)

// killJob enqueues a job and fails it permanently so it lands in skipq_dead_jobs.
func killJob(t *testing.T, c *skipq.Client, p skipq.EnqueueParams) *skipq.Job {
	t.Helper()
	enqueued := mustEnqueue(t, c, p)
	jobs, err := c.Claim(context.Background(), skipq.ClaimParams{Queue: enqueued.Queue, WorkerID: "w", Lease: time.Minute})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: %v %v", jobs, err)
	}
	if _, err := c.Fail(context.Background(), jobs[0], skipq.Permanent(errors.New("broken")), 0); err != nil {
		t.Fatal(err)
	}
	return enqueued
}

func TestListDeadNewestFirstWithPaging(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	ctx := context.Background()
	var ids []int64
	for range 5 {
		ids = append(ids, killJob(t, c, skipq.EnqueueParams{Kind: "k"}).ID)
	}
	killJob(t, c, skipq.EnqueueParams{Kind: "k", Queue: "other"})

	page1, err := c.ListDead(ctx, skipq.ListDeadParams{Queue: "default", Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 3 || page1[0].ID != ids[4] || page1[2].ID != ids[2] {
		t.Fatalf("page 1: %v", jobIDs(page1))
	}
	if page1[0].State != skipq.StateDead || page1[0].LastError != "broken" {
		t.Fatalf("dead job fields: %+v", page1[0])
	}

	page2, err := c.ListDead(ctx, skipq.ListDeadParams{Queue: "default", Limit: 3, BeforeID: page1[2].ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 || page2[0].ID != ids[1] || page2[1].ID != ids[0] {
		t.Fatalf("page 2: %v", jobIDs(page2))
	}

	other, err := c.ListDead(ctx, skipq.ListDeadParams{Queue: "other"})
	if err != nil || len(other) != 1 || other[0].Queue != "other" {
		t.Fatalf("queue filter: %v %v", jobIDs(other), err)
	}
}

func TestRetryDeadPutsJobBackWithFreshAttempts(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	c := skipq.New(db)
	ctx := context.Background()
	dead := killJob(t, c, skipq.EnqueueParams{Kind: "k", MaxAttempts: 3, IdempotencyKey: "order-7"})

	job, err := c.RetryDead(ctx, dead.ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != dead.ID || job.State != skipq.StateAvailable || job.Attempt != 0 ||
		job.MaxAttempts != 3 || job.LastError != "broken" || job.IdempotencyKey != "order-7" {
		t.Fatalf("retried job: %+v", job)
	}
	if n := countDead(t, db); n != 0 {
		t.Fatalf("dead jobs = %d, want 0", n)
	}
	if got := claimOne(t, c, time.Minute); got.ID != dead.ID || got.Attempt != 1 {
		t.Fatalf("claimed %+v", got)
	}
	// The key is reserved again now that the job is back in skipq_jobs.
	if res, err := c.Enqueue(ctx, skipq.EnqueueParams{Kind: "k", IdempotencyKey: "order-7"}); err != nil || !res.Duplicate {
		t.Fatalf("enqueue with same key: %+v %v", res, err)
	}
}

func TestRetryDeadUnknownJob(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	if _, err := c.RetryDead(context.Background(), 12345); !errors.Is(err, skipq.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestRetryDeadConflictsWhenKeyWasReused(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	c := skipq.New(db)
	ctx := context.Background()
	dead := killJob(t, c, skipq.EnqueueParams{Kind: "k", IdempotencyKey: "invoice-1"})
	mustEnqueue(t, c, skipq.EnqueueParams{Kind: "k", IdempotencyKey: "invoice-1"}) // key reused

	if _, err := c.RetryDead(ctx, dead.ID); !errors.Is(err, skipq.ErrIdempotencyConflict) {
		t.Fatalf("want ErrIdempotencyConflict, got %v", err)
	}
	if n := countDead(t, db); n != 1 {
		t.Fatalf("a failed retry must leave the dead job in place; dead jobs = %d", n)
	}
}

func jobIDs(jobs []*skipq.Job) []int64 {
	ids := make([]int64, len(jobs))
	for i, j := range jobs {
		ids[i] = j.ID
	}
	return ids
}

func TestListDeadAcrossQueues(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	a := killJob(t, c, skipq.EnqueueParams{Kind: "k"})
	b := killJob(t, c, skipq.EnqueueParams{Kind: "k", Queue: "other"})

	all, err := c.ListDead(context.Background(), skipq.ListDeadParams{})
	if err != nil || len(all) != 2 || all[0].ID != b.ID || all[1].ID != a.ID {
		t.Fatalf("all queues: %v %v", jobIDs(all), err)
	}
}
