package skipq_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/MilindDevX/skipq"
)

func TestMigrateTwiceIsANoOp(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	if err := skipq.Migrate(context.Background(), db); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

func TestEnqueueAppliesDefaults(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))

	job := mustEnqueue(t, c, skipq.EnqueueParams{
		Kind:    "email.send",
		Payload: map[string]string{"to": "a@example.com"},
	})

	if job.ID == 0 {
		t.Fatal("job ID not set")
	}
	if job.Queue != "default" || job.State != skipq.StateAvailable || job.Attempt != 0 || job.MaxAttempts != 25 {
		t.Fatalf("unexpected defaults: %+v", job)
	}
	var payload map[string]string
	if err := json.Unmarshal(job.Payload, &payload); err != nil || payload["to"] != "a@example.com" {
		t.Fatalf("payload round-trip: %s (%v)", job.Payload, err)
	}
	if time.Since(job.RunAt) > time.Minute {
		t.Fatalf("run_at should default to now, got %v", job.RunAt)
	}

	got := mustGet(t, c, job.ID)
	if got.Kind != "email.send" || got.State != skipq.StateAvailable {
		t.Fatalf("Get returned %+v", got)
	}
}

func TestEnqueueRejectsMissingKind(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	if _, err := c.Enqueue(context.Background(), skipq.EnqueueParams{}); err == nil {
		t.Fatal("expected an error for an empty kind")
	}
}

func TestEnqueueWithSameIdempotencyKeyReturnsExistingJob(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	ctx := context.Background()

	first, err := c.Enqueue(ctx, skipq.EnqueueParams{Kind: "charge", IdempotencyKey: "order-42"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Enqueue(ctx, skipq.EnqueueParams{Kind: "charge", IdempotencyKey: "order-42"})
	if err != nil {
		t.Fatal(err)
	}

	if first.Duplicate || !second.Duplicate {
		t.Fatalf("duplicate flags: first=%v second=%v", first.Duplicate, second.Duplicate)
	}
	if first.Job.ID != second.Job.ID {
		t.Fatalf("expected same job, got %d and %d", first.Job.ID, second.Job.ID)
	}
}

func TestConcurrentEnqueuesWithSameKeyCreateOneJob(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	c := skipq.New(db)

	const n = 20
	ids := make([]int64, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := c.Enqueue(context.Background(), skipq.EnqueueParams{Kind: "charge", IdempotencyKey: "race"})
			if err != nil {
				t.Errorf("enqueue %d: %v", i, err)
				return
			}
			ids[i] = res.Job.ID
		}()
	}
	wg.Wait()

	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("got different job IDs: %v", ids)
		}
	}
	var count int
	if err := db.QueryRow(context.Background(), "SELECT count(*) FROM skipq_jobs").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}
}

func TestEnqueueTxRollsBackWithCallerTransaction(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	c := skipq.New(db)
	ctx := context.Background()

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.EnqueueTx(ctx, tx, skipq.EnqueueParams{Kind: "welcome"})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Get(ctx, res.Job.ID); !errors.Is(err, skipq.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after rollback, got %v", err)
	}
}

func TestGetUnknownJobReturnsErrNotFound(t *testing.T) {
	t.Parallel()
	c := skipq.New(newTestDB(t))
	if _, err := c.Get(context.Background(), 999999); !errors.Is(err, skipq.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
