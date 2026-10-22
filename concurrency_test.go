package skipq_test

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/MilindDevX/skipq"
)

// TestNoJobRunsTwiceWithManyWorkers is the core safety check for
// FOR UPDATE SKIP LOCKED: with many workers racing for the same queue and no
// crashes, every job must run exactly once.
func TestNoJobRunsTwiceWithManyWorkers(t *testing.T) {
	t.Parallel()
	const (
		jobs           = 2000
		pools          = 4
		workersPerPool = 8 // 32 workers in total
	)
	db := newTestDB(t)
	c := skipq.New(db)
	ctx := context.Background()

	for i := range jobs {
		mustEnqueue(t, c, skipq.EnqueueParams{Kind: "count", Priority: i % 3})
	}

	var (
		mu       sync.Mutex
		runs     = make(map[int64]int, jobs)
		finished = make(chan struct{})
	)
	handler := func(ctx context.Context, job *skipq.Job) error {
		time.Sleep(time.Duration(rand.IntN(500)) * time.Microsecond) // widen race windows
		mu.Lock()
		defer mu.Unlock()
		runs[job.ID]++
		if len(runs) == jobs {
			select {
			case <-finished:
			default:
				close(finished)
			}
		}
		return nil
	}

	for range pools {
		startPool(t, c, skipq.PoolConfig{
			Workers:      workersPerPool,
			PollInterval: 5 * time.Millisecond,
			Lease:        time.Minute, // far longer than any job, so no lease expires
			Handlers:     map[string]skipq.Handler{"count": handler},
		})
	}

	select {
	case <-finished:
	case <-time.After(60 * time.Second):
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("only %d of %d jobs ran within 60s", len(runs), jobs)
	}

	// Wait for the last acks to commit before checking the table.
	deadline := time.Now().Add(10 * time.Second)
	var succeeded, firstTry int
	for {
		err := db.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE state = 'succeeded'),
			       count(*) FILTER (WHERE state = 'succeeded' AND attempt = 1)
			FROM skipq_jobs`).Scan(&succeeded, &firstTry)
		if err != nil {
			t.Fatal(err)
		}
		if succeeded == jobs || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	for id, n := range runs {
		if n != 1 {
			t.Errorf("job %d ran %d times", id, n)
		}
	}
	if succeeded != jobs || firstTry != jobs {
		t.Fatalf("succeeded=%d first-attempt=%d, want %d of each", succeeded, firstTry, jobs)
	}
}
