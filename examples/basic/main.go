// Command basic enqueues a batch of jobs, works them with a pool, and prints
// what happened. Every fifth job fails on its first attempt to show retries.
// Run it twice: the second run finds every job by its idempotency key and
// enqueues nothing new.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MilindDevX/skipq"
)

type resizePayload struct {
	Image int `json:"image"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// ctx is cancelled on Ctrl-C, which starts the pool's graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	url := cmp.Or(os.Getenv("DATABASE_URL"), "postgres://skipq:skipq@localhost:54329/skipq?sslmode=disable")
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := skipq.Migrate(ctx, db); err != nil {
		return err
	}
	client := skipq.New(db)

	const n = 20
	ids := make([]int64, 0, n)
	var duplicates int
	for i := 1; i <= n; i++ {
		res, err := client.Enqueue(ctx, skipq.EnqueueParams{
			Kind:           "image.resize",
			Payload:        resizePayload{Image: i},
			Priority:       i % 3,
			IdempotencyKey: fmt.Sprintf("example-resize-%d", i),
		})
		if err != nil {
			return err
		}
		if res.Duplicate {
			duplicates++
		}
		ids = append(ids, res.Job.ID)
	}
	fmt.Printf("enqueued %d jobs (%d already existed)\n", n-duplicates, duplicates)

	pool, err := client.NewPool(skipq.PoolConfig{
		Workers: 4,
		Backoff: func(int) time.Duration { return 200 * time.Millisecond }, // short, for the demo
		Logger:  slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		Handlers: map[string]skipq.Handler{
			"image.resize": func(ctx context.Context, job *skipq.Job) error {
				var p resizePayload
				if err := json.Unmarshal(job.Payload, &p); err != nil {
					return skipq.Permanent(fmt.Errorf("bad payload: %w", err)) // retrying won't fix it
				}
				if p.Image%5 == 0 && job.Attempt == 1 {
					fmt.Printf("  image %2d: attempt %d failed, will retry\n", p.Image, job.Attempt)
					return errors.New("thumbnail service timed out")
				}
				time.Sleep(50 * time.Millisecond) // pretend to work
				fmt.Printf("  image %2d: done on attempt %d\n", p.Image, job.Attempt)
				return nil
			},
		},
	})
	if err != nil {
		return err
	}

	poolCtx, stopPool := context.WithCancel(ctx)
	poolDone := make(chan error, 1)
	go func() { poolDone <- pool.Run(poolCtx) }()

	// Wait until every job has finished one way or the other.
	for _, id := range ids {
		for {
			job, err := client.Get(ctx, id)
			if err != nil {
				stopPool()
				return err
			}
			if job.State == skipq.StateSucceeded || job.State == skipq.StateDead {
				break
			}
			select {
			case <-ctx.Done():
				fmt.Println("interrupted; shutting down")
				stopPool()
				return <-poolDone
			case <-time.After(50 * time.Millisecond):
			}
		}
	}

	stopPool()
	if err := <-poolDone; err != nil {
		return err
	}
	fmt.Println("all jobs finished")
	return nil
}
