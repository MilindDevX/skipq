package skipq_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MilindDevX/skipq"
	"github.com/MilindDevX/skipq/internal/testdb"
)

func newTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return testdb.New(t)
}

func mustEnqueue(t *testing.T, c *skipq.Client, p skipq.EnqueueParams) *skipq.Job {
	t.Helper()
	res, err := c.Enqueue(context.Background(), p)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return res.Job
}

func mustGet(t *testing.T, c *skipq.Client, id int64) *skipq.Job {
	t.Helper()
	job, err := c.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get job %d: %v", id, err)
	}
	return job
}
