package skipq

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrIdempotencyConflict means a dead job cannot be retried because a newer
// job already uses its idempotency key.
var ErrIdempotencyConflict = errors.New("skipq: idempotency key is already used by another job")

const deadJobColumns = `id, queue, kind, payload, priority, attempt, max_attempts, last_error,
	idempotency_key, created_at, dead_at`

func scanDeadJob(row pgx.Row) (*Job, error) {
	var (
		j                Job
		lastErr, idemKey *string
		deadAt           time.Time
	)
	if err := row.Scan(&j.ID, &j.Queue, &j.Kind, &j.Payload, &j.Priority, &j.Attempt, &j.MaxAttempts,
		&lastErr, &idemKey, &j.CreatedAt, &deadAt); err != nil {
		return nil, err
	}
	j.State = StateDead
	j.LastError = deref(lastErr)
	j.IdempotencyKey = deref(idemKey)
	j.UpdatedAt = deadAt
	j.FinishedAt = &deadAt
	return &j, nil
}

// ListDeadParams selects a page of dead jobs.
type ListDeadParams struct {
	Queue    string // only this queue; empty means every queue
	Limit    int    // page size; defaults to 50, capped at 500
	BeforeID int64  // return jobs with ID < BeforeID; 0 starts at the newest
}

// ListDead returns dead jobs newest first. To page, pass the last ID of one
// page as BeforeID of the next (keyset pagination: no OFFSET scan, and new
// dead jobs do not shift the pages).
func (c *Client) ListDead(ctx context.Context, p ListDeadParams) ([]*Job, error) {
	limit := min(cmp.Or(p.Limit, 50), 500)
	if limit < 0 {
		return nil, errors.New("skipq: ListDeadParams.Limit must not be negative")
	}
	rows, err := c.db.Query(ctx, `
		SELECT `+deadJobColumns+` FROM skipq_dead_jobs
		WHERE ($1 = '' OR queue = $1) AND ($2 = 0 OR id < $2)
		ORDER BY id DESC
		LIMIT $3`, p.Queue, p.BeforeID, limit)
	if err != nil {
		return nil, fmt.Errorf("skipq: list dead jobs: %w", err)
	}
	defer rows.Close()

	jobs := []*Job{}
	for rows.Next() {
		job, err := scanDeadJob(rows)
		if err != nil {
			return nil, fmt.Errorf("skipq: scan dead job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("skipq: list dead jobs: %w", err)
	}
	return jobs, nil
}

// RetryDead moves a dead job back into skipq_jobs under the same ID, ready to
// run now with a fresh set of attempts. Its last error is kept for context.
// It returns ErrNotFound if there is no such dead job, and
// ErrIdempotencyConflict if its key has since been used by another job.
func (c *Client) RetryDead(ctx context.Context, id int64) (*Job, error) {
	// One statement, so the delete and insert commit or fail together: a
	// unique-key conflict on the insert also undoes the delete.
	job, err := scanJob(c.db.QueryRow(ctx, `
		WITH moved AS (
			DELETE FROM skipq_dead_jobs WHERE id = $1
			RETURNING id, queue, kind, payload, priority, max_attempts, last_error, idempotency_key, created_at
		)
		INSERT INTO skipq_jobs (id, queue, kind, payload, priority, max_attempts, last_error,
		                        idempotency_key, created_at, state, attempt, run_at)
		SELECT id, queue, kind, payload, priority, max_attempts, last_error,
		       idempotency_key, created_at, 'available', 0, now()
		FROM moved
		RETURNING `+jobColumns, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "skipq_jobs_idempotency_key_idx" {
		return nil, ErrIdempotencyConflict
	}
	if err != nil {
		return nil, fmt.Errorf("skipq: retry dead job %d: %w", id, err)
	}
	return job, nil
}
