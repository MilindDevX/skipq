package skipq

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	DefaultQueue       = "default"
	DefaultMaxAttempts = 25
)

// EnqueueParams describes a job to add. Only Kind is required.
type EnqueueParams struct {
	Queue string // defaults to DefaultQueue
	Kind  string // selects the handler, e.g. "email.send"
	// Payload is marshalled to JSON. Pass json.RawMessage for JSON you have
	// already encoded (a plain []byte would be encoded as a base64 string).
	Payload     any
	Priority    int       // higher runs first; default 0
	MaxAttempts int       // defaults to DefaultMaxAttempts
	RunAt       time.Time // zero means now; a future time delays the job
	// IdempotencyKey, if set, makes Enqueue return the existing job with this
	// key instead of inserting a duplicate.
	IdempotencyKey string
}

// EnqueueResult is what Enqueue returns. Duplicate is true when a job with the
// same IdempotencyKey already existed and Job is that existing job.
type EnqueueResult struct {
	Job       *Job
	Duplicate bool
}

// Enqueue durably adds a job. It returns after the row is committed.
func (c *Client) Enqueue(ctx context.Context, p EnqueueParams) (EnqueueResult, error) {
	return enqueue(ctx, c.db, p)
}

// EnqueueTx adds a job inside the caller's transaction, so the job exists only
// if tx commits. Use it to enqueue work about rows written in the same
// transaction (the transactional outbox pattern).
func (c *Client) EnqueueTx(ctx context.Context, tx pgx.Tx, p EnqueueParams) (EnqueueResult, error) {
	return enqueue(ctx, tx, p)
}

func enqueue(ctx context.Context, q querier, p EnqueueParams) (EnqueueResult, error) {
	if p.Kind == "" {
		return EnqueueResult{}, errors.New("skipq: EnqueueParams.Kind is required")
	}
	if p.MaxAttempts < 0 {
		return EnqueueResult{}, errors.New("skipq: EnqueueParams.MaxAttempts must not be negative")
	}
	payload := []byte("{}")
	if p.Payload != nil {
		var err error
		if payload, err = json.Marshal(p.Payload); err != nil {
			return EnqueueResult{}, fmt.Errorf("skipq: marshal payload: %w", err)
		}
	}
	var runAt *time.Time
	if !p.RunAt.IsZero() {
		runAt = &p.RunAt
	}
	key := nullable(p.IdempotencyKey)

	// With a key, the INSERT may hit an existing row. ON CONFLICT DO NOTHING
	// then returns no row and we read the existing one. If that row was
	// dead-lettered between the two statements (freeing the key), try again.
	for range 3 {
		job, err := scanJob(q.QueryRow(ctx, `
			INSERT INTO skipq_jobs (queue, kind, payload, priority, max_attempts, run_at, idempotency_key)
			VALUES ($1, $2, $3, $4, $5, COALESCE($6, now()), $7)
			ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
			RETURNING `+jobColumns,
			cmp.Or(p.Queue, DefaultQueue), p.Kind, payload, p.Priority,
			cmp.Or(p.MaxAttempts, DefaultMaxAttempts), runAt, key))
		if err == nil {
			return EnqueueResult{Job: job}, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return EnqueueResult{}, fmt.Errorf("skipq: insert job: %w", err)
		}

		job, err = scanJob(q.QueryRow(ctx,
			`SELECT `+jobColumns+` FROM skipq_jobs WHERE idempotency_key = $1`, key))
		if err == nil {
			return EnqueueResult{Job: job, Duplicate: true}, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return EnqueueResult{}, fmt.Errorf("skipq: read existing job: %w", err)
		}
	}
	return EnqueueResult{}, fmt.Errorf("skipq: idempotency key %q kept changing hands; try again", p.IdempotencyKey)
}
