// Package skipq is a durable background-job queue backed by Postgres.
//
// Jobs are rows in the skipq_jobs table. Workers claim them with
// SELECT ... FOR UPDATE SKIP LOCKED, so many workers can take different jobs
// at once without blocking each other. Delivery is at-least-once: a job runs
// until it succeeds or runs out of attempts, and may occasionally run twice,
// so handlers should be idempotent. See docs/design.md for the full contract.
package skipq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// JobState is where a job is in its life. See docs/design.md section 3.
type JobState string

const (
	StateAvailable JobState = "available" // waiting to be claimed (possibly scheduled for later)
	StateRunning   JobState = "running"   // claimed by a worker that holds a lease
	StateSucceeded JobState = "succeeded" // handler returned nil and the ack committed
	StateDead      JobState = "dead"      // out of attempts; lives in skipq_dead_jobs
)

var (
	// ErrNotFound means no job with that ID exists in skipq_jobs or skipq_dead_jobs.
	ErrNotFound = errors.New("skipq: job not found")
	// ErrLeaseLost means the job was reclaimed by another worker (or released)
	// after this worker claimed it, so this worker may no longer record a result.
	ErrLeaseLost = errors.New("skipq: lease lost; job was reclaimed")
)

// Job is one unit of work.
type Job struct {
	ID          int64
	Queue       string
	Kind        string
	Payload     json.RawMessage
	State       JobState
	Priority    int
	Attempt     int // number of times the job has been claimed; also the fencing token
	MaxAttempts int
	RunAt       time.Time
	// LeaseExpiresAt is set while the job is running.
	LeaseExpiresAt *time.Time
	LockedBy       string
	LastError      string
	IdempotencyKey string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	FinishedAt     *time.Time
}

// Client enqueues, inspects and transitions jobs. It is safe for concurrent use.
type Client struct {
	db *pgxpool.Pool
}

// New returns a Client that uses db. Call Migrate once before using it.
func New(db *pgxpool.Pool) *Client {
	return &Client{db: db}
}

// querier is the part of pgx that both *pgxpool.Pool and pgx.Tx provide,
// so the same SQL code can run inside or outside a caller's transaction.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

const jobColumns = `id, queue, kind, payload, state, priority, attempt, max_attempts, run_at,
	lease_expires_at, locked_by, last_error, idempotency_key, created_at, updated_at, finished_at`

// qualified prefixes each column in a comma-separated list with a table
// alias, for queries where a bare "id" would be ambiguous.
func qualified(alias, columns string) string {
	cols := strings.Split(columns, ",")
	for i, col := range cols {
		cols[i] = alias + "." + strings.TrimSpace(col)
	}
	return strings.Join(cols, ", ")
}

func scanJob(row pgx.Row) (*Job, error) {
	var (
		j                          Job
		state                      string
		lockedBy, lastErr, idemKey *string
	)
	err := row.Scan(&j.ID, &j.Queue, &j.Kind, &j.Payload, &state, &j.Priority, &j.Attempt,
		&j.MaxAttempts, &j.RunAt, &j.LeaseExpiresAt, &lockedBy, &lastErr, &idemKey,
		&j.CreatedAt, &j.UpdatedAt, &j.FinishedAt)
	if err != nil {
		return nil, err
	}
	j.State = JobState(state)
	j.LockedBy = deref(lockedBy)
	j.LastError = deref(lastErr)
	j.IdempotencyKey = deref(idemKey)
	return &j, nil
}

// Get returns the job with the given ID, including dead-lettered jobs
// (State == StateDead). It returns ErrNotFound if there is no such job.
func (c *Client) Get(ctx context.Context, id int64) (*Job, error) {
	job, err := scanJob(c.db.QueryRow(ctx, `SELECT `+jobColumns+` FROM skipq_jobs WHERE id = $1`, id))
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("skipq: get job %d: %w", id, err)
	}

	job, err = scanDeadJob(c.db.QueryRow(ctx,
		`SELECT `+deadJobColumns+` FROM skipq_dead_jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("skipq: get dead job %d: %w", id, err)
	}
	return job, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// nullable turns "" into SQL NULL.
func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
