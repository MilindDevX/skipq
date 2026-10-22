package skipq

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Every statement below that changes a running job matches on
// (id, state = 'running', attempt). attempt is incremented on each claim, so
// it works as a fencing token: once a job has been reclaimed, the previous
// holder's attempt number no longer matches and its writes affect 0 rows,
// which we report as ErrLeaseLost.

// ClaimParams controls which jobs Claim takes and for how long.
type ClaimParams struct {
	Queue    string        // defaults to DefaultQueue
	Limit    int           // maximum jobs to claim; defaults to 1
	WorkerID string        // stored in locked_by for debugging
	Lease    time.Duration // how long the claimer owns the job before it can be rescued
}

// Claim takes up to Limit available jobs whose run_at has passed, highest
// priority first, and marks them running under a lease. Concurrent callers
// never receive the same job: rows locked by one claimer are skipped by the
// others (FOR UPDATE SKIP LOCKED). It returns an empty slice when no job is ready.
func (c *Client) Claim(ctx context.Context, p ClaimParams) ([]*Job, error) {
	if p.Lease <= 0 {
		return nil, errors.New("skipq: ClaimParams.Lease must be positive")
	}
	rows, err := c.db.Query(ctx, `
		WITH next AS (
			SELECT id FROM skipq_jobs
			WHERE state = 'available' AND queue = $1 AND run_at <= now()
			ORDER BY priority DESC, run_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE skipq_jobs j
		SET state = 'running',
		    attempt = j.attempt + 1,
		    locked_by = $3,
		    lease_expires_at = now() + $4 * interval '1 microsecond',
		    updated_at = now()
		FROM next
		WHERE j.id = next.id
		RETURNING `+qualified("j", jobColumns),
		cmp.Or(p.Queue, DefaultQueue), cmp.Or(p.Limit, 1), nullable(p.WorkerID), p.Lease.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("skipq: claim: %w", err)
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("skipq: scan claimed job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("skipq: claim: %w", err)
	}
	return jobs, nil
}

// Ack records that the job succeeded. It returns ErrLeaseLost if the job has
// been reclaimed since this worker claimed it. An expired lease that nobody
// has rescued yet is still accepted, because no one else holds the job.
func (c *Client) Ack(ctx context.Context, job *Job) error {
	tag, err := c.db.Exec(ctx, `
		UPDATE skipq_jobs
		SET state = 'succeeded', lease_expires_at = NULL, finished_at = now(), updated_at = now()
		WHERE id = $1 AND state = 'running' AND attempt = $2`,
		job.ID, job.Attempt)
	if err != nil {
		return fmt.Errorf("skipq: ack job %d: %w", job.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// ExtendLease pushes a running job's lease to now + lease and returns the new
// expiry. Pool calls it periodically (a heartbeat) so long jobs can use short
// leases. It returns ErrLeaseLost if the job was reclaimed.
func (c *Client) ExtendLease(ctx context.Context, job *Job, lease time.Duration) (time.Time, error) {
	var expires time.Time
	err := c.db.QueryRow(ctx, `
		UPDATE skipq_jobs
		SET lease_expires_at = now() + $3 * interval '1 microsecond'
		WHERE id = $1 AND state = 'running' AND attempt = $2
		RETURNING lease_expires_at`,
		job.ID, job.Attempt, lease.Microseconds()).Scan(&expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, ErrLeaseLost
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("skipq: extend lease of job %d: %w", job.ID, err)
	}
	return expires, nil
}

// Fail records a failed attempt. If attempts remain and jobErr is not
// Permanent, the job becomes available again after retryIn and Fail returns
// StateAvailable. Otherwise the job moves to skipq_dead_jobs and Fail returns
// StateDead. It returns ErrLeaseLost if the job was reclaimed.
func (c *Client) Fail(ctx context.Context, job *Job, jobErr error, retryIn time.Duration) (JobState, error) {
	msg := "unknown error"
	if jobErr != nil {
		msg = jobErr.Error()
	}

	// The two statements have mutually exclusive conditions, so at most one
	// of them matches; each is atomic on its own.
	tag, err := c.db.Exec(ctx, `
		WITH moved AS (
			DELETE FROM skipq_jobs
			WHERE id = $1 AND state = 'running' AND attempt = $2
			  AND (attempt >= max_attempts OR $3)
			RETURNING id, queue, kind, payload, priority, attempt, max_attempts, idempotency_key, created_at
		)
		INSERT INTO skipq_dead_jobs (id, queue, kind, payload, priority, attempt, max_attempts,
		                             last_error, idempotency_key, created_at)
		SELECT id, queue, kind, payload, priority, attempt, max_attempts, $4, idempotency_key, created_at
		FROM moved`,
		job.ID, job.Attempt, IsPermanent(jobErr), msg)
	if err != nil {
		return "", fmt.Errorf("skipq: dead-letter job %d: %w", job.ID, err)
	}
	if tag.RowsAffected() == 1 {
		return StateDead, nil
	}

	tag, err = c.db.Exec(ctx, `
		UPDATE skipq_jobs
		SET state = 'available',
		    run_at = now() + $3 * interval '1 microsecond',
		    last_error = $4,
		    locked_by = NULL,
		    lease_expires_at = NULL,
		    updated_at = now()
		WHERE id = $1 AND state = 'running' AND attempt = $2 AND attempt < max_attempts`,
		job.ID, job.Attempt, max(retryIn, 0).Microseconds(), msg)
	if err != nil {
		return "", fmt.Errorf("skipq: retry job %d: %w", job.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return "", ErrLeaseLost
	}
	return StateAvailable, nil
}

// Release hands a running job back without recording a failure, so another
// worker can claim it straight away. The attempt it used still counts toward
// MaxAttempts. Pool uses this when graceful shutdown runs out of time.
func (c *Client) Release(ctx context.Context, job *Job) error {
	tag, err := c.db.Exec(ctx, `
		UPDATE skipq_jobs
		SET state = 'available', run_at = now(), locked_by = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND state = 'running' AND attempt = $2`,
		job.ID, job.Attempt)
	if err != nil {
		return fmt.Errorf("skipq: release job %d: %w", job.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// RescueResult reports what one Rescue call did.
type RescueResult struct {
	Retried int // expired jobs put back to available
	Dead    int // expired jobs that had no attempts left
}

// rescueBatch bounds how many rows one Rescue statement touches, so a big
// backlog of expired leases is handled over several calls instead of one
// long-running statement.
const rescueBatch = 1000

// Rescue finds running jobs whose lease has expired (their worker crashed or
// the job overran its lease) and makes them available again, or dead-letters
// them if they have used all their attempts. Safe to call concurrently from
// many processes.
func (c *Client) Rescue(ctx context.Context) (RescueResult, error) {
	var res RescueResult

	tag, err := c.db.Exec(ctx, `
		WITH moved AS (
			DELETE FROM skipq_jobs
			WHERE id IN (
				SELECT id FROM skipq_jobs
				WHERE state = 'running' AND lease_expires_at < now() AND attempt >= max_attempts
				LIMIT $1
				FOR UPDATE SKIP LOCKED
			)
			RETURNING id, queue, kind, payload, priority, attempt, max_attempts, idempotency_key, created_at
		)
		INSERT INTO skipq_dead_jobs (id, queue, kind, payload, priority, attempt, max_attempts,
		                             last_error, idempotency_key, created_at)
		SELECT id, queue, kind, payload, priority, attempt, max_attempts,
		       'lease expired on final attempt (worker crashed or job exceeded its lease)',
		       idempotency_key, created_at
		FROM moved`, rescueBatch)
	if err != nil {
		return res, fmt.Errorf("skipq: rescue dead-letter: %w", err)
	}
	res.Dead = int(tag.RowsAffected())

	tag, err = c.db.Exec(ctx, `
		UPDATE skipq_jobs
		SET state = 'available',
		    run_at = now(),
		    last_error = 'lease expired (worker crashed or job exceeded its lease)',
		    locked_by = NULL,
		    lease_expires_at = NULL,
		    updated_at = now()
		WHERE id IN (
			SELECT id FROM skipq_jobs
			WHERE state = 'running' AND lease_expires_at < now() AND attempt < max_attempts
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)`, rescueBatch)
	if err != nil {
		return res, fmt.Errorf("skipq: rescue retry: %w", err)
	}
	res.Retried = int(tag.RowsAffected())
	return res, nil
}

// permanentError marks an error that retrying cannot fix.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps err so that Fail (and Pool) dead-letter the job at once
// instead of retrying it. Use it for errors such as an invalid payload.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err, or any error it wraps, came from Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
