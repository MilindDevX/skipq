package skipq

import "time"

// Observer receives events from a Pool, for example to export metrics (see
// the metrics package). Methods are called from worker goroutines, so they
// must be safe for concurrent use and should return quickly.
type Observer interface {
	JobStarted(JobEvent)
	JobFinished(JobEvent)
}

// Outcome is what happened to a job after its handler returned.
type Outcome string

const (
	OutcomeSucceeded Outcome = "succeeded"  // acked
	OutcomeRetried   Outcome = "retried"    // failed, scheduled to run again
	OutcomeDead      Outcome = "dead"       // failed, moved to skipq_dead_jobs
	OutcomeReleased  Outcome = "released"   // interrupted by shutdown, handed back
	OutcomeLeaseLost Outcome = "lease_lost" // another worker owns the job now; result not recorded
	// OutcomeRecordFailed means the database write for the result failed
	// (for example, the database was down). The job runs again after its lease.
	OutcomeRecordFailed Outcome = "record_failed"
)

// UnknownKind is the Kind reported for jobs whose kind has no handler. Using
// one shared value keeps metric labels bounded even if callers enqueue
// arbitrary kinds.
const UnknownKind = "_unknown"

// JobEvent describes one run of a job.
type JobEvent struct {
	Queue   string
	Kind    string // UnknownKind when no handler is registered for the job's kind
	JobID   int64
	Attempt int
	// Wait is how long the job waited between becoming runnable (its run_at)
	// and being claimed, measured on the database clock.
	Wait time.Duration
	// The fields below are set only in JobFinished.
	Duration time.Duration // how long the handler ran
	Outcome  Outcome
	Err      error // the handler's error, if any
}

type nopObserver struct{}

func (nopObserver) JobStarted(JobEvent)  {}
func (nopObserver) JobFinished(JobEvent) {}
