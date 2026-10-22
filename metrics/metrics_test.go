package metrics_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/MilindDevX/skipq"
	"github.com/MilindDevX/skipq/internal/testdb"
	"github.com/MilindDevX/skipq/metrics"
)

func family(t *testing.T, reg *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return f
		}
	}
	t.Fatalf("metric %s not found", name)
	return nil
}

func TestObserverCountsRunsAndTimings(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	start := skipq.JobEvent{Queue: "default", Kind: "email", Attempt: 1, Wait: 2 * time.Second}
	m.JobStarted(start)
	ok := start
	ok.Outcome, ok.Duration = skipq.OutcomeSucceeded, 30*time.Millisecond
	m.JobFinished(ok)
	failed := start
	failed.Outcome, failed.Duration, failed.Err = skipq.OutcomeRetried, time.Second, errors.New("x")
	m.JobFinished(failed)
	m.Enqueued("default", false)
	m.Enqueued("default", true)

	want := `
# HELP skipq_job_runs_total Job runs finished by this process, by outcome.
# TYPE skipq_job_runs_total counter
skipq_job_runs_total{kind="email",outcome="retried",queue="default"} 1
skipq_job_runs_total{kind="email",outcome="succeeded",queue="default"} 1
# HELP skipq_jobs_enqueued_total Enqueue calls handled by this process; duplicate="true" means an existing job was returned for the idempotency key.
# TYPE skipq_jobs_enqueued_total counter
skipq_jobs_enqueued_total{duplicate="false",queue="default"} 1
skipq_jobs_enqueued_total{duplicate="true",queue="default"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want),
		"skipq_job_runs_total", "skipq_jobs_enqueued_total"); err != nil {
		t.Fatal(err)
	}

	wait := family(t, reg, "skipq_job_wait_seconds").GetMetric()[0].GetHistogram()
	if wait.GetSampleCount() != 1 || wait.GetSampleSum() != 2 {
		t.Fatalf("wait histogram: count=%d sum=%v", wait.GetSampleCount(), wait.GetSampleSum())
	}
	run := family(t, reg, "skipq_job_run_seconds").GetMetric()
	if len(run) != 1 || run[0].GetHistogram().GetSampleCount() != 2 {
		t.Fatalf("run histogram should have one series with 2 samples, got %v", run)
	}
}

func TestQueueCollectorReadsStateFromDatabase(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	c := skipq.New(db)
	ctx := context.Background()
	enqueue := func(p skipq.EnqueueParams) {
		t.Helper()
		if _, err := c.Enqueue(ctx, p); err != nil {
			t.Fatal(err)
		}
	}

	enqueue(skipq.EnqueueParams{Kind: "k", RunAt: time.Now().Add(-10 * time.Second)}) // oldest ready job
	enqueue(skipq.EnqueueParams{Kind: "k"})
	enqueue(skipq.EnqueueParams{Kind: "k"})
	if _, err := c.Claim(ctx, skipq.ClaimParams{WorkerID: "w", Lease: time.Minute}); err != nil { // one running
		t.Fatal(err)
	}
	enqueue(skipq.EnqueueParams{Kind: "k", RunAt: time.Now().Add(time.Hour)}) // scheduled
	enqueue(skipq.EnqueueParams{Kind: "k", Queue: "other"})
	jobs, err := c.Claim(ctx, skipq.ClaimParams{Queue: "other", WorkerID: "w", Lease: time.Minute})
	if err != nil || len(jobs) != 1 {
		t.Fatal(jobs, err)
	}
	if _, err := c.Fail(ctx, jobs[0], skipq.Permanent(errors.New("x")), 0); err != nil { // one dead
		t.Fatal(err)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.NewQueueCollector(db))

	want := `
# HELP skipq_jobs Jobs currently in the database, by queue and state.
# TYPE skipq_jobs gauge
skipq_jobs{queue="default",state="available"} 2
skipq_jobs{queue="default",state="dead"} 0
skipq_jobs{queue="default",state="running"} 1
skipq_jobs{queue="default",state="scheduled"} 1
skipq_jobs{queue="other",state="available"} 0
skipq_jobs{queue="other",state="dead"} 1
skipq_jobs{queue="other",state="running"} 0
skipq_jobs{queue="other",state="scheduled"} 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "skipq_jobs"); err != nil {
		t.Fatal(err)
	}

	// The claim took the oldest job, so the oldest ready one is a few ms old;
	// enqueue an older one to check lag is measured from run_at.
	enqueue(skipq.EnqueueParams{Kind: "k", RunAt: time.Now().Add(-time.Minute)})
	lag := family(t, reg, "skipq_queue_lag_seconds").GetMetric()
	if len(lag) != 1 || lag[0].GetGauge().GetValue() < 59 || lag[0].GetGauge().GetValue() > 120 {
		t.Fatalf("lag for default queue should be about 60s, got %v", lag)
	}
}

func TestQueueCollectorReportsDatabaseErrors(t *testing.T) {
	t.Parallel()
	db := testdb.New(t)
	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.NewQueueCollector(db))
	db.Close()

	if _, err := reg.Gather(); err == nil {
		t.Fatal("gather should fail when the database is unreachable")
	}
}
