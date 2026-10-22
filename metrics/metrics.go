// Package metrics exports skipq metrics to Prometheus.
//
// Two kinds of metrics come from two places:
//
//   - Metrics (an skipq.Observer) counts events in this process: job runs by
//     outcome, wait and run time histograms, enqueue calls. Pass it as
//     PoolConfig.Observer in worker processes.
//   - NewQueueCollector reads the current number of jobs per queue and state
//     from the database on every scrape, so it is correct across all
//     processes. Register it in one process (the skipq server does).
//
// Label values come from queue and kind names. Keep them to a small fixed set:
// every distinct value creates a new time series.
package metrics

import (
	"context"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/MilindDevX/skipq"
)

// latencyBuckets span 5ms to about 2.7 minutes, doubling each step.
var latencyBuckets = prometheus.ExponentialBuckets(0.005, 2, 16)

// Metrics records pool and enqueue events. It implements skipq.Observer.
type Metrics struct {
	runs     *prometheus.CounterVec
	wait     *prometheus.HistogramVec
	run      *prometheus.HistogramVec
	enqueued *prometheus.CounterVec
}

var _ skipq.Observer = (*Metrics)(nil)

// New creates the event metrics and registers them with reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "skipq_job_runs_total",
			Help: "Job runs finished by this process, by outcome.",
		}, []string{"queue", "kind", "outcome"}),
		wait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "skipq_job_wait_seconds",
			Help:    "Time from a job becoming runnable (run_at) to a worker claiming it.",
			Buckets: latencyBuckets,
		}, []string{"queue", "kind"}),
		run: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "skipq_job_run_seconds",
			Help:    "Time a handler took to run.",
			Buckets: latencyBuckets,
		}, []string{"queue", "kind"}),
		enqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "skipq_jobs_enqueued_total",
			Help: `Enqueue calls handled by this process; duplicate="true" means an existing job was returned for the idempotency key.`,
		}, []string{"queue", "duplicate"}),
	}
	reg.MustRegister(m.runs, m.wait, m.run, m.enqueued)
	return m
}

// JobStarted implements skipq.Observer.
func (m *Metrics) JobStarted(e skipq.JobEvent) {
	m.wait.WithLabelValues(e.Queue, e.Kind).Observe(e.Wait.Seconds())
}

// JobFinished implements skipq.Observer.
func (m *Metrics) JobFinished(e skipq.JobEvent) {
	m.runs.WithLabelValues(e.Queue, e.Kind, string(e.Outcome)).Inc()
	m.run.WithLabelValues(e.Queue, e.Kind).Observe(e.Duration.Seconds())
}

// Enqueued counts one enqueue call.
func (m *Metrics) Enqueued(queue string, duplicate bool) {
	m.enqueued.WithLabelValues(queue, strconv.FormatBool(duplicate)).Inc()
}

var (
	jobsDesc = prometheus.NewDesc("skipq_jobs",
		"Jobs currently in the database, by queue and state.", []string{"queue", "state"}, nil)
	lagDesc = prometheus.NewDesc("skipq_queue_lag_seconds",
		"Age of the oldest job that is ready to run but not yet claimed.", []string{"queue"}, nil)
)

// states lists every state the collector reports, so a queue that has jobs
// in one state shows explicit zeros for the others instead of gaps.
var states = []string{"available", "scheduled", "running", "dead"}

const scrapeTimeout = 5 * time.Second

type queueCollector struct {
	db *pgxpool.Pool
}

// NewQueueCollector returns a collector that queries job counts and queue lag
// on every scrape. "scheduled" means available but with run_at in the future.
func NewQueueCollector(db *pgxpool.Pool) prometheus.Collector {
	return &queueCollector{db: db}
}

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- jobsDesc
	ch <- lagDesc
}

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), scrapeTimeout)
	defer cancel()

	counts, err := c.counts(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(jobsDesc, err)
		return
	}
	for queue, byState := range counts {
		for _, state := range states {
			ch <- prometheus.MustNewConstMetric(jobsDesc, prometheus.GaugeValue, byState[state], queue, state)
		}
	}

	rows, err := c.db.Query(ctx, `
		SELECT queue, EXTRACT(EPOCH FROM now() - min(run_at))::float8
		FROM skipq_jobs
		WHERE state = 'available' AND run_at <= now()
		GROUP BY queue`)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(lagDesc, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var (
			queue string
			lag   float64
		)
		if err := rows.Scan(&queue, &lag); err != nil {
			ch <- prometheus.NewInvalidMetric(lagDesc, err)
			return
		}
		ch <- prometheus.MustNewConstMetric(lagDesc, prometheus.GaugeValue, lag, queue)
	}
	if err := rows.Err(); err != nil {
		ch <- prometheus.NewInvalidMetric(lagDesc, err)
	}
}

func (c *queueCollector) counts(ctx context.Context) (map[string]map[string]float64, error) {
	// Only available and running rows are counted in skipq_jobs; both states
	// have partial indexes, so succeeded history does not slow the scrape.
	rows, err := c.db.Query(ctx, `
		SELECT queue,
		       CASE WHEN state = 'available' AND run_at > now() THEN 'scheduled' ELSE state END,
		       count(*)
		FROM skipq_jobs
		WHERE state IN ('available', 'running')
		GROUP BY 1, 2
		UNION ALL
		SELECT queue, 'dead', count(*) FROM skipq_dead_jobs GROUP BY 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := map[string]map[string]float64{}
	for rows.Next() {
		var (
			queue, state string
			n            int64
		)
		if err := rows.Scan(&queue, &state, &n); err != nil {
			return nil, err
		}
		if counts[queue] == nil {
			counts[queue] = map[string]float64{}
		}
		counts[queue][state] += float64(n)
	}
	return counts, rows.Err()
}
