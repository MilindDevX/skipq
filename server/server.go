// Package server is skipq's HTTP API: enqueue jobs, check their status, and
// list and retry dead jobs. cmd/skipq runs it as a standalone service.
package server

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/MilindDevX/skipq"
	"github.com/MilindDevX/skipq/metrics"
)

// Limits on caller-supplied values.
const (
	maxBodyBytes = 1 << 20 // 1 MiB per request, payload included
	maxNameLen   = 200     // queue, kind and idempotency key
	maxAttempts  = 1000
)

// Config configures the API.
type Config struct {
	DB *pgxpool.Pool // required
	// Metrics, if set, counts enqueue calls.
	Metrics *metrics.Metrics
	// Gatherer, if set, is served at GET /metrics.
	Gatherer prometheus.Gatherer
	// APIToken, if set, must be sent as "Authorization: Bearer <token>" on
	// every /v1/ request. /healthz and /metrics stay open.
	APIToken string
	Logger   *slog.Logger // defaults to slog.Default()
}

type api struct {
	cfg    Config
	client *skipq.Client
	log    *slog.Logger
}

// New returns the API's HTTP handler.
func New(cfg Config) http.Handler {
	a := &api{cfg: cfg, client: skipq.New(cfg.DB), log: cfg.Logger}
	if a.log == nil {
		a.log = slog.Default()
	}

	v1 := http.NewServeMux()
	v1.HandleFunc("POST /v1/jobs", a.enqueue)
	v1.HandleFunc("GET /v1/jobs/{id}", a.getJob)
	v1.HandleFunc("GET /v1/dead-jobs", a.listDead)
	v1.HandleFunc("POST /v1/dead-jobs/{id}/retry", a.retryDead)

	mux := http.NewServeMux()
	mux.Handle("/v1/", a.requireToken(v1))
	mux.HandleFunc("GET /healthz", a.health)
	if cfg.Gatherer != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(cfg.Gatherer, promhttp.HandlerOpts{}))
	}
	return mux
}

func (a *api) requireToken(next http.Handler) http.Handler {
	if a.cfg.APIToken == "" {
		return next
	}
	want := []byte("Bearer " + a.cfg.APIToken)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// ConstantTimeCompare takes the same time whatever the first wrong
		// byte is, so response timing does not reveal the token.
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="skipq"`)
			writeError(w, http.StatusUnauthorized, "missing or invalid API token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

type enqueueRequest struct {
	Queue          string          `json:"queue"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int64           `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          *time.Time      `json:"run_at"`
	IdempotencyKey string          `json:"idempotency_key"`
}

func (req *enqueueRequest) validate() error {
	switch {
	case req.Kind == "":
		return errors.New("kind is required")
	case len(req.Kind) > maxNameLen || len(req.Queue) > maxNameLen || len(req.IdempotencyKey) > maxNameLen:
		return fmt.Errorf("queue, kind and idempotency_key must be at most %d bytes", maxNameLen)
	case req.MaxAttempts < 0 || req.MaxAttempts > maxAttempts:
		return fmt.Errorf("max_attempts must be between 0 (default) and %d", maxAttempts)
	case req.Priority < math.MinInt32 || req.Priority > math.MaxInt32:
		return errors.New("priority must fit in a 32-bit integer")
	}
	return nil
}

// enqueue handles POST /v1/jobs. The idempotency key may come from the
// Idempotency-Key header or the body; if both are present they must match.
// A new job returns 201; a replay of an existing key returns 200 with
// Idempotent-Replayed: true.
func (a *api) enqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if err := decodeJSON(w, r, &req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body must be at most %d bytes", maxBodyBytes))
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		if req.IdempotencyKey != "" && req.IdempotencyKey != key {
			writeError(w, http.StatusBadRequest, "Idempotency-Key header and idempotency_key field differ")
			return
		}
		req.IdempotencyKey = key
	}
	if err := req.validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	params := skipq.EnqueueParams{
		Queue:          req.Queue,
		Kind:           req.Kind,
		Priority:       int(req.Priority),
		MaxAttempts:    req.MaxAttempts,
		IdempotencyKey: req.IdempotencyKey,
	}
	if len(req.Payload) > 0 {
		params.Payload = req.Payload
	}
	if req.RunAt != nil {
		params.RunAt = *req.RunAt
	}
	res, err := a.client.Enqueue(r.Context(), params)
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	if a.cfg.Metrics != nil {
		a.cfg.Metrics.Enqueued(res.Job.Queue, res.Duplicate)
	}

	if res.Duplicate {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, toJobJSON(res.Job))
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/v1/jobs/%d", res.Job.ID))
	writeJSON(w, http.StatusCreated, toJobJSON(res.Job))
}

func (a *api) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	job, err := a.client.Get(r.Context(), id)
	if errors.Is(err, skipq.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toJobJSON(job))
}

type deadPage struct {
	Jobs []jobJSON `json:"jobs"`
	// NextBeforeID is the before_id for the next page; absent on the last page.
	NextBeforeID int64 `json:"next_before_id,omitempty"`
}

// listDead handles GET /v1/dead-jobs?queue=&limit=&before_id=.
func (a *api) listDead(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	params := skipq.ListDeadParams{Queue: q.Get("queue"), Limit: 50}
	var err error
	if s := q.Get("limit"); s != "" {
		if params.Limit, err = strconv.Atoi(s); err != nil || params.Limit < 1 || params.Limit > 500 {
			writeError(w, http.StatusBadRequest, "limit must be an integer from 1 to 500")
			return
		}
	}
	if s := q.Get("before_id"); s != "" {
		if params.BeforeID, err = strconv.ParseInt(s, 10, 64); err != nil || params.BeforeID < 1 {
			writeError(w, http.StatusBadRequest, "before_id must be a positive integer")
			return
		}
	}

	jobs, err := a.client.ListDead(r.Context(), params)
	if err != nil {
		a.internalError(w, r, err)
		return
	}
	page := deadPage{Jobs: make([]jobJSON, len(jobs))}
	for i, job := range jobs {
		page.Jobs[i] = toJobJSON(job)
	}
	if len(jobs) == params.Limit { // a full page: there may be more
		page.NextBeforeID = jobs[len(jobs)-1].ID
	}
	writeJSON(w, http.StatusOK, page)
}

func (a *api) retryDead(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	job, err := a.client.RetryDead(r.Context(), id)
	switch {
	case errors.Is(err, skipq.ErrNotFound):
		writeError(w, http.StatusNotFound, "dead job not found")
	case errors.Is(err, skipq.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "another job now uses this job's idempotency key")
	case err != nil:
		a.internalError(w, r, err)
	default:
		writeJSON(w, http.StatusOK, toJobJSON(job))
	}
}

func (a *api) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.cfg.DB.Ping(ctx); err != nil {
		a.log.Warn("skipq: health check failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "database unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// jobJSON is the API's view of a job. locked_by is left out on purpose: it
// holds host names and process IDs that API callers do not need.
type jobJSON struct {
	ID             int64           `json:"id"`
	Queue          string          `json:"queue"`
	Kind           string          `json:"kind"`
	State          skipq.JobState  `json:"state"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          *time.Time      `json:"run_at,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

func toJobJSON(j *skipq.Job) jobJSON {
	out := jobJSON{
		ID: j.ID, Queue: j.Queue, Kind: j.Kind, State: j.State, Payload: j.Payload,
		Priority: j.Priority, Attempt: j.Attempt, MaxAttempts: j.MaxAttempts,
		LeaseExpiresAt: j.LeaseExpiresAt, LastError: j.LastError, IdempotencyKey: j.IdempotencyKey,
		CreatedAt: j.CreatedAt, UpdatedAt: j.UpdatedAt, FinishedAt: j.FinishedAt,
	}
	if !j.RunAt.IsZero() { // dead jobs have no run_at
		out.RunAt = &j.RunAt
	}
	return out
}

// decodeJSON reads exactly one JSON value, rejecting unknown fields and
// bodies over maxBodyBytes.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("body must contain a single JSON object")
	}
	return nil
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeError(w, http.StatusBadRequest, "job id must be a positive integer")
		return 0, false
	}
	return id, true
}

// internalError logs the real error and returns a generic message, so
// database details never reach API callers.
func (a *api) internalError(w http.ResponseWriter, r *http.Request, err error) {
	a.log.Error("skipq: request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}
