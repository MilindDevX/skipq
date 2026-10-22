package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/MilindDevX/skipq"
	"github.com/MilindDevX/skipq/internal/testdb"
	"github.com/MilindDevX/skipq/metrics"
	"github.com/MilindDevX/skipq/server"
)

type harness struct {
	t      *testing.T
	url    string
	client *skipq.Client
	token  string
}

func newHarness(t *testing.T, token string) *harness {
	t.Helper()
	db := testdb.New(t)
	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.NewQueueCollector(db))
	srv := httptest.NewServer(server.New(server.Config{
		DB:       db,
		Metrics:  metrics.New(reg),
		Gatherer: reg,
		APIToken: token,
	}))
	t.Cleanup(srv.Close)
	return &harness{t: t, url: srv.URL, client: skipq.New(db), token: token}
}

type response struct {
	status int
	header http.Header
	body   string
}

func (h *harness) do(method, path, body string, headers ...string) response {
	h.t.Helper()
	req, err := http.NewRequest(method, h.url+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return response{status: resp.StatusCode, header: resp.Header, body: string(b)}
}

type jobBody struct {
	ID             int64           `json:"id"`
	Queue          string          `json:"queue"`
	Kind           string          `json:"kind"`
	State          string          `json:"state"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          *time.Time      `json:"run_at"`
	LastError      string          `json:"last_error"`
	IdempotencyKey string          `json:"idempotency_key"`
}

func decode[T any](t *testing.T, r response) T {
	t.Helper()
	var v T
	if err := json.Unmarshal([]byte(r.body), &v); err != nil {
		t.Fatalf("decode %q: %v", r.body, err)
	}
	return v
}

func wantStatus(t *testing.T, r response, status int) {
	t.Helper()
	if r.status != status {
		t.Fatalf("status = %d, want %d; body: %s", r.status, status, r.body)
	}
}

func TestEnqueueAndGetJob(t *testing.T) {
	t.Parallel()
	h := newHarness(t, "")

	r := h.do("POST", "/v1/jobs", `{"kind":"email.send","payload":{"to":"a@example.com"},"priority":5,"max_attempts":3}`)
	wantStatus(t, r, http.StatusCreated)
	created := decode[jobBody](t, r)
	var payload map[string]string
	if err := json.Unmarshal(created.Payload, &payload); err != nil || payload["to"] != "a@example.com" {
		t.Fatalf("payload %s (%v)", created.Payload, err)
	}
	if created.ID == 0 || created.State != "available" || created.Queue != "default" ||
		created.Priority != 5 || created.MaxAttempts != 3 {
		t.Fatalf("created job: %+v", created)
	}
	if loc := r.header.Get("Location"); loc != fmt.Sprintf("/v1/jobs/%d", created.ID) {
		t.Fatalf("Location = %q", loc)
	}

	r = h.do("GET", fmt.Sprintf("/v1/jobs/%d", created.ID), "")
	wantStatus(t, r, http.StatusOK)
	if got := decode[jobBody](t, r); got.ID != created.ID || got.Kind != "email.send" {
		t.Fatalf("get: %+v", got)
	}
}

func TestEnqueueScheduledJob(t *testing.T) {
	t.Parallel()
	h := newHarness(t, "")
	runAt := time.Now().Add(time.Hour).UTC().Truncate(time.Second)

	r := h.do("POST", "/v1/jobs", fmt.Sprintf(`{"kind":"k","run_at":%q}`, runAt.Format(time.RFC3339)))
	wantStatus(t, r, http.StatusCreated)
	if got := decode[jobBody](t, r); got.RunAt == nil || !got.RunAt.Equal(runAt) {
		t.Fatalf("run_at = %v, want %v", got.RunAt, runAt)
	}
}

func TestEnqueueIdempotencyKeyHeaderReturnsExistingJob(t *testing.T) {
	t.Parallel()
	h := newHarness(t, "")

	first := h.do("POST", "/v1/jobs", `{"kind":"charge"}`, "Idempotency-Key", "order-42")
	wantStatus(t, first, http.StatusCreated)
	second := h.do("POST", "/v1/jobs", `{"kind":"charge"}`, "Idempotency-Key", "order-42")
	wantStatus(t, second, http.StatusOK)

	if second.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("missing Idempotent-Replayed header: %v", second.header)
	}
	a, b := decode[jobBody](t, first), decode[jobBody](t, second)
	if a.ID != b.ID || b.IdempotencyKey != "order-42" {
		t.Fatalf("expected the same job: %+v vs %+v", a, b)
	}
}

func TestEnqueueRejectsBadRequests(t *testing.T) {
	t.Parallel()
	h := newHarness(t, "")
	cases := []struct {
		name    string
		body    string
		headers []string
		status  int
	}{
		{"missing kind", `{}`, nil, http.StatusBadRequest},
		{"not json", `kind=x`, nil, http.StatusBadRequest},
		{"unknown field", `{"kind":"k","prio":1}`, nil, http.StatusBadRequest},
		{"trailing data", `{"kind":"k"}{"kind":"k"}`, nil, http.StatusBadRequest},
		{"negative attempts", `{"kind":"k","max_attempts":-1}`, nil, http.StatusBadRequest},
		{"huge priority", `{"kind":"k","priority":99999999999}`, nil, http.StatusBadRequest},
		{"long kind", fmt.Sprintf(`{"kind":%q}`, strings.Repeat("k", 201)), nil, http.StatusBadRequest},
		{"key mismatch", `{"kind":"k","idempotency_key":"a"}`, []string{"Idempotency-Key", "b"}, http.StatusBadRequest},
		{"too large", fmt.Sprintf(`{"kind":"k","payload":{"x":%q}}`, strings.Repeat("x", 2<<20)), nil, http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		r := h.do("POST", "/v1/jobs", tc.body, tc.headers...)
		if r.status != tc.status {
			t.Errorf("%s: status = %d, want %d; body: %.200s", tc.name, r.status, tc.status, r.body)
			continue
		}
		if e := decode[map[string]string](t, r); e["error"] == "" {
			t.Errorf("%s: response has no error message: %s", tc.name, r.body)
		}
	}
}

func TestGetJobErrors(t *testing.T) {
	t.Parallel()
	h := newHarness(t, "")
	wantStatus(t, h.do("GET", "/v1/jobs/999999", ""), http.StatusNotFound)
	wantStatus(t, h.do("GET", "/v1/jobs/abc", ""), http.StatusBadRequest)
}

func TestDeadJobsListAndRetry(t *testing.T) {
	t.Parallel()
	h := newHarness(t, "")
	ctx := context.Background()
	var ids []int64
	for range 3 {
		res, err := h.client.Enqueue(ctx, skipq.EnqueueParams{Kind: "k"})
		if err != nil {
			t.Fatal(err)
		}
		jobs, err := h.client.Claim(ctx, skipq.ClaimParams{WorkerID: "w", Lease: time.Minute})
		if err != nil || len(jobs) != 1 {
			t.Fatal(jobs, err)
		}
		if _, err := h.client.Fail(ctx, jobs[0], skipq.Permanent(errors.New("bad input")), 0); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, res.Job.ID)
	}

	type page struct {
		Jobs         []jobBody `json:"jobs"`
		NextBeforeID int64     `json:"next_before_id"`
	}
	r := h.do("GET", "/v1/dead-jobs?limit=2", "")
	wantStatus(t, r, http.StatusOK)
	p1 := decode[page](t, r)
	if len(p1.Jobs) != 2 || p1.Jobs[0].ID != ids[2] || p1.Jobs[0].State != "dead" ||
		p1.Jobs[0].LastError != "bad input" || p1.NextBeforeID != ids[1] {
		t.Fatalf("page 1: %+v", p1)
	}
	r = h.do("GET", fmt.Sprintf("/v1/dead-jobs?limit=2&before_id=%d", p1.NextBeforeID), "")
	wantStatus(t, r, http.StatusOK)
	if p2 := decode[page](t, r); len(p2.Jobs) != 1 || p2.Jobs[0].ID != ids[0] || p2.NextBeforeID != 0 {
		t.Fatalf("page 2: %+v", p2)
	}
	wantStatus(t, h.do("GET", "/v1/dead-jobs?limit=lots", ""), http.StatusBadRequest)

	r = h.do("POST", fmt.Sprintf("/v1/dead-jobs/%d/retry", ids[0]), "")
	wantStatus(t, r, http.StatusOK)
	if got := decode[jobBody](t, r); got.ID != ids[0] || got.State != "available" || got.Attempt != 0 {
		t.Fatalf("retried: %+v", got)
	}
	wantStatus(t, h.do("POST", fmt.Sprintf("/v1/dead-jobs/%d/retry", ids[0]), ""), http.StatusNotFound)
}

func TestRetryDeadJobConflict(t *testing.T) {
	t.Parallel()
	h := newHarness(t, "")
	ctx := context.Background()
	res, err := h.client.Enqueue(ctx, skipq.EnqueueParams{Kind: "k", IdempotencyKey: "inv-1"})
	if err != nil {
		t.Fatal(err)
	}
	jobs, _ := h.client.Claim(ctx, skipq.ClaimParams{WorkerID: "w", Lease: time.Minute})
	if _, err := h.client.Fail(ctx, jobs[0], skipq.Permanent(errors.New("x")), 0); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, h.do("POST", "/v1/jobs", `{"kind":"k"}`, "Idempotency-Key", "inv-1"), http.StatusCreated)

	wantStatus(t, h.do("POST", fmt.Sprintf("/v1/dead-jobs/%d/retry", res.Job.ID), ""), http.StatusConflict)
}

func TestAPITokenProtectsV1Routes(t *testing.T) {
	t.Parallel()
	h := newHarness(t, "s3cret")

	wantStatus(t, h.do("POST", "/v1/jobs", `{"kind":"k"}`), http.StatusCreated)

	h.token = ""
	r := h.do("POST", "/v1/jobs", `{"kind":"k"}`)
	wantStatus(t, r, http.StatusUnauthorized)
	if r.header.Get("WWW-Authenticate") == "" {
		t.Fatal("401 should carry WWW-Authenticate")
	}
	wantStatus(t, h.do("GET", "/v1/jobs/1", "", "Authorization", "Bearer wrong"), http.StatusUnauthorized)

	// Health and metrics stay open for load balancers and Prometheus.
	wantStatus(t, h.do("GET", "/healthz", ""), http.StatusOK)
	wantStatus(t, h.do("GET", "/metrics", ""), http.StatusOK)
}

func TestHealthAndMetrics(t *testing.T) {
	t.Parallel()
	h := newHarness(t, "")
	wantStatus(t, h.do("POST", "/v1/jobs", `{"kind":"k"}`), http.StatusCreated)
	wantStatus(t, h.do("POST", "/v1/jobs", `{"kind":"k"}`, "Idempotency-Key", "x"), http.StatusCreated)
	wantStatus(t, h.do("POST", "/v1/jobs", `{"kind":"k"}`, "Idempotency-Key", "x"), http.StatusOK)

	wantStatus(t, h.do("GET", "/healthz", ""), http.StatusOK)

	r := h.do("GET", "/metrics", "")
	wantStatus(t, r, http.StatusOK)
	for _, want := range []string{
		`skipq_jobs{queue="default",state="available"} 2`,
		`skipq_jobs_enqueued_total{duplicate="false",queue="default"} 2`,
		`skipq_jobs_enqueued_total{duplicate="true",queue="default"} 1`,
	} {
		if !strings.Contains(r.body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
}
