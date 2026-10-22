// Command skipq runs the skipq HTTP API and Prometheus metrics endpoint.
//
// Configuration comes from environment variables:
//
//	DATABASE_URL     Postgres connection string (default: the docker compose database)
//	SKIPQ_ADDR       listen address (default 127.0.0.1:8080)
//	SKIPQ_API_TOKEN  if set, /v1/ requests need "Authorization: Bearer <token>"
//	SKIPQ_MIGRATE    run migrations at startup (default true)
//
// Workers are not part of this binary: handlers are Go code, so they run in
// your own programs using skipq.Pool.
package main

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/MilindDevX/skipq"
	"github.com/MilindDevX/skipq/metrics"
	"github.com/MilindDevX/skipq/server"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("skipq: exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := cmp.Or(os.Getenv("DATABASE_URL"), "postgres://skipq:skipq@localhost:54329/skipq?sslmode=disable")
	addr := cmp.Or(os.Getenv("SKIPQ_ADDR"), "127.0.0.1:8080")
	token := os.Getenv("SKIPQ_API_TOKEN")
	migrate, err := strconv.ParseBool(cmp.Or(os.Getenv("SKIPQ_MIGRATE"), "true"))
	if err != nil {
		return errors.New("SKIPQ_MIGRATE must be true or false")
	}

	db, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return err // pgx errors do not include the password
	}
	defer db.Close()
	if migrate {
		if err := skipq.Migrate(ctx, db); err != nil {
			return err
		}
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		metrics.NewQueueCollector(db),
	)

	if token == "" && !isLoopback(addr) {
		log.Warn("skipq: SKIPQ_API_TOKEN is empty and the API listens beyond loopback; anyone who can reach this port can enqueue jobs", "addr", addr)
	}

	srv := &http.Server{
		Addr: addr,
		Handler: server.New(server.Config{
			DB:       db,
			Metrics:  metrics.New(reg),
			Gatherer: reg,
			APIToken: token,
			Logger:   log,
		}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Info("skipq: listening", "addr", addr, "auth", token != "")
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}
	log.Info("skipq: shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// isLoopback reports whether addr only accepts connections from this machine.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
