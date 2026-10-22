// Package testdb gives integration tests their own migrated Postgres schema.
package testdb

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MilindDevX/skipq"
)

// DefaultURL matches docker-compose.yml. Override with SKIPQ_TEST_DATABASE_URL.
const DefaultURL = "postgres://skipq:skipq@localhost:54329/skipq?sslmode=disable"

// New creates a fresh schema, applies skipq's migrations to it, and returns a
// pool whose search_path points at it, so parallel tests never see each
// other's jobs. The schema is dropped when the test ends.
func New(t testing.TB) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	url := os.Getenv("SKIPQ_TEST_DATABASE_URL")
	if url == "" {
		url = DefaultURL
	}

	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	t.Cleanup(admin.Close)
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := admin.Ping(pingCtx); err != nil {
		t.Fatalf("cannot reach Postgres at %s: %v\nStart it with: docker compose up -d --wait", url, err)
	}

	schema := fmt.Sprintf("skipq_test_%d", rand.Uint64())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop schema: %v", err)
		}
	})

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 40
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(db.Close)

	if err := skipq.Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}
