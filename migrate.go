package skipq

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// migrationLockKey is an arbitrary constant that names skipq's advisory lock.
const migrationLockKey int64 = 0x736b697071 // "skipq" in ASCII

// Migrate creates or upgrades skipq's tables in the first schema on the
// connection's search_path. It is safe to call from many processes at once:
// all pending migrations run in one transaction that holds an advisory lock,
// so only one caller applies them and the rest see them as done.
func Migrate(ctx context.Context, db *pgxpool.Pool) error {
	names, err := fs.Glob(migrationFiles, "migrations/*.sql") // sorted by name
	if err != nil {
		return fmt.Errorf("skipq: list migrations: %w", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("skipq: begin migration: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after Commit

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("skipq: take migration lock: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS skipq_migrations (
		version    text PRIMARY KEY,
		applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("skipq: create migrations table: %w", err)
	}

	for _, name := range names {
		var applied bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM skipq_migrations WHERE version = $1)`, name).Scan(&applied); err != nil {
			return fmt.Errorf("skipq: check migration %s: %w", name, err)
		}
		if applied {
			continue
		}
		sql, err := migrationFiles.ReadFile(name)
		if err != nil {
			return fmt.Errorf("skipq: read migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("skipq: apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO skipq_migrations (version) VALUES ($1)`, name); err != nil {
			return fmt.Errorf("skipq: record migration %s: %w", name, err)
		}
	}
	return tx.Commit(ctx)
}
