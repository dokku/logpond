package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"
)

// Migration is a single ordered schema change. Stmts are executed in
// sequence inside a transaction; on success the version is recorded in
// the migrations table.
type Migration struct {
	Version int
	Stmts   []string
}

// CoreMigrations returns the canonical catalog migration set. Tests may
// append additional Migrations with strictly greater versions to verify
// the migration runner.
func CoreMigrations() []Migration {
	return []Migration{
		{
			Version: 1,
			Stmts: []string{
				`CREATE TABLE segments (
    id              TEXT PRIMARY KEY,
    state           TEXT NOT NULL,
    time_start      TIMESTAMP NOT NULL,
    time_end        TIMESTAMP NOT NULL,
    row_count       INTEGER NOT NULL,
    size_bytes      INTEGER NOT NULL,
    size_compressed INTEGER,
    local_path      TEXT,
    s3_url          TEXT,
    archive_ref     TEXT,
    manifest_sha256 TEXT,
    parquet_sha256  TEXT,
    source_names    TEXT,
    created_at      TIMESTAMP NOT NULL,
    sealed_at       TIMESTAMP,
    archived_at     TIMESTAMP,
    rehydrated_at   TIMESTAMP,
    evict_after     TIMESTAMP
)`,
				`CREATE INDEX idx_segments_time ON segments(time_start, time_end)`,
				`CREATE INDEX idx_segments_state ON segments(state)`,
				`CREATE TABLE saved_queries (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    query_json TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
)`,
				`CREATE TABLE jobs (
    id            TEXT PRIMARY KEY,
    type          TEXT NOT NULL,
    state         TEXT NOT NULL,
    payload_json  TEXT NOT NULL,
    progress_json TEXT,
    script_stdout TEXT,
    script_stderr TEXT,
    started_at    TIMESTAMP,
    finished_at   TIMESTAMP,
    error_json    TEXT
)`,
				`CREATE INDEX idx_jobs_state ON jobs(state)`,
				`CREATE TABLE custom_facets (
    name              TEXT PRIMARY KEY,
    field             TEXT NOT NULL,
    display_label     TEXT,
    cardinality_cap   INTEGER,
    value_type        TEXT NOT NULL DEFAULT 'string',
    created_at        TIMESTAMP NOT NULL,
    updated_at        TIMESTAMP NOT NULL,
    source            TEXT NOT NULL
)`,
			},
		},
	}
}

func applyMigrations(ctx context.Context, db *sql.DB, migrations []Migration) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS migrations (
        version    INTEGER PRIMARY KEY,
        applied_at TIMESTAMP NOT NULL
    )`); err != nil {
		return fmt.Errorf("creating migrations table: %w", err)
	}
	applied, err := loadApplied(ctx, db)
	if err != nil {
		return err
	}
	sorted := make([]Migration, len(migrations))
	copy(sorted, migrations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Version < sorted[j].Version })
	maxApplied := 0
	for v := range applied {
		if v > maxApplied {
			maxApplied = v
		}
	}
	for _, m := range sorted {
		if applied[m.Version] {
			continue
		}
		if m.Version <= maxApplied {
			return fmt.Errorf("%w: migration v%d would land before applied v%d", ErrMigrationOutOfOrder, m.Version, maxApplied)
		}
		if err := runMigration(ctx, db, m); err != nil {
			return fmt.Errorf("migration v%d: %w", m.Version, err)
		}
		maxApplied = m.Version
	}
	return nil
}

func runMigration(ctx context.Context, db *sql.DB, m Migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, stmt := range m.Stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("executing %q: %w", firstLine(stmt), err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO migrations(version, applied_at) VALUES(?, ?)`, m.Version, time.Now().UTC()); err != nil {
		return fmt.Errorf("recording version: %w", err)
	}
	return tx.Commit()
}

func loadApplied(ctx context.Context, db *sql.DB) (map[int]bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT version FROM migrations`)
	if err != nil {
		return nil, fmt.Errorf("loading applied migrations: %w", err)
	}
	defer rows.Close()
	out := map[int]bool{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = true
	}
	return out, rows.Err()
}

func firstLine(s string) string {
	for i, r := range s {
		if r == '\n' {
			return s[:i]
		}
	}
	return s
}
