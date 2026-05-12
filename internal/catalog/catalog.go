// Package catalog provides access to the SQLite database that is the
// authoritative index of every segment Logpond knows about. Schema
// reference: PRD §10.2.
package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	_ "modernc.org/sqlite"
)

// Catalog wraps a *sql.DB connection to the catalog file. All read-side
// helpers required by later phases hang off this type; write-side helpers
// will be added as their callers come online.
type Catalog struct {
	db *sql.DB
}

// Open opens (or creates) the catalog file at path and applies the
// supplied migrations in version order. Pass nil to use the package's
// CoreMigrations.
func Open(ctx context.Context, path string, migrations []Migration) (*Catalog, error) {
	dsn := buildDSN(path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening catalog %s: %w", path, err)
	}
	// modernc.org/sqlite's "sqlite" driver is single-writer; cap the pool
	// so concurrent writers serialize through the same connection rather
	// than racing the WAL.
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pinging catalog: %w", err)
	}
	if migrations == nil {
		migrations = CoreMigrations()
	}
	if err := applyMigrations(ctx, db, migrations); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Catalog{db: db}, nil
}

// Close releases the underlying database handle.
func (c *Catalog) Close() error { return c.db.Close() }

// DB exposes the underlying *sql.DB for callers that need to compose
// transactions across catalog and other state.
func (c *Catalog) DB() *sql.DB { return c.db }

func buildDSN(path string) string {
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "synchronous(NORMAL)")
	return "file:" + path + "?" + q.Encode()
}

// Segment is the read-side projection of the segments row. Write-side
// fields will be filled in as their producers come online in later
// phases; for now we keep enough of the schema to satisfy the read stubs.
type Segment struct {
	ID             string
	State          string
	TimeStart      time.Time
	TimeEnd        time.Time
	RowCount       int64
	SizeBytes      int64
	SizeCompressed sql.NullInt64
	LocalPath      sql.NullString
	S3URL          sql.NullString
	ArchiveRef     sql.NullString
	ManifestSHA256 sql.NullString
	ParquetSHA256  sql.NullString
	SourceNames    sql.NullString
	CreatedAt      time.Time
	SealedAt       sql.NullTime
	ArchivedAt     sql.NullTime
	RehydratedAt   sql.NullTime
	EvictAfter     sql.NullTime
}

const segmentColumns = `id, state, time_start, time_end, row_count, size_bytes, size_compressed,
        local_path, s3_url, archive_ref, manifest_sha256, parquet_sha256, source_names,
        created_at, sealed_at, archived_at, rehydrated_at, evict_after`

// ListSegments returns every segment ordered by time_start descending.
func (c *Catalog) ListSegments(ctx context.Context) ([]Segment, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT `+segmentColumns+` FROM segments ORDER BY time_start DESC`)
	if err != nil {
		return nil, fmt.Errorf("listing segments: %w", err)
	}
	defer rows.Close()
	var out []Segment
	for rows.Next() {
		s, err := scanSegment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// GetSegment returns the segment with the given id. Returns
// sql.ErrNoRows if no such segment exists.
func (c *Catalog) GetSegment(ctx context.Context, id string) (Segment, error) {
	row := c.db.QueryRowContext(ctx, `SELECT `+segmentColumns+` FROM segments WHERE id = ?`, id)
	return scanSegment(row)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanSegment(r rowScanner) (Segment, error) {
	var s Segment
	err := r.Scan(
		&s.ID, &s.State, &s.TimeStart, &s.TimeEnd, &s.RowCount, &s.SizeBytes, &s.SizeCompressed,
		&s.LocalPath, &s.S3URL, &s.ArchiveRef, &s.ManifestSHA256, &s.ParquetSHA256, &s.SourceNames,
		&s.CreatedAt, &s.SealedAt, &s.ArchivedAt, &s.RehydratedAt, &s.EvictAfter,
	)
	if err != nil {
		return Segment{}, err
	}
	return s, nil
}

// AppliedMigrations returns the versions recorded in the migrations
// table, in ascending order. Useful for diagnostics and tests.
func (c *Catalog) AppliedMigrations(ctx context.Context) ([]int, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT version FROM migrations ORDER BY version ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// ErrMigrationOutOfOrder is returned when a migration's version is not
// strictly greater than every applied version.
var ErrMigrationOutOfOrder = errors.New("migration out of order")
