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
	"strings"
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

// Segment lifecycle states recorded in segments.state. The values here
// mirror PRD §10.2's enumeration.
const (
	StateActive     = "active"
	StateSealed     = "sealed"
	StateArchived   = "archived"
	StateRehydrated = "rehydrated"
	StateLost       = "lost"
)

// InsertSegment writes a new segments row. State defaults to
// StateActive when empty; CreatedAt defaults to time.Now().UTC() when
// zero.
func (c *Catalog) InsertSegment(ctx context.Context, s Segment) error {
	if s.State == "" {
		s.State = StateActive
	}
	if s.CreatedAt.IsZero() {
		s.CreatedAt = time.Now().UTC()
	}
	_, err := c.db.ExecContext(ctx, `INSERT INTO segments(
        id, state, time_start, time_end, row_count, size_bytes, size_compressed,
        local_path, s3_url, archive_ref, manifest_sha256, parquet_sha256, source_names,
        created_at, sealed_at, archived_at, rehydrated_at, evict_after
    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		s.ID, s.State, s.TimeStart.UTC(), s.TimeEnd.UTC(), s.RowCount, s.SizeBytes, s.SizeCompressed,
		s.LocalPath, s.S3URL, s.ArchiveRef, s.ManifestSHA256, s.ParquetSHA256, s.SourceNames,
		s.CreatedAt.UTC(), s.SealedAt, s.ArchivedAt, s.RehydratedAt, s.EvictAfter,
	)
	if err != nil {
		return fmt.Errorf("inserting segment %s: %w", s.ID, err)
	}
	return nil
}

// UpdateSegmentState transitions a segment to newState and optionally
// updates the timestamp column that corresponds to the new state.
func (c *Catalog) UpdateSegmentState(ctx context.Context, id, newState string) error {
	now := time.Now().UTC()
	var col string
	switch newState {
	case StateSealed:
		col = "sealed_at"
	case StateArchived:
		col = "archived_at"
	case StateRehydrated:
		col = "rehydrated_at"
	}
	q := `UPDATE segments SET state = ?`
	args := []any{newState}
	if col != "" {
		q += `, ` + col + ` = ?`
		args = append(args, now)
	}
	q += ` WHERE id = ?`
	args = append(args, id)
	res, err := c.db.ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("updating segment %s state: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("segment %s not found", id)
	}
	return nil
}

// SealedSegmentUpdate captures the fields populated when an active
// segment finishes sealing into a Parquet file.
type SealedSegmentUpdate struct {
	RowCount       int64
	SizeBytes      int64
	SizeCompressed int64
	LocalPath      string
	ParquetSHA256  string
	SourceNames    string // JSON-encoded array per §10.2
	TimeStart      time.Time
	TimeEnd        time.Time
}

// MarkSegmentSealed updates an active segment row to the sealed state
// with the finalized size, checksum, and timing fields populated.
func (c *Catalog) MarkSegmentSealed(ctx context.Context, id string, u SealedSegmentUpdate) error {
	now := time.Now().UTC()
	_, err := c.db.ExecContext(ctx, `UPDATE segments SET
        state = ?,
        time_start = ?,
        time_end = ?,
        row_count = ?,
        size_bytes = ?,
        size_compressed = ?,
        local_path = ?,
        parquet_sha256 = ?,
        source_names = ?,
        sealed_at = ?
    WHERE id = ?`,
		StateSealed, u.TimeStart.UTC(), u.TimeEnd.UTC(), u.RowCount, u.SizeBytes,
		sql.NullInt64{Int64: u.SizeCompressed, Valid: u.SizeCompressed > 0},
		sql.NullString{String: u.LocalPath, Valid: u.LocalPath != ""},
		sql.NullString{String: u.ParquetSHA256, Valid: u.ParquetSHA256 != ""},
		sql.NullString{String: u.SourceNames, Valid: u.SourceNames != ""},
		now, id,
	)
	if err != nil {
		return fmt.Errorf("marking segment %s sealed: %w", id, err)
	}
	return nil
}

// DeleteSegment removes a segments row. Used for crash recovery when a
// catalog entry points at a missing file (PRD §7.2).
func (c *Catalog) DeleteSegment(ctx context.Context, id string) error {
	_, err := c.db.ExecContext(ctx, `DELETE FROM segments WHERE id = ?`, id)
	return err
}

// SegmentsInRange returns segments whose [time_start, time_end]
// overlaps [from, to], ordered by time_start descending so callers
// can scan newest-first. Lost segments are excluded — their files are
// gone and no longer queryable.
func (c *Catalog) SegmentsInRange(ctx context.Context, from, to time.Time) ([]Segment, error) {
	rows, err := c.db.QueryContext(ctx, `
        SELECT `+segmentColumns+` FROM segments
        WHERE state != ?
          AND time_start <= ?
          AND time_end   >= ?
        ORDER BY time_start DESC`,
		StateLost, to.UTC(), from.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("listing segments in range: %w", err)
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

// ListSegmentsByState returns segments in the given state ordered by
// time_start ascending so callers process them oldest-first.
func (c *Catalog) ListSegmentsByState(ctx context.Context, state string) ([]Segment, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT `+segmentColumns+` FROM segments WHERE state = ? ORDER BY time_start ASC`, state)
	if err != nil {
		return nil, fmt.Errorf("listing segments in state %s: %w", state, err)
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

// CustomFacetSourceConfig marks rows that originated from the static
// YAML config; CustomFacetSourceUI marks rows that the operator added
// at runtime via the Admin API.
const (
	CustomFacetSourceConfig = "config"
	CustomFacetSourceUI     = "ui"
)

// CustomFacet mirrors a row in the custom_facets table.
type CustomFacet struct {
	Name           string
	Field          string
	DisplayLabel   sql.NullString
	CardinalityCap sql.NullInt64
	ValueType      string
	Source         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

const customFacetColumns = `name, field, display_label, cardinality_cap, value_type, source, created_at, updated_at`

// ListCustomFacets returns every row in the custom_facets table ordered
// by created_at ascending so the registry preserves insertion order.
func (c *Catalog) ListCustomFacets(ctx context.Context) ([]CustomFacet, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT `+customFacetColumns+` FROM custom_facets ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("listing custom facets: %w", err)
	}
	defer rows.Close()
	var out []CustomFacet
	for rows.Next() {
		f, err := scanCustomFacet(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// GetCustomFacet returns the row with the given name. Returns
// sql.ErrNoRows when no such facet exists.
func (c *Catalog) GetCustomFacet(ctx context.Context, name string) (CustomFacet, error) {
	row := c.db.QueryRowContext(ctx, `SELECT `+customFacetColumns+` FROM custom_facets WHERE name = ?`, name)
	return scanCustomFacet(row)
}

// InsertCustomFacet writes a new custom_facets row. CreatedAt and
// UpdatedAt default to time.Now().UTC() when zero.
func (c *Catalog) InsertCustomFacet(ctx context.Context, f CustomFacet) error {
	now := time.Now().UTC()
	if f.CreatedAt.IsZero() {
		f.CreatedAt = now
	}
	if f.UpdatedAt.IsZero() {
		f.UpdatedAt = now
	}
	if f.ValueType == "" {
		f.ValueType = "string"
	}
	_, err := c.db.ExecContext(ctx, `INSERT INTO custom_facets(
        name, field, display_label, cardinality_cap, value_type, source, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		f.Name, f.Field, f.DisplayLabel, f.CardinalityCap, f.ValueType, f.Source,
		f.CreatedAt.UTC(), f.UpdatedAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("inserting custom facet %s: %w", f.Name, err)
	}
	return nil
}

// CustomFacetUpdate carries patchable fields for a PATCH-style call.
// Nil pointers mean "leave the column unchanged".
type CustomFacetUpdate struct {
	DisplayLabel   *string
	CardinalityCap *int64
}

// UpdateCustomFacet applies u to the row named name. Returns
// sql.ErrNoRows when no such facet exists.
func (c *Catalog) UpdateCustomFacet(ctx context.Context, name string, u CustomFacetUpdate) (CustomFacet, error) {
	now := time.Now().UTC()
	setParts := []string{"updated_at = ?"}
	args := []any{now}
	if u.DisplayLabel != nil {
		setParts = append(setParts, "display_label = ?")
		args = append(args, sql.NullString{String: *u.DisplayLabel, Valid: *u.DisplayLabel != ""})
	}
	if u.CardinalityCap != nil {
		setParts = append(setParts, "cardinality_cap = ?")
		args = append(args, sql.NullInt64{Int64: *u.CardinalityCap, Valid: *u.CardinalityCap > 0})
	}
	args = append(args, name)
	q := `UPDATE custom_facets SET ` + strings.Join(setParts, ", ") + ` WHERE name = ?`
	res, err := c.db.ExecContext(ctx, q, args...)
	if err != nil {
		return CustomFacet{}, fmt.Errorf("updating custom facet %s: %w", name, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return CustomFacet{}, err
	}
	if n == 0 {
		return CustomFacet{}, sql.ErrNoRows
	}
	return c.GetCustomFacet(ctx, name)
}

// DeleteCustomFacet removes the row named name. Returns false when no
// such facet exists.
func (c *Catalog) DeleteCustomFacet(ctx context.Context, name string) (bool, error) {
	res, err := c.db.ExecContext(ctx, `DELETE FROM custom_facets WHERE name = ?`, name)
	if err != nil {
		return false, fmt.Errorf("deleting custom facet %s: %w", name, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func scanCustomFacet(r rowScanner) (CustomFacet, error) {
	var f CustomFacet
	if err := r.Scan(&f.Name, &f.Field, &f.DisplayLabel, &f.CardinalityCap, &f.ValueType, &f.Source, &f.CreatedAt, &f.UpdatedAt); err != nil {
		return CustomFacet{}, err
	}
	return f, nil
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
