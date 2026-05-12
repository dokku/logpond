// Package duckdb wraps the embedded DuckDB engine that backs Logpond's
// active segments. Each open segment file is one *Connection; once a
// segment seals (PRD §7.2) the file is exported to Parquet and the
// connection is closed.
package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dokku/logpond/internal/ingest"
	duckdbdriver "github.com/marcboeker/go-duckdb/v2"
)

// SchemaSQL creates the events table that matches PRD §7.2's row schema.
// Exposed so callers can also issue the same CREATE TABLE against an
// in-memory DuckDB for tests that want to inspect rows without a file.
const SchemaSQL = `CREATE TABLE IF NOT EXISTS events (
    timestamp TIMESTAMP NOT NULL,
    service VARCHAR,
    level VARCHAR,
    message VARCHAR,
    host VARCHAR,
    source VARCHAR NOT NULL,
    attributes VARCHAR,
    raw VARCHAR
)`

// Connection owns a single DuckDB database file and a cached sql.Conn
// the appender is bound to. The cached connection is held for the life
// of the Connection so the appender keeps a stable driver.Conn.
type Connection struct {
	path string
	db   *sql.DB
	conn *sql.Conn

	mu       sync.Mutex
	appender *duckdbdriver.Appender
}

// Open opens or creates a DuckDB database at path, applies the memory
// limit (PRD §7.11.1 default 128MB), and ensures the events table
// exists. Pass an empty memoryLimit to skip the pragma.
func Open(ctx context.Context, path string, memoryLimit string) (*Connection, error) {
	connector, err := duckdbdriver.NewConnector(path, func(execer driver.ExecerContext) error {
		if memoryLimit == "" {
			return nil
		}
		_, err := execer.ExecContext(ctx, fmt.Sprintf("SET memory_limit='%s'", memoryLimit), nil)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("opening duckdb %s: %w", path, err)
	}
	db := sql.OpenDB(connector)
	// The appender holds one sql.Conn for the lifetime of the segment;
	// allow a small pool so reader queries (counts, sealing exports)
	// can run alongside it without deadlocking on the appender's conn.
	db.SetMaxOpenConns(4)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pinging duckdb %s: %w", path, err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("acquiring duckdb conn: %w", err)
	}
	if _, err := conn.ExecContext(ctx, SchemaSQL); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("creating events table: %w", err)
	}
	return &Connection{path: path, db: db, conn: conn}, nil
}

// Path returns the database file path.
func (c *Connection) Path() string { return c.path }

// DB exposes the underlying *sql.DB for ad-hoc queries (sealing reads
// row counts, tests issue SELECTs). Do not close it directly — use
// Connection.Close.
func (c *Connection) DB() *sql.DB { return c.db }

// Conn returns the cached *sql.Conn used by the appender. Useful for
// running statements (e.g. COPY) against the same connection that owns
// the database file.
func (c *Connection) Conn() *sql.Conn { return c.conn }

// InsertBatch appends events using DuckDB's row-at-a-time appender API
// and flushes at the end. The appender batches under the hood.
func (c *Connection) InsertBatch(ctx context.Context, events []ingest.Event) error {
	if len(events) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.appender == nil {
		if err := c.openAppender(ctx); err != nil {
			return err
		}
	}

	for i := range events {
		ev := &events[i]
		attrs, err := encodeAttributes(ev.Attributes)
		if err != nil {
			return fmt.Errorf("encoding attributes for event %d: %w", i, err)
		}
		if err := c.appender.AppendRow(
			ev.Timestamp.UTC(),
			nullableString(ev.Service),
			nullableString(ev.Level),
			nullableString(ev.Message),
			nullableString(ev.Host),
			ev.Source,
			attrs,
			ev.Raw,
		); err != nil {
			return fmt.Errorf("appending row %d: %w", i, err)
		}
	}
	if err := c.appender.Flush(); err != nil {
		return fmt.Errorf("flushing appender: %w", err)
	}
	return nil
}

func (c *Connection) openAppender(ctx context.Context) error {
	var appender *duckdbdriver.Appender
	err := c.conn.Raw(func(driverConn any) error {
		dc, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("driver connection does not implement driver.Conn (got %T)", driverConn)
		}
		a, err := duckdbdriver.NewAppenderFromConn(dc, "", "events")
		if err != nil {
			return err
		}
		appender = a
		return nil
	})
	if err != nil {
		return fmt.Errorf("opening appender: %w", err)
	}
	c.appender = appender
	return nil
}

// RowCount returns the number of rows currently in the events table.
func (c *Connection) RowCount(ctx context.Context) (int64, error) {
	if err := c.flushAppender(); err != nil {
		return 0, err
	}
	var n int64
	row := c.conn.QueryRowContext(ctx, "SELECT count(*) FROM events")
	if err := row.Scan(&n); err != nil {
		return 0, fmt.Errorf("counting events: %w", err)
	}
	return n, nil
}

// TimeRange returns the min/max event timestamps in the segment, or
// (zero, zero, false) when the table is empty.
func (c *Connection) TimeRange(ctx context.Context) (time.Time, time.Time, bool, error) {
	if err := c.flushAppender(); err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	var lo, hi sql.NullTime
	row := c.conn.QueryRowContext(ctx, "SELECT min(timestamp), max(timestamp) FROM events")
	if err := row.Scan(&lo, &hi); err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("scanning time range: %w", err)
	}
	if !lo.Valid || !hi.Valid {
		return time.Time{}, time.Time{}, false, nil
	}
	return lo.Time.UTC(), hi.Time.UTC(), true, nil
}

// SourceNames returns the distinct source values present in the events
// table (PRD §10.2 segments.source_names).
func (c *Connection) SourceNames(ctx context.Context) ([]string, error) {
	if err := c.flushAppender(); err != nil {
		return nil, err
	}
	rows, err := c.conn.QueryContext(ctx, "SELECT DISTINCT source FROM events ORDER BY source")
	if err != nil {
		return nil, fmt.Errorf("listing source names: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s sql.NullString
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		if s.Valid {
			out = append(out, s.String)
		}
	}
	return out, rows.Err()
}

// ExportParquet writes the events table to outPath as Parquet with
// zstd-3 compression. The file is written via DuckDB's COPY statement,
// which guarantees a file readable by the DuckDB CLI and pyarrow.
func (c *Connection) ExportParquet(ctx context.Context, outPath string) error {
	if err := c.flushAppender(); err != nil {
		return err
	}
	stmt := fmt.Sprintf(
		"COPY (SELECT * FROM events ORDER BY timestamp) TO %s (FORMAT PARQUET, COMPRESSION 'zstd', COMPRESSION_LEVEL 3)",
		quoteString(outPath),
	)
	if _, err := c.conn.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("copying to parquet %s: %w", outPath, err)
	}
	return nil
}

// FlushAppender pushes any pending appender rows to disk. Useful before
// sealing and in tests that read from the same connection.
func (c *Connection) FlushAppender() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flushAppender()
}

func (c *Connection) flushAppender() error {
	if c.appender == nil {
		return nil
	}
	return c.appender.Flush()
}

// Close releases the appender, cached connection, and database handle.
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	if c.appender != nil {
		if err := c.appender.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("closing appender: %w", err)
		}
		c.appender = nil
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("closing duckdb conn: %w", err)
		}
		c.conn = nil
	}
	if c.db != nil {
		if err := c.db.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("closing duckdb db: %w", err)
		}
		c.db = nil
	}
	return firstErr
}

func encodeAttributes(attrs map[string]any) (string, error) {
	if len(attrs) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(attrs)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func quoteString(s string) string {
	// Single-quote literal with embedded single-quotes doubled.
	out := make([]byte, 0, len(s)+2)
	out = append(out, '\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\'')
		}
		out = append(out, s[i])
	}
	out = append(out, '\'')
	return string(out)
}
