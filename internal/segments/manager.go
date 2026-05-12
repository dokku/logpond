// Package segments implements the active-segment write path and the
// sealing lifecycle described in PRD §7.2. The Manager keeps one open
// DuckDB file per UTC time window, routes flushed events into the right
// window, and a background goroutine seals closed windows into Parquet
// files registered with the catalog.
package segments

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/storage/duckdb"
)

// SegmentIDFormat is the canonical UTC segment id (PRD §7.2).
const SegmentIDFormat = "200601021504"

// Options configures a Manager.
type Options struct {
	DataDir     string
	Window      time.Duration
	SealGrace   time.Duration // how long after window close before sealing eligible
	MemoryLimit string        // DuckDB memory_limit pragma value
	Catalog     *catalog.Catalog
	Logger      *slog.Logger
	Now         func() time.Time // injectable clock for tests
}

// Manager owns the open active segments and the sealing loop.
type Manager struct {
	dataDir     string
	activeDir   string
	sealedDir   string
	orphansDir  string
	tmpDir      string
	window      time.Duration
	sealGrace   time.Duration
	memoryLimit string
	catalog     *catalog.Catalog
	logger      *slog.Logger
	now         func() time.Time

	mu      sync.Mutex
	active  map[string]*activeSegment // id -> open file
	closing bool                      // set during Close to refuse new writes
}

type activeSegment struct {
	id        string
	timeStart time.Time
	timeEnd   time.Time
	conn      *duckdb.Connection
}

// New constructs a Manager. The data layout (active/sealed/orphans/tmp
// subdirs) is materialized eagerly so Flush calls don't race a mkdir.
func New(opts Options) (*Manager, error) {
	if opts.DataDir == "" {
		return nil, errors.New("DataDir is required")
	}
	if opts.Window <= 0 {
		return nil, errors.New("Window must be > 0")
	}
	if opts.Catalog == nil {
		return nil, errors.New("Catalog is required")
	}
	if opts.SealGrace <= 0 {
		opts.SealGrace = 60 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}

	m := &Manager{
		dataDir:     opts.DataDir,
		activeDir:   filepath.Join(opts.DataDir, "segments", "active"),
		sealedDir:   filepath.Join(opts.DataDir, "segments", "sealed"),
		orphansDir:  filepath.Join(opts.DataDir, "orphans"),
		tmpDir:      filepath.Join(opts.DataDir, "segments", "tmp"),
		window:      opts.Window,
		sealGrace:   opts.SealGrace,
		memoryLimit: opts.MemoryLimit,
		catalog:     opts.Catalog,
		logger:      opts.Logger,
		now:         opts.Now,
		active:      map[string]*activeSegment{},
	}

	for _, d := range []string{m.activeDir, m.sealedDir, m.orphansDir, m.tmpDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("creating %s: %w", d, err)
		}
	}
	return m, nil
}

// SegmentID returns the canonical id of the window that contains t,
// expressed in UTC. The window is the floor of t to the configured
// width.
func (m *Manager) SegmentID(t time.Time) string {
	return SegmentID(t.UTC(), m.window)
}

// SegmentID computes the window id for t given window width w.
func SegmentID(t time.Time, w time.Duration) string {
	return WindowStart(t, w).UTC().Format(SegmentIDFormat)
}

// WindowStart returns the start of the window containing t.
func WindowStart(t time.Time, w time.Duration) time.Time {
	return t.UTC().Truncate(w)
}

// CurrentSegmentID returns the segment id for the manager's "now".
func (m *Manager) CurrentSegmentID() string { return m.SegmentID(m.now()) }

// Flush routes events into their target segments. PRD §7.2: events
// outside any currently-open active window land in the current segment
// with the `logpond_late` / `logpond_intended_segment` attributes set.
func (m *Manager) Flush(ctx context.Context, events []ingest.Event) error {
	if len(events) == 0 {
		return nil
	}

	currentID := m.CurrentSegmentID()
	if _, err := m.openOrCreate(ctx, currentID); err != nil {
		return err
	}

	groups := map[string][]ingest.Event{}
	for _, ev := range events {
		evID := m.SegmentID(ev.Timestamp)
		targetID, late := m.resolveTarget(evID, currentID)
		if late {
			ev = annotateLate(ev, evID)
		}
		groups[targetID] = append(groups[targetID], ev)
	}

	for id, batch := range groups {
		seg, err := m.openOrCreate(ctx, id)
		if err != nil {
			return err
		}
		if err := seg.conn.InsertBatch(ctx, batch); err != nil {
			return fmt.Errorf("writing %d events to segment %s: %w", len(batch), id, err)
		}
	}
	return nil
}

func (m *Manager) resolveTarget(evID, currentID string) (string, bool) {
	m.mu.Lock()
	_, openHere := m.active[evID]
	m.mu.Unlock()
	if openHere {
		return evID, false
	}
	return currentID, true
}

func annotateLate(ev ingest.Event, intended string) ingest.Event {
	cp := make(map[string]any, len(ev.Attributes)+2)
	for k, v := range ev.Attributes {
		cp[k] = v
	}
	cp["logpond_late"] = true
	cp["logpond_intended_segment"] = intended
	ev.Attributes = cp
	return ev
}

func (m *Manager) openOrCreate(ctx context.Context, id string) (*activeSegment, error) {
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return nil, errors.New("manager is closing")
	}
	if s, ok := m.active[id]; ok {
		m.mu.Unlock()
		return s, nil
	}
	m.mu.Unlock()

	start, err := ParseSegmentID(id)
	if err != nil {
		return nil, err
	}
	end := start.Add(m.window)

	path := filepath.Join(m.activeDir, id+".duckdb")
	conn, err := duckdb.Open(ctx, path, m.memoryLimit)
	if err != nil {
		return nil, fmt.Errorf("opening active segment %s: %w", id, err)
	}

	seg := &activeSegment{id: id, timeStart: start, timeEnd: end, conn: conn}

	if err := m.ensureCatalogRow(ctx, id, start, end, path); err != nil {
		_ = conn.Close()
		return nil, err
	}

	m.mu.Lock()
	if existing, ok := m.active[id]; ok {
		m.mu.Unlock()
		_ = conn.Close()
		return existing, nil
	}
	m.active[id] = seg
	m.mu.Unlock()
	return seg, nil
}

func (m *Manager) ensureCatalogRow(ctx context.Context, id string, start, end time.Time, path string) error {
	if _, err := m.catalog.GetSegment(ctx, id); err == nil {
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("looking up segment %s: %w", id, err)
	}
	if err := m.catalog.InsertSegment(ctx, catalog.Segment{
		ID:        id,
		State:     catalog.StateActive,
		TimeStart: start,
		TimeEnd:   end,
		LocalPath: sql.NullString{String: path, Valid: true},
		CreatedAt: m.now().UTC(),
	}); err != nil {
		return fmt.Errorf("inserting segment %s: %w", id, err)
	}
	return nil
}

// ParseSegmentID parses an `YYYYMMDDhhmm` id back into a UTC time.
func ParseSegmentID(id string) (time.Time, error) {
	t, err := time.ParseInLocation(SegmentIDFormat, id, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing segment id %q: %w", id, err)
	}
	return t, nil
}

// Close flushes any open segments and closes their DuckDB connections.
// It does not seal them — the sealing loop owns sealing. Used during
// orderly shutdown so the next start can recover.
func (m *Manager) Close() error {
	m.mu.Lock()
	m.closing = true
	segs := make([]*activeSegment, 0, len(m.active))
	for _, s := range m.active {
		segs = append(segs, s)
	}
	m.active = map[string]*activeSegment{}
	m.mu.Unlock()

	var firstErr error
	for _, s := range segs {
		if err := s.conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("closing segment %s: %w", s.id, err)
		}
	}
	return firstErr
}

// QueryActive runs a SQL statement against the active segment with id.
// Returns (rows, true, nil) on success and (nil, false, nil) if the
// segment is not currently open as active — callers fall back to a
// catalog-based lookup in that case (it may have just sealed).
//
// The manager keeps DuckDB MaxOpenConns at 4, so concurrent reader
// queries don't deadlock the appender's held connection.
func (m *Manager) QueryActive(ctx context.Context, id, sqlText string, args ...any) (*sql.Rows, bool, error) {
	m.mu.Lock()
	seg, ok := m.active[id]
	m.mu.Unlock()
	if !ok {
		return nil, false, nil
	}
	if err := seg.conn.FlushAppender(); err != nil {
		return nil, true, fmt.Errorf("flushing appender before query: %w", err)
	}
	rows, err := seg.conn.DB().QueryContext(ctx, sqlText, args...)
	if err != nil {
		return nil, true, fmt.Errorf("querying active segment %s: %w", id, err)
	}
	return rows, true, nil
}

// SealableNow returns the active segments whose window closed at least
// sealGrace ago, oldest-first.
func (m *Manager) SealableNow() []string {
	cutoff := m.now().UTC().Add(-m.sealGrace)
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []string{}
	for id, s := range m.active {
		if !s.timeEnd.After(cutoff) {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// SealOnce processes every currently-sealable segment serially. Returns
// the ids that sealed successfully before any error.
func (m *Manager) SealOnce(ctx context.Context) ([]string, error) {
	ids := m.SealableNow()
	var sealed []string
	for _, id := range ids {
		if err := m.sealOne(ctx, id); err != nil {
			m.logger.Error("sealing segment failed", "id", id, "err", err)
			return sealed, err
		}
		sealed = append(sealed, id)
	}
	return sealed, nil
}

func (m *Manager) sealOne(ctx context.Context, id string) error {
	m.mu.Lock()
	seg, ok := m.active[id]
	if ok {
		delete(m.active, id)
	}
	m.mu.Unlock()
	if !ok {
		return nil
	}
	defer func() { _ = seg.conn.Close() }()

	if err := seg.conn.FlushAppender(); err != nil {
		return fmt.Errorf("flushing appender for %s: %w", id, err)
	}
	rows, err := seg.conn.RowCount(ctx)
	if err != nil {
		return err
	}
	if rows == 0 {
		_ = os.Remove(seg.conn.Path())
		if err := m.catalog.DeleteSegment(ctx, id); err != nil {
			return fmt.Errorf("deleting empty segment %s: %w", id, err)
		}
		m.logger.Info("sealed empty segment", "id", id)
		return nil
	}
	timeStart, timeEnd, _, err := seg.conn.TimeRange(ctx)
	if err != nil {
		return err
	}
	sources, err := seg.conn.SourceNames(ctx)
	if err != nil {
		return err
	}

	tmpPath := filepath.Join(m.tmpDir, "segment-"+id+".parquet.tmp")
	finalPath := filepath.Join(m.sealedDir, "segment-"+id+".parquet")
	_ = os.Remove(tmpPath)

	if err := seg.conn.ExportParquet(ctx, tmpPath); err != nil {
		return err
	}
	sum, sz, err := hashAndSize(tmpPath)
	if err != nil {
		return err
	}
	if err := fsyncFile(tmpPath); err != nil {
		return fmt.Errorf("fsync %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("renaming %s -> %s: %w", tmpPath, finalPath, err)
	}
	if err := fsyncDir(m.sealedDir); err != nil {
		return fmt.Errorf("fsync sealedDir: %w", err)
	}

	sourcesJSON, err := json.Marshal(sources)
	if err != nil {
		return fmt.Errorf("encoding source_names: %w", err)
	}
	if err := m.catalog.MarkSegmentSealed(ctx, id, catalog.SealedSegmentUpdate{
		RowCount:       rows,
		SizeBytes:      sz,
		SizeCompressed: sz,
		LocalPath:      finalPath,
		ParquetSHA256:  sum,
		SourceNames:    string(sourcesJSON),
		TimeStart:      timeStart,
		TimeEnd:        timeEnd,
	}); err != nil {
		return err
	}

	if err := os.Remove(seg.conn.Path()); err != nil && !errors.Is(err, os.ErrNotExist) {
		m.logger.Warn("removing sealed duckdb file", "id", id, "err", err)
	}
	m.logger.Info("sealed segment",
		"id", id,
		"rows", rows,
		"bytes", sz,
		"path", finalPath,
	)
	return nil
}

// RunSealLoop blocks until ctx is cancelled, calling SealOnce on each
// tick. interval defaults to the manager's seal grace period.
func (m *Manager) RunSealLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = m.sealGrace
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := m.SealOnce(ctx); err != nil {
				m.logger.Warn("sealing pass failed", "err", err)
			}
		}
	}
}
