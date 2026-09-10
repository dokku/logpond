package segments

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/storage/duckdb"
)

// RecoverResult summarises what the recovery pass did. Useful for
// startup log lines and tests.
type RecoverResult struct {
	ActiveRegistered []string // active DuckDB files registered in catalog
	SealedRegistered []string // sealed Parquet files registered in catalog
	OrphansMoved     []string // active DuckDB ids superseded by a sealed Parquet
	LostMarked       []string // catalog entries whose files vanished
}

// Recover scans the data directory and reconciles with the catalog per
// PRD §7.2's "Crash recovery" rules. Call before starting to accept
// writes.
func (m *Manager) Recover(ctx context.Context) (RecoverResult, error) {
	var result RecoverResult

	activeFiles, err := scanIDs(m.activeDir, ".duckdb", "")
	if err != nil {
		return result, err
	}
	sealedFiles, err := scanIDs(m.sealedDir, ".parquet", "segment-")
	if err != nil {
		return result, err
	}

	// Rule: both DuckDB and Parquet for same id → keep Parquet, archive
	// the DuckDB into /data/orphans/.
	for id, ddPath := range activeFiles {
		if _, ok := sealedFiles[id]; !ok {
			continue
		}
		dest := filepath.Join(m.orphansDir, filepath.Base(ddPath))
		if err := os.Rename(ddPath, dest); err != nil {
			return result, fmt.Errorf("orphaning %s: %w", ddPath, err)
		}
		result.OrphansMoved = append(result.OrphansMoved, id)
		delete(activeFiles, id)
	}

	knownIDs, err := m.knownSegmentIDs(ctx)
	if err != nil {
		return result, err
	}

	// Rule: active DuckDB without entry → register as active.
	for id, ddPath := range activeFiles {
		if _, ok := knownIDs[id]; ok {
			continue
		}
		start, err := ParseSegmentID(id)
		if err != nil {
			m.logger.Warn("recovery: ignoring file with unparseable id", "path", ddPath, "err", err)
			continue
		}
		end := start.Add(m.window)
		if err := m.catalog.InsertSegment(ctx, catalog.Segment{
			ID:        id,
			State:     catalog.StateActive,
			TimeStart: start,
			TimeEnd:   end,
			LocalPath: sql.NullString{String: ddPath, Valid: true},
			CreatedAt: m.now().UTC(),
		}); err != nil {
			return result, fmt.Errorf("registering recovered active %s: %w", id, err)
		}
		result.ActiveRegistered = append(result.ActiveRegistered, id)
		knownIDs[id] = catalog.StateActive
	}

	// Rule: sealed Parquet without entry → register as sealed.
	for id, parquetPath := range sealedFiles {
		if _, ok := knownIDs[id]; ok {
			continue
		}
		if err := m.registerSealedFromDisk(ctx, id, parquetPath); err != nil {
			return result, fmt.Errorf("registering recovered sealed %s: %w", id, err)
		}
		result.SealedRegistered = append(result.SealedRegistered, id)
		knownIDs[id] = catalog.StateSealed
	}

	// Rule: catalog entry without file → mark lost.
	missing, err := m.findMissingFromDisk(ctx, activeFiles, sealedFiles)
	if err != nil {
		return result, err
	}
	for _, id := range missing {
		if err := m.catalog.UpdateSegmentState(ctx, id, catalog.StateLost); err != nil {
			return result, fmt.Errorf("marking %s lost: %w", id, err)
		}
		result.LostMarked = append(result.LostMarked, id)
	}

	// Reopen surviving active segments so queries and the sealing loop can
	// access their persisted events, including windows from before startup.
	for id := range activeFiles {
		if _, err := m.openOrCreate(ctx, id); err != nil {
			return result, fmt.Errorf("reopening recovered active %s: %w", id, err)
		}
	}

	return result, nil
}

func (m *Manager) knownSegmentIDs(ctx context.Context) (map[string]string, error) {
	segs, err := m.catalog.ListSegments(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(segs))
	for _, s := range segs {
		out[s.ID] = s.State
	}
	return out, nil
}

func (m *Manager) findMissingFromDisk(ctx context.Context, active, sealed map[string]string) ([]string, error) {
	segs, err := m.catalog.ListSegments(ctx)
	if err != nil {
		return nil, err
	}
	var lost []string
	for _, s := range segs {
		switch s.State {
		case catalog.StateActive:
			if _, ok := active[s.ID]; !ok {
				lost = append(lost, s.ID)
			}
		case catalog.StateSealed:
			if _, ok := sealed[s.ID]; !ok {
				lost = append(lost, s.ID)
			}
		}
	}
	return lost, nil
}

func (m *Manager) registerSealedFromDisk(ctx context.Context, id, path string) error {
	rows, lo, hi, sources, err := inspectParquet(ctx, path)
	if err != nil {
		return err
	}
	sum, sz, err := hashAndSize(path)
	if err != nil {
		return err
	}
	start, err := ParseSegmentID(id)
	if err != nil {
		return err
	}
	end := start.Add(m.window)
	if !lo.IsZero() {
		start = lo
	}
	if !hi.IsZero() {
		end = hi
	}
	sourcesJSON, err := json.Marshal(sources)
	if err != nil {
		return err
	}
	now := m.now().UTC()
	if err := m.catalog.InsertSegment(ctx, catalog.Segment{
		ID:             id,
		State:          catalog.StateSealed,
		TimeStart:      start,
		TimeEnd:        end,
		RowCount:       rows,
		SizeBytes:      sz,
		SizeCompressed: sql.NullInt64{Int64: sz, Valid: true},
		LocalPath:      sql.NullString{String: path, Valid: true},
		ParquetSHA256:  sql.NullString{String: sum, Valid: true},
		SourceNames:    sql.NullString{String: string(sourcesJSON), Valid: len(sources) > 0},
		CreatedAt:      now,
		SealedAt:       sql.NullTime{Time: now, Valid: true},
	}); err != nil {
		return err
	}
	return nil
}

func scanIDs(dir, ext, prefix string) (map[string]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("reading %s: %w", dir, err)
	}
	out := map[string]string{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ext) {
			continue
		}
		id := strings.TrimSuffix(name, ext)
		id = strings.TrimPrefix(id, prefix)
		if _, err := ParseSegmentID(id); err != nil {
			continue
		}
		out[id] = filepath.Join(dir, name)
	}
	return out, nil
}

func inspectParquet(ctx context.Context, path string) (rows int64, lo, hi time.Time, sources []string, err error) {
	conn, err := duckdb.Open(ctx, "", "")
	if err != nil {
		return 0, time.Time{}, time.Time{}, nil, fmt.Errorf("opening in-memory duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	q := fmt.Sprintf("SELECT count(*), min(timestamp), max(timestamp) FROM read_parquet('%s')", escapeSingleQuotes(path))
	row := conn.Conn().QueryRowContext(ctx, q)
	var loNT, hiNT sql.NullTime
	if err := row.Scan(&rows, &loNT, &hiNT); err != nil {
		return 0, time.Time{}, time.Time{}, nil, fmt.Errorf("inspecting parquet %s: %w", path, err)
	}
	if loNT.Valid {
		lo = loNT.Time.UTC()
	}
	if hiNT.Valid {
		hi = hiNT.Time.UTC()
	}
	q2 := fmt.Sprintf("SELECT DISTINCT source FROM read_parquet('%s') ORDER BY source", escapeSingleQuotes(path))
	rs, err := conn.Conn().QueryContext(ctx, q2)
	if err != nil {
		return 0, time.Time{}, time.Time{}, nil, fmt.Errorf("listing sources in %s: %w", path, err)
	}
	defer rs.Close()
	for rs.Next() {
		var s sql.NullString
		if err := rs.Scan(&s); err != nil {
			return 0, time.Time{}, time.Time{}, nil, err
		}
		if s.Valid {
			sources = append(sources, s.String)
		}
	}
	if err := rs.Err(); err != nil {
		return 0, time.Time{}, time.Time{}, nil, err
	}
	return rows, lo, hi, sources, nil
}

func escapeSingleQuotes(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
