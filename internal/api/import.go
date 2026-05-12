package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/segments"
)

// importMaxBody caps a single sideload request body at 5GB per PRD
// §13.12. Streaming through multipart keeps memory bounded; the cap is
// an outer guard against pathological clients.
const importMaxBody = 5 << 30

type importResponse struct {
	SegmentID  string      `json:"segment_id"`
	State      string      `json:"state"`
	RowCount   int64       `json:"row_count"`
	SizeBytes  int64       `json:"size_bytes"`
	EvictAfter *time.Time  `json:"evict_after"`
	Persistent bool        `json:"persistent"`
	Overlaps   []OverlapID `json:"overlaps"`
}

// OverlapID is the response shape for a segment whose time window
// intersects the imported segment's window (PRD §13.12).
type OverlapID struct {
	SegmentID string    `json:"segment_id"`
	TimeStart time.Time `json:"time_start"`
	TimeEnd   time.Time `json:"time_end"`
}

func (s *Server) handleImport(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	persistent := r.URL.Query().Get("persistent") == "true"

	// 32MB part buffer; the file streams to disk past that.
	r.Body = http.MaxBytesReader(w, r.Body, importMaxBody)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large", err.Error(), nil)
		return
	}

	rehydratedDir := filepath.Join(s.dataDir, "rehydrated")
	if err := os.MkdirAll(rehydratedDir, 0o755); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}

	// Stage uploads to a per-request temp directory so a failure can
	// nuke the whole thing without affecting the live tree.
	stageDir, err := os.MkdirTemp(rehydratedDir, ".import-")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}
	defer os.RemoveAll(stageDir)

	parquetTmp := filepath.Join(stageDir, "parquet")
	manifestTmp := filepath.Join(stageDir, "manifest.json")

	if err := savePart(r, "parquet", parquetTmp); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "parquet part: "+err.Error(), nil)
		return
	}
	if err := savePart(r, "manifest", manifestTmp); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "manifest part: "+err.Error(), nil)
		return
	}

	manifest, err := segments.ReadManifestFile(manifestTmp)
	if err != nil {
		writeError(w, http.StatusBadRequest, "manifest_invalid", err.Error(), nil)
		return
	}
	sha, _, err := segments.ParquetSHA256(parquetTmp)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}
	if !strings.EqualFold(sha, manifest.ParquetSHA256) {
		writeError(w, http.StatusBadRequest, "manifest_invalid",
			fmt.Sprintf("parquet sha mismatch: got %s want %s", sha, manifest.ParquetSHA256), nil)
		return
	}

	if _, err := s.catalog.GetSegment(r.Context(), manifest.SegmentID); err == nil {
		writeError(w, http.StatusConflict, "segment_conflict",
			fmt.Sprintf("segment %s already exists in catalog", manifest.SegmentID), nil)
		return
	} else if !errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}

	overlapping, err := s.catalog.SegmentsOverlapping(r.Context(), manifest.TimeStart, manifest.TimeEnd)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}
	overlaps := make([]OverlapID, 0, len(overlapping))
	for _, o := range overlapping {
		if o.ID == manifest.SegmentID {
			continue
		}
		overlaps = append(overlaps, OverlapID{
			SegmentID: o.ID,
			TimeStart: o.TimeStart,
			TimeEnd:   o.TimeEnd,
		})
	}

	parquetFinal := filepath.Join(rehydratedDir, segments.ParquetFilename(manifest.SegmentID))
	manifestFinal := filepath.Join(rehydratedDir, segments.ManifestFilename(manifest.SegmentID))
	if err := os.Rename(parquetTmp, parquetFinal); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}
	if err := os.Rename(manifestTmp, manifestFinal); err != nil {
		_ = os.Remove(parquetFinal)
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}

	importPersistent := persistent || manifest.Persistent
	var evict *time.Time
	if !importPersistent {
		t := time.Now().UTC().Add(s.rehydrationTTL)
		evict = &t
	}

	sourcesJSON := ""
	if len(manifest.SourceNames) > 0 {
		if buf, err := json.Marshal(manifest.SourceNames); err == nil {
			sourcesJSON = string(buf)
		}
	}

	if err := s.catalog.InsertImportedSegment(r.Context(), catalog.ImportedSegment{
		ID:             manifest.SegmentID,
		TimeStart:      manifest.TimeStart,
		TimeEnd:        manifest.TimeEnd,
		RowCount:       manifest.RowCount,
		SizeBytes:      manifest.SizeBytes,
		SizeCompressed: manifest.SizeCompressed,
		LocalPath:      parquetFinal,
		ParquetSHA256:  manifest.ParquetSHA256,
		SourceNames:    sourcesJSON,
		EvictAfter:     evict,
	}); err != nil {
		_ = os.Remove(parquetFinal)
		_ = os.Remove(manifestFinal)
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}

	writeJSON(w, http.StatusCreated, importResponse{
		SegmentID:  manifest.SegmentID,
		State:      catalog.StateRehydrated,
		RowCount:   manifest.RowCount,
		SizeBytes:  manifest.SizeBytes,
		EvictAfter: evict,
		Persistent: importPersistent,
		Overlaps:   overlaps,
	})
}

func savePart(r *http.Request, name, dst string) error {
	f, header, err := r.FormFile(name)
	if err != nil {
		return fmt.Errorf("missing %s part: %w", name, err)
	}
	defer f.Close()
	_ = header
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, f); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// ImportWatcher polls the data/import/ directory every interval and
// picks up triplets of segment-<id>.parquet, segment-<id>.manifest.json,
// and segment-<id>.ready. On successful registration the files are moved
// to import/imported/<id>/. On failure they go to import/failed/<id>/
// with an error.txt explaining the rejection.
type ImportWatcher struct {
	dataDir string
	catalog *catalog.Catalog
	ttl     time.Duration
	logger  interface{ Warn(msg string, args ...any) }
}

// ImportWatcherOptions configures the watcher.
type ImportWatcherOptions struct {
	DataDir string
	Catalog *catalog.Catalog
	TTL     time.Duration
	Logger  interface{ Warn(msg string, args ...any) }
}

// NewImportWatcher constructs an ImportWatcher.
func NewImportWatcher(opts ImportWatcherOptions) *ImportWatcher {
	return &ImportWatcher{
		dataDir: opts.DataDir,
		catalog: opts.Catalog,
		ttl:     opts.TTL,
		logger:  opts.Logger,
	}
}

// Run blocks until ctx ends, sweeping the import dir on each tick.
func (w *ImportWatcher) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	// Run once immediately so dropped files don't wait a full tick.
	_ = w.Sweep(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := w.Sweep(ctx); err != nil && w.logger != nil {
				w.logger.Warn("import watcher sweep", "err", err)
			}
		}
	}
}

// Sweep scans the import directory once and processes any complete
// triplets. It returns the first directory-level error; per-file
// failures are logged inline via failed/<id>/error.txt.
func (w *ImportWatcher) Sweep(ctx context.Context) error {
	importDir := filepath.Join(w.dataDir, "import")
	if err := os.MkdirAll(importDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", importDir, err)
	}
	entries, err := os.ReadDir(importDir)
	if err != nil {
		return fmt.Errorf("reading %s: %w", importDir, err)
	}
	ready := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".ready") {
			ready[strings.TrimSuffix(name, ".ready")] = true
		}
	}
	for stem := range ready {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := w.processOne(ctx, importDir, stem); err != nil && w.logger != nil {
			w.logger.Warn("import watcher: process", "stem", stem, "err", err)
		}
	}
	return nil
}

func (w *ImportWatcher) processOne(ctx context.Context, importDir, stem string) error {
	parquetSrc := filepath.Join(importDir, stem+".parquet")
	manifestSrc := filepath.Join(importDir, stem+".manifest.json")
	readySrc := filepath.Join(importDir, stem+".ready")

	manifest, importErr := segments.ReadManifestFile(manifestSrc)
	if importErr != nil {
		return w.moveToFailed(stem, parquetSrc, manifestSrc, readySrc, importErr)
	}
	sha, _, err := segments.ParquetSHA256(parquetSrc)
	if err != nil {
		return w.moveToFailed(stem, parquetSrc, manifestSrc, readySrc, err)
	}
	if !strings.EqualFold(sha, manifest.ParquetSHA256) {
		return w.moveToFailed(stem, parquetSrc, manifestSrc, readySrc,
			fmt.Errorf("parquet sha mismatch: got %s want %s", sha, manifest.ParquetSHA256))
	}
	if _, err := w.catalog.GetSegment(ctx, manifest.SegmentID); err == nil {
		return w.moveToFailed(stem, parquetSrc, manifestSrc, readySrc,
			fmt.Errorf("segment %s already exists", manifest.SegmentID))
	} else if !errors.Is(err, sql.ErrNoRows) {
		return w.moveToFailed(stem, parquetSrc, manifestSrc, readySrc, err)
	}

	rehydratedDir := filepath.Join(w.dataDir, "rehydrated")
	if err := os.MkdirAll(rehydratedDir, 0o755); err != nil {
		return w.moveToFailed(stem, parquetSrc, manifestSrc, readySrc, err)
	}
	parquetFinal := filepath.Join(rehydratedDir, segments.ParquetFilename(manifest.SegmentID))
	manifestFinal := filepath.Join(rehydratedDir, segments.ManifestFilename(manifest.SegmentID))
	if err := os.Rename(parquetSrc, parquetFinal); err != nil {
		return w.moveToFailed(stem, parquetSrc, manifestSrc, readySrc, err)
	}
	if err := os.Rename(manifestSrc, manifestFinal); err != nil {
		_ = os.Remove(parquetFinal)
		return w.moveToFailed(stem, parquetSrc, manifestSrc, readySrc, err)
	}

	var evict *time.Time
	if !manifest.Persistent {
		t := time.Now().UTC().Add(w.ttl)
		evict = &t
	}
	sourcesJSON := ""
	if len(manifest.SourceNames) > 0 {
		if buf, err := json.Marshal(manifest.SourceNames); err == nil {
			sourcesJSON = string(buf)
		}
	}
	if err := w.catalog.InsertImportedSegment(ctx, catalog.ImportedSegment{
		ID:             manifest.SegmentID,
		TimeStart:      manifest.TimeStart,
		TimeEnd:        manifest.TimeEnd,
		RowCount:       manifest.RowCount,
		SizeBytes:      manifest.SizeBytes,
		SizeCompressed: manifest.SizeCompressed,
		LocalPath:      parquetFinal,
		ParquetSHA256:  manifest.ParquetSHA256,
		SourceNames:    sourcesJSON,
		EvictAfter:     evict,
	}); err != nil {
		_ = os.Remove(parquetFinal)
		_ = os.Remove(manifestFinal)
		return w.moveToFailed(stem, parquetSrc, manifestSrc, readySrc, err)
	}
	// Success: relocate the source-side ready marker (parquet/manifest
	// already renamed into /rehydrated/). The imported/<id>/ marker is
	// what an operator looks at to confirm pickup.
	importedDir := filepath.Join(w.dataDir, "import", "imported", manifest.SegmentID)
	if err := os.MkdirAll(importedDir, 0o755); err != nil {
		return err
	}
	_ = os.Rename(readySrc, filepath.Join(importedDir, stem+".ready"))
	return nil
}

func (w *ImportWatcher) moveToFailed(stem, parquetSrc, manifestSrc, readySrc string, cause error) error {
	failedDir := filepath.Join(w.dataDir, "import", "failed", stem)
	if err := os.MkdirAll(failedDir, 0o755); err != nil {
		return err
	}
	for _, src := range []string{parquetSrc, manifestSrc, readySrc} {
		if _, err := os.Stat(src); err != nil {
			continue
		}
		dst := filepath.Join(failedDir, filepath.Base(src))
		_ = os.Rename(src, dst)
	}
	_ = os.WriteFile(filepath.Join(failedDir, "error.txt"), []byte(cause.Error()+"\n"), 0o644)
	return cause
}
