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

	"github.com/go-chi/chi/v5"

	"github.com/dokku/logpond/internal/archive"
	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/jobs"
	"github.com/dokku/logpond/internal/segments"
)

type rehydrateRequest struct {
	TimeRange  *timeRangeReq `json:"time_range,omitempty"`
	SegmentID  string        `json:"segment_id,omitempty"`
	TTLDays    int           `json:"ttl_days,omitempty"`
	Persistent bool          `json:"persistent,omitempty"`
}

type timeRangeReq struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

type rehydrateResponse struct {
	JobID            string   `json:"job_id"`
	Segments         []string `json:"segments"`
	SegmentCount     int      `json:"segment_count"`
	TotalBytes       int64    `json:"total_bytes"`
	EstimatedSeconds int      `json:"estimated_seconds"`
	StatusURL        string   `json:"status_url"`
}

func (s *Server) handleRehydrate(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if s.archiveBackend == nil || s.archiveBackend.Capabilities().Name == "none" {
		writeError(w, http.StatusNotImplemented, "rehydrate_unsupported",
			"archive backend is set to 'none'", nil)
		return
	}
	caps := s.archiveBackend.Capabilities()
	if !caps.Retrieve {
		writeError(w, http.StatusNotImplemented, "rehydrate_unsupported",
			"backend does not support retrieve", nil)
		return
	}
	if s.jobs == nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "jobs manager not configured", nil)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error(), nil)
		return
	}
	var req rehydrateRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error(), nil)
			return
		}
	}

	segs, err := s.selectRehydrateCandidates(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error(), nil)
		return
	}
	if len(segs) == 0 {
		writeError(w, http.StatusNotFound, "not_found", "no archived segments overlap the requested range", nil)
		return
	}

	ttl := s.rehydrationTTL
	if req.TTLDays > 0 {
		ttl = time.Duration(req.TTLDays) * 24 * time.Hour
	}
	persistent := req.Persistent

	totalBytes := int64(0)
	for _, seg := range segs {
		totalBytes += seg.SizeBytes
	}

	jobID, err := s.jobs.Run(r.Context(), jobs.CreatePayload{
		Type:       jobs.TypeRehydrate,
		Payload:    map[string]any{"segments": segIDs(segs), "ttl_seconds": int(ttl.Seconds()), "persistent": persistent},
		TotalItems: len(segs),
		TotalBytes: totalBytes,
	}, func(ctx context.Context, id string) error {
		return s.runRehydrateJob(ctx, id, segs, ttl, persistent)
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}

	writeJSON(w, http.StatusAccepted, rehydrateResponse{
		JobID:            jobID,
		Segments:         segIDs(segs),
		SegmentCount:     len(segs),
		TotalBytes:       totalBytes,
		EstimatedSeconds: estimateRehydrateSeconds(totalBytes),
		StatusURL:        "/api/jobs/" + jobID,
	})
}

func (s *Server) selectRehydrateCandidates(ctx context.Context, req rehydrateRequest) ([]catalog.Segment, error) {
	if id := strings.TrimSpace(req.SegmentID); id != "" {
		seg, err := s.catalog.GetSegment(ctx, id)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil
			}
			return nil, err
		}
		return []catalog.Segment{seg}, nil
	}
	if req.TimeRange == nil {
		return nil, fmt.Errorf("time_range is required when segment_id is not set")
	}
	if !req.TimeRange.To.After(req.TimeRange.From) {
		return nil, fmt.Errorf("time_range.to must be after time_range.from")
	}
	all, err := s.catalog.SegmentsOverlapping(ctx, req.TimeRange.From, req.TimeRange.To)
	if err != nil {
		return nil, err
	}
	out := make([]catalog.Segment, 0, len(all))
	for _, seg := range all {
		// Already-local rehydrate (PRD §7.10): a rehydrated row in the
		// time range still counts as a candidate so the job can bump
		// its TTL.
		if seg.State == catalog.StateRehydrated && seg.LocalPath.Valid && seg.LocalPath.String != "" {
			out = append(out, seg)
			continue
		}
		// Only segments with an archive copy can be rehydrated.
		if seg.S3URL.Valid && seg.S3URL.String != "" {
			out = append(out, seg)
			continue
		}
		if seg.ArchiveRef.Valid && seg.ArchiveRef.String != "" {
			out = append(out, seg)
		}
	}
	return out, nil
}

// estimateRehydrateSeconds is a coarse heuristic the response surfaces
// for UI progress hints. ~50MB/s is a reasonable lower bound for warm
// S3 + local-disk paths; if anything we want to over-estimate slightly
// so a successful job feels like it beat the prediction.
func estimateRehydrateSeconds(totalBytes int64) int {
	const bytesPerSecond = 50 * 1024 * 1024
	if totalBytes <= 0 {
		return 1
	}
	secs := totalBytes / bytesPerSecond
	if secs < 1 {
		secs = 1
	}
	return int(secs)
}

func (s *Server) runRehydrateJob(ctx context.Context, jobID string, segs []catalog.Segment, ttl time.Duration, persistent bool) error {
	rehydratedDir := filepath.Join(s.dataDir, "rehydrated")
	if err := os.MkdirAll(rehydratedDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", rehydratedDir, err)
	}

	var evict *time.Time
	if !persistent {
		t := time.Now().UTC().Add(ttl)
		evict = &t
	}

	var stdoutAcc, stderrAcc string
	flushOutput := func() {
		if stdoutAcc == "" && stderrAcc == "" {
			return
		}
		_ = s.jobs.AttachScriptOutput(ctx, jobID, stdoutAcc, stderrAcc)
	}

	for i, seg := range segs {
		if err := ctx.Err(); err != nil {
			flushOutput()
			return err
		}

		// Already-local: TTL bump only.
		if seg.State == catalog.StateRehydrated && seg.LocalPath.Valid && fileExists(seg.LocalPath.String) {
			if !persistent {
				if err := s.catalog.SetEvictAfter(ctx, seg.ID, evict); err != nil {
					flushOutput()
					return fmt.Errorf("segment %s evict_after: %w", seg.ID, err)
				}
			} else {
				if err := s.catalog.SetEvictAfter(ctx, seg.ID, nil); err != nil {
					flushOutput()
					return fmt.Errorf("segment %s clear evict_after: %w", seg.ID, err)
				}
			}
			_ = s.jobs.UpdateProgress(ctx, jobID, jobs.Progress{Completed: i + 1, Total: len(segs)})
			continue
		}

		ref := buildSegmentRef(seg)
		res, err := s.archiveBackend.Retrieve(ctx, ref, rehydratedDir)
		stdoutAcc = appendSegmentBlock(stdoutAcc, seg.ID, res.Stdout)
		stderrAcc = appendSegmentBlock(stderrAcc, seg.ID, res.Stderr)
		if err != nil {
			flushOutput()
			if errors.Is(err, archive.ErrUnsupported) {
				return fmt.Errorf("rehydrate_unsupported")
			}
			return fmt.Errorf("segment %s: %w", seg.ID, err)
		}
		sourcesJSON := ""
		if len(res.Manifest.SourceNames) > 0 {
			if buf, err := json.Marshal(res.Manifest.SourceNames); err == nil {
				sourcesJSON = string(buf)
			}
		}
		if err := s.catalog.MarkSegmentRehydrated(ctx, seg.ID, catalog.RehydrateUpdate{
			LocalPath:      res.ParquetPath,
			RowCount:       res.Manifest.RowCount,
			SizeBytes:      res.Manifest.SizeBytes,
			SizeCompressed: res.Manifest.SizeCompressed,
			ParquetSHA256:  res.Manifest.ParquetSHA256,
			SourceNames:    sourcesJSON,
			EvictAfter:     evict,
		}); err != nil {
			flushOutput()
			return fmt.Errorf("segment %s catalog update: %w", seg.ID, err)
		}
		_ = s.jobs.UpdateProgress(ctx, jobID, jobs.Progress{Completed: i + 1, Total: len(segs)})
	}
	flushOutput()
	return nil
}

func buildSegmentRef(seg catalog.Segment) archive.SegmentRef {
	parquetSHA := ""
	if seg.ParquetSHA256.Valid {
		parquetSHA = seg.ParquetSHA256.String
	}
	return archive.SegmentRef{
		ID:            seg.ID,
		TimeStart:     seg.TimeStart,
		TimeEnd:       seg.TimeEnd,
		RowCount:      seg.RowCount,
		SizeBytes:     seg.SizeBytes,
		ParquetPath:   nullString(seg.LocalPath),
		ParquetSHA256: parquetSHA,
		SourceNames:   decodeSourceNames(seg.SourceNames),
	}
}

func nullString(n sql.NullString) string {
	if !n.Valid {
		return ""
	}
	return n.String
}

func fileExists(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

// handleDeleteRehydrated removes a rehydrated segment from local disk
// (PRD §13.11). The archive copy is untouched; the segment transitions
// back to `archived` and remains queryable via /api/rehydrate.
func (s *Server) handleDeleteRehydrated(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	seg, err := s.catalog.GetSegment(r.Context(), id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "not_found", "segment not found", nil)
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}
	if seg.State != catalog.StateRehydrated {
		writeError(w, http.StatusNotFound, "not_found",
			fmt.Sprintf("segment %s is not rehydrated (state=%s)", id, seg.State), nil)
		return
	}
	if s.executor != nil && s.executor.IsSegmentBusy(id) {
		writeError(w, http.StatusConflict, "segment_busy",
			"segment has an in-flight query", nil)
		return
	}
	if err := evictRehydratedSegment(r.Context(), s.catalog, seg); err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// evictRehydratedSegment is the shared eviction body used by the DELETE
// handler and the TTL eviction loop. Removes the local file (and the
// matching manifest), then clears the catalog's local_path / evict_after
// and transitions state back to archived when an archive copy still
// exists, or to lost when no archive ref is recorded.
func evictRehydratedSegment(ctx context.Context, cat *catalog.Catalog, seg catalog.Segment) error {
	if seg.LocalPath.Valid && seg.LocalPath.String != "" {
		if err := os.Remove(seg.LocalPath.String); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("removing %s: %w", seg.LocalPath.String, err)
		}
		manifestPath := filepath.Join(filepath.Dir(seg.LocalPath.String), segments.ManifestFilename(seg.ID))
		if err := os.Remove(manifestPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("removing %s: %w", manifestPath, err)
		}
	}
	hasArchive := (seg.S3URL.Valid && seg.S3URL.String != "") ||
		(seg.ArchiveRef.Valid && seg.ArchiveRef.String != "")
	next := catalog.StateArchived
	if !hasArchive {
		next = catalog.StateLost
	}
	return cat.ClearLocalFile(ctx, seg.ID, next)
}
