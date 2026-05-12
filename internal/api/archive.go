package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/dokku/logpond/internal/archive"
	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/config"
	"github.com/dokku/logpond/internal/jobs"
)

type archiveRequest struct {
	SegmentID string `json:"segment_id,omitempty"`
	OlderThan string `json:"older_than,omitempty"`
}

type archiveResponse struct {
	JobID     string   `json:"job_id"`
	Segments  []string `json:"segments"`
	StatusURL string   `json:"status_url"`
}

func (s *Server) handleArchive(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if s.archiveBackend == nil || s.archiveBackend.Capabilities().Name == "none" {
		writeError(w, http.StatusNotImplemented, "archive_unsupported",
			"archive backend is set to 'none'", nil)
		return
	}
	caps := s.archiveBackend.Capabilities()
	if !caps.Archive {
		writeError(w, http.StatusServiceUnavailable, caps.Name+"_unavailable",
			"backend does not support archive", nil)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error(), nil)
		return
	}
	var req archiveRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error(), nil)
			return
		}
	}

	hasID := strings.TrimSpace(req.SegmentID) != ""
	hasOlder := strings.TrimSpace(req.OlderThan) != ""
	if hasID == hasOlder {
		writeError(w, http.StatusBadRequest, "bad_request",
			"exactly one of segment_id or older_than is required", nil)
		return
	}

	segs, err := s.selectArchiveCandidates(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error(), nil)
		return
	}
	if len(segs) == 0 {
		writeError(w, http.StatusNotFound, "not_found", "no matching segments", nil)
		return
	}

	if s.jobs == nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "jobs manager not configured", nil)
		return
	}

	jobID, err := s.jobs.Run(r.Context(), jobs.CreatePayload{
		Type:       jobs.TypeArchive,
		Payload:    map[string]any{"segments": segIDs(segs)},
		TotalItems: len(segs),
	}, func(ctx context.Context, id string) error {
		return s.runArchiveJob(ctx, id, segs)
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}

	writeJSON(w, http.StatusAccepted, archiveResponse{
		JobID:     jobID,
		Segments:  segIDs(segs),
		StatusURL: "/api/jobs/" + jobID,
	})
}

func segIDs(segs []catalog.Segment) []string {
	out := make([]string, 0, len(segs))
	for _, s := range segs {
		out = append(out, s.ID)
	}
	return out
}

func (s *Server) selectArchiveCandidates(ctx context.Context, req archiveRequest) ([]catalog.Segment, error) {
	if req.SegmentID != "" {
		seg, err := s.catalog.GetSegment(ctx, req.SegmentID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil
			}
			return nil, err
		}
		if seg.State != catalog.StateSealed {
			return nil, fmt.Errorf("segment %s is not sealed (state=%s)", seg.ID, seg.State)
		}
		if !seg.LocalPath.Valid || seg.LocalPath.String == "" {
			return nil, fmt.Errorf("segment %s has no local file", seg.ID)
		}
		return []catalog.Segment{seg}, nil
	}
	d, err := config.ParseDuration(req.OlderThan)
	if err != nil {
		return nil, fmt.Errorf("invalid older_than: %w", err)
	}
	if d <= 0 {
		return nil, fmt.Errorf("older_than must be positive")
	}
	cutoff := time.Now().UTC().Add(-d)
	sealed, err := s.catalog.ListSegmentsByState(ctx, catalog.StateSealed)
	if err != nil {
		return nil, err
	}
	out := []catalog.Segment{}
	for _, seg := range sealed {
		if !seg.LocalPath.Valid || seg.LocalPath.String == "" {
			continue
		}
		if seg.TimeEnd.Before(cutoff) {
			out = append(out, seg)
		}
	}
	return out, nil
}

func (s *Server) runArchiveJob(ctx context.Context, jobID string, segs []catalog.Segment) error {
	for i, seg := range segs {
		if err := ctx.Err(); err != nil {
			return err
		}
		parquetSHA := ""
		if seg.ParquetSHA256.Valid {
			parquetSHA = seg.ParquetSHA256.String
		}
		sources := decodeSourceNames(seg.SourceNames)
		ref := archive.SegmentRef{
			ID:            seg.ID,
			TimeStart:     seg.TimeStart,
			TimeEnd:       seg.TimeEnd,
			RowCount:      seg.RowCount,
			SizeBytes:     seg.SizeBytes,
			ParquetPath:   seg.LocalPath.String,
			ParquetSHA256: parquetSHA,
			SourceNames:   sources,
		}
		res, err := s.archiveBackend.Archive(ctx, ref)
		if err != nil {
			return fmt.Errorf("segment %s: %w", seg.ID, err)
		}
		if err := s.catalog.MarkSegmentArchived(ctx, seg.ID, catalog.ArchiveUpdate{
			S3URL:          res.S3URL,
			ArchiveRef:     res.ArchiveRef,
			ManifestSHA256: res.ManifestSHA256,
			ParquetSHA256:  res.ParquetSHA256,
		}); err != nil {
			return fmt.Errorf("segment %s catalog update: %w", seg.ID, err)
		}
		_ = s.jobs.UpdateProgress(ctx, jobID, jobs.Progress{
			Completed: i + 1,
			Total:     len(segs),
		})
	}
	return nil
}

func decodeSourceNames(ns sql.NullString) []string {
	if !ns.Valid || ns.String == "" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(ns.String), &out); err != nil {
		return nil
	}
	return out
}

type verifyRequest struct {
	SegmentIDs []string `json:"segment_ids,omitempty"`
}

func (s *Server) handleArchiveVerify(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if s.archiveBackend == nil || s.archiveBackend.Capabilities().Name == "none" {
		writeError(w, http.StatusNotImplemented, "archive_unsupported",
			"archive backend is set to 'none'", nil)
		return
	}
	caps := s.archiveBackend.Capabilities()
	if !caps.Verify {
		writeError(w, http.StatusNotImplemented, "verify_unsupported",
			"backend does not support verify", nil)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error(), nil)
		return
	}
	var req verifyRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error(), nil)
			return
		}
	}

	result, err := s.archiveBackend.Verify(r.Context(), req.SegmentIDs)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, caps.Name+"_unavailable", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if s.jobs == nil {
		writeError(w, http.StatusServiceUnavailable, "jobs_unavailable",
			"jobs manager not configured", nil)
		return
	}
	view, err := s.jobs.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "not_found", "job not found", nil)
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, view)
}
