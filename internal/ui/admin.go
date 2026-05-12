package ui

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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dokku/logpond/internal/archive"
	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/facets"
	"github.com/dokku/logpond/internal/jobs"
	"github.com/dokku/logpond/internal/segments"
	"github.com/go-chi/chi/v5"
)

// buildArchiveView shapes the archive pane for either the S3 or script
// variants. Live capability state comes from the backend's probe cache.
func (s *Server) buildArchiveView() archiveView {
	v := archiveView{
		Kind:   s.archiveInfo.Kind,
		Detail: s.archiveInfo.Detail,
	}
	switch s.archiveInfo.Kind {
	case "s3":
		v.S3 = &archiveS3View{
			Endpoint: s.archiveInfo.Endpoint,
			Bucket:   s.archiveInfo.Bucket,
			Prefix:   s.archiveInfo.Prefix,
		}
	case "script":
		v.Script = &archiveScriptView{
			Path:    s.archiveInfo.Path,
			Timeout: s.archiveInfo.Timeout,
		}
	}
	if s.archiveBackend != nil {
		d := s.archiveBackend.CapabilityDetail()
		v.Capabilities = archiveCapsView{
			Archive:  d.Archive,
			Retrieve: d.Retrieve,
			Verify:   d.Verify,
		}
	} else {
		v.Capabilities = archiveCapsView{Archive: "no", Retrieve: "no", Verify: "no"}
	}
	cached := s.snapshotAdmin()
	v.LastVerify = cached.lastVerify
	v.LastTest = cached.lastTest
	return v
}

// buildFacetTable mirrors the facets registry into the table rows the
// admin pane renders, with editability flags so the template doesn't
// have to re-read the facets package's constants.
func (s *Server) buildFacetTable() facetTableView {
	t := facetTableView{}
	if s.facets == nil {
		return t
	}
	for _, d := range s.facets.List() {
		row := facetRowView{
			Name:           d.Name,
			Field:          d.Field,
			DisplayLabel:   d.DisplayLabel,
			CardinalityCap: d.CardinalityCap,
			ValueType:      d.ValueType,
			Kind:           string(d.Kind),
			Source:         string(d.Source),
		}
		switch {
		case d.Kind == facets.KindBuiltin:
			row.CapEditable = true
		case d.Source == facets.SourceUI:
			row.Editable = true
		}
		t.Items = append(t.Items, row)
	}
	return t
}

// computeStorage sums catalog rows into per-state buckets. The active
// segment's size isn't tracked in the catalog yet (it's the DuckDB
// file's growing on-disk size), so we leave its Count at the number of
// active rows and SizeBytes at zero with a "—" display.
func (s *Server) computeStorage(r *http.Request) storageView {
	v := storageView{MaxSize: s.retentionInfo.MaxSize, UpdatedAt: nowUTC().Format("15:04:05Z")}
	if s.catalog == nil {
		return v
	}
	segs, err := s.catalog.ListSegments(r.Context())
	if err != nil {
		s.logger.Warn("admin: list segments for storage", "err", err)
		return v
	}
	for _, seg := range segs {
		switch seg.State {
		case catalog.StateActive:
			v.Active.Count++
		case catalog.StateSealed:
			v.Sealed.Count++
			v.Sealed.SizeBytes += seg.SizeBytes
		case catalog.StateRehydrated:
			v.Rehydrated.Count++
			v.Rehydrated.SizeBytes += seg.SizeBytes
		case catalog.StateArchived:
			v.Archived.Count++
			if seg.LocalPath.Valid && seg.LocalPath.String != "" {
				v.Archived.SizeBytes += seg.SizeBytes
			}
		}
	}
	v.Sealed.SizeDisplay = humanBytes(v.Sealed.SizeBytes)
	v.Rehydrated.SizeDisplay = humanBytes(v.Rehydrated.SizeBytes)
	v.Archived.SizeDisplay = humanBytes(v.Archived.SizeBytes)
	v.TotalLocal.Count = v.Sealed.Count + v.Rehydrated.Count + v.Active.Count
	v.TotalLocal.SizeBytes = v.Sealed.SizeBytes + v.Rehydrated.SizeBytes + v.Archived.SizeBytes
	v.TotalLocal.SizeDisplay = humanBytes(v.TotalLocal.SizeBytes)
	v.Active.SizeDisplay = ""
	return v
}

// buildSegmentsTable returns rows scoped to the optional state filter,
// applying a deterministic newest-first order so paging is stable.
func (s *Server) buildSegmentsTable(r *http.Request, state string, offset, limit int) segmentsTableView {
	t := segmentsTableView{
		State:  state,
		Offset: offset,
		Limit:  limit,
		States: []string{"", catalog.StateActive, catalog.StateSealed, catalog.StateArchived, catalog.StateRehydrated, catalog.StateLost},
	}
	if s.catalog == nil {
		return t
	}
	segs, err := s.catalog.ListSegments(r.Context())
	if err != nil {
		s.logger.Warn("admin: list segments", "err", err)
		return t
	}
	if state != "" {
		filtered := segs[:0]
		for _, seg := range segs {
			if seg.State == state {
				filtered = append(filtered, seg)
			}
		}
		segs = filtered
	}
	t.Total = len(segs)
	if offset > len(segs) {
		offset = len(segs)
	}
	end := offset + limit
	if end > len(segs) {
		end = len(segs)
	}
	page := segs[offset:end]
	for _, seg := range page {
		t.Items = append(t.Items, toSegmentRow(seg))
	}
	t.HasPrev = offset > 0
	t.HasNext = end < len(segs)
	if t.HasPrev {
		prev := offset - limit
		if prev < 0 {
			prev = 0
		}
		t.PrevURL = fmt.Sprintf("/ui/admin/segments?state=%s&offset=%d&limit=%d", state, prev, limit)
	}
	if t.HasNext {
		t.NextURL = fmt.Sprintf("/ui/admin/segments?state=%s&offset=%d&limit=%d", state, end, limit)
	}
	return t
}

func toSegmentRow(seg catalog.Segment) segmentRowView {
	row := segmentRowView{
		ID:          seg.ID,
		State:       seg.State,
		StateLabel:  seg.State,
		TimeStart:   seg.TimeStart.UTC().Format("15:04"),
		TimeEnd:     seg.TimeEnd.UTC().Format("15:04"),
		Rows:        seg.RowCount,
		RowsDisplay: humanCount(seg.RowCount),
		SizeBytes:   seg.SizeBytes,
		SizeDisplay: humanBytes(seg.SizeBytes),
	}
	row.TimeRange = fmt.Sprintf("%s–%s", row.TimeStart, row.TimeEnd)
	if names := decodeSourceList(seg.SourceNames); len(names) > 0 {
		row.Sources = names
	}
	switch seg.State {
	case catalog.StateActive:
		row.IsCurrent = true
		row.Action = segmentActionView{Kind: "none", Tooltip: "current segment"}
	case catalog.StateSealed:
		if seg.LocalPath.Valid {
			row.Action = segmentActionView{Kind: "archive"}
		} else {
			row.Action = segmentActionView{Kind: "none", Tooltip: "no local file"}
		}
	case catalog.StateArchived:
		if seg.LocalPath.Valid {
			row.Action = segmentActionView{Kind: "none", Tooltip: "archived; local copy retained"}
		} else {
			row.Action = segmentActionView{Kind: "rehydrate"}
		}
	case catalog.StateRehydrated:
		row.Action = segmentActionView{Kind: "evict"}
	case catalog.StateLost:
		row.Action = segmentActionView{Kind: "none", Tooltip: "file lost"}
	default:
		row.Action = segmentActionView{Kind: "none"}
	}
	return row
}

func decodeSourceList(ns sql.NullString) []string {
	if !ns.Valid || ns.String == "" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(ns.String), &out); err == nil {
		return out
	}
	return nil
}

// handleAdminRetentionRun runs (or dry-runs) the retention policy and
// renders the result fragment back into the admin pane.
func (s *Server) handleAdminRetentionRun(w http.ResponseWriter, r *http.Request) {
	if s.retention == nil {
		s.renderRetentionResult(w, &retentionResultView{Error: "retention evaluator not configured"})
		return
	}
	dryRun := r.URL.Query().Get("dry_run") == "true"
	res, err := s.retention.Run(r.Context(), dryRun)
	view := &retentionResultView{DryRun: dryRun, Evaluated: res.Evaluated}
	if err != nil {
		view.Error = err.Error()
		s.saveRetentionResult(view)
		s.renderRetentionResult(w, view)
		return
	}
	for _, a := range res.Actions {
		view.Actions = append(view.Actions, retentionActionView{
			SegmentID: a.SegmentID,
			Action:    a.Action,
			Reason:    a.Reason,
			Executed:  a.Executed,
			Error:     a.Error,
		})
	}
	s.saveRetentionResult(view)
	s.renderRetentionResult(w, view)
}

func (s *Server) renderRetentionResult(w http.ResponseWriter, v *retentionResultView) {
	s.renderHTML(w, "fragments/admin_retention_result.html", v, http.StatusOK)
}

// handleAdminArchiveVerify invokes the backend's verify operation and
// renders the matching fragment.
func (s *Server) handleAdminArchiveVerify(w http.ResponseWriter, r *http.Request) {
	view := &verifyResultView{}
	if s.archiveBackend == nil {
		view.Error = "archive backend not configured"
		s.saveVerifyResult(view)
		s.renderHTML(w, "fragments/admin_verify_result.html", view, http.StatusOK)
		return
	}
	caps := s.archiveBackend.Capabilities()
	if !caps.Verify {
		view.Error = "backend does not support verify"
		s.saveVerifyResult(view)
		s.renderHTML(w, "fragments/admin_verify_result.html", view, http.StatusOK)
		return
	}
	res, err := s.archiveBackend.Verify(r.Context(), nil)
	if err != nil {
		view.Error = err.Error()
		s.saveVerifyResult(view)
		s.renderHTML(w, "fragments/admin_verify_result.html", view, http.StatusOK)
		return
	}
	view.Backend = res.Backend
	view.Scanned = res.Scanned
	view.Verified = res.Verified
	view.Orphaned = res.OrphanedParquets
	view.Dangling = res.DanglingManifests
	view.Missing = res.MissingParquets
	for _, f := range res.Failed {
		view.Failed = append(view.Failed, verifyFailedView{
			SegmentID: f.SegmentID, ExitCode: f.ExitCode, Note: f.Note,
		})
	}
	s.saveVerifyResult(view)
	s.renderHTML(w, "fragments/admin_verify_result.html", view, http.StatusOK)
}

// handleAdminArchiveTest re-runs the script backend's --probe and
// surfaces the updated capability state. See IMPLEMENTATION-NOTES Phase
// 13 for why we deferred the synthetic-segment variant.
func (s *Server) handleAdminArchiveTest(w http.ResponseWriter, r *http.Request) {
	view := &testInvocationView{}
	if s.archiveBackend == nil {
		view.Error = "archive backend not configured"
		s.saveTestResult(view)
		s.renderHTML(w, "fragments/admin_test_result.html", view, http.StatusOK)
		return
	}
	probe, ok := s.archiveBackend.(interface {
		Probe(ctx context.Context) error
	})
	if !ok {
		view.Error = "active backend does not support probe (use S3 verify)"
		s.saveTestResult(view)
		s.renderHTML(w, "fragments/admin_test_result.html", view, http.StatusOK)
		return
	}
	if err := probe.Probe(r.Context()); err != nil {
		view.Error = err.Error()
	}
	d := s.archiveBackend.CapabilityDetail()
	view.Capabilities = archiveCapsView{
		Archive: d.Archive, Retrieve: d.Retrieve, Verify: d.Verify,
	}
	s.saveTestResult(view)
	s.renderHTML(w, "fragments/admin_test_result.html", view, http.StatusOK)
}

func (s *Server) handleAdminFacetsTable(w http.ResponseWriter, r *http.Request) {
	s.renderHTML(w, "fragments/admin_facet_table.html", s.buildFacetTable(), http.StatusOK)
}

func (s *Server) handleAdminFacetCreate(w http.ResponseWriter, r *http.Request) {
	if s.facets == nil {
		http.Error(w, "facets registry not configured", http.StatusServiceUnavailable)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	cap, _ := strconv.Atoi(strings.TrimSpace(r.FormValue("cardinality_cap")))
	in := facets.CreateInput{
		Name:           strings.TrimSpace(r.FormValue("name")),
		Field:          strings.TrimSpace(r.FormValue("field")),
		DisplayLabel:   strings.TrimSpace(r.FormValue("display_label")),
		CardinalityCap: cap,
		ValueType:      strings.TrimSpace(r.FormValue("value_type")),
	}
	if _, err := s.facets.CreateUI(r.Context(), in); err != nil {
		s.renderFacetError(w, err)
		return
	}
	s.renderHTML(w, "fragments/admin_facet_table.html", s.buildFacetTable(), http.StatusOK)
}

func (s *Server) handleAdminFacetPatch(w http.ResponseWriter, r *http.Request) {
	if s.facets == nil {
		http.Error(w, "facets registry not configured", http.StatusServiceUnavailable)
		return
	}
	name := chi.URLParam(r, "name")
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	patch := facets.PatchInput{}
	if v := strings.TrimSpace(r.FormValue("display_label")); v != "" {
		patch.DisplayLabel = &v
	}
	if v := strings.TrimSpace(r.FormValue("cardinality_cap")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			patch.CardinalityCap = &n
		}
	}
	if _, err := s.facets.UpdateUI(r.Context(), name, patch); err != nil {
		s.renderFacetError(w, err)
		return
	}
	s.renderHTML(w, "fragments/admin_facet_table.html", s.buildFacetTable(), http.StatusOK)
}

func (s *Server) handleAdminFacetDelete(w http.ResponseWriter, r *http.Request) {
	if s.facets == nil {
		http.Error(w, "facets registry not configured", http.StatusServiceUnavailable)
		return
	}
	name := chi.URLParam(r, "name")
	if err := s.facets.DeleteUI(r.Context(), name); err != nil {
		s.renderFacetError(w, err)
		return
	}
	s.renderHTML(w, "fragments/admin_facet_table.html", s.buildFacetTable(), http.StatusOK)
}

func (s *Server) renderFacetError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, facets.ErrConflict):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, facets.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, facets.ErrInvalid):
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleAdminStorage(w http.ResponseWriter, r *http.Request) {
	s.renderHTML(w, "fragments/admin_storage.html", s.computeStorage(r), http.StatusOK)
}

func (s *Server) handleAdminSegments(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	offset := parseQueryInt(r, "offset", 0)
	limit := parseQueryInt(r, "limit", defaultSegmentsLimit)
	t := s.buildSegmentsTable(r, state, offset, limit)
	s.renderHTML(w, "fragments/admin_segments.html", t, http.StatusOK)
}

// handleAdminSegmentArchive kicks off an archive job for a single
// segment, replacing the action cell with a polling indicator.
func (s *Server) handleAdminSegmentArchive(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if s.archiveBackend == nil || !s.archiveBackend.Capabilities().Archive {
		http.Error(w, "archive backend unavailable", http.StatusServiceUnavailable)
		return
	}
	if s.jobs == nil || s.catalog == nil {
		http.Error(w, "jobs manager not configured", http.StatusInternalServerError)
		return
	}
	seg, err := s.catalog.GetSegment(r.Context(), id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "segment not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if seg.State != catalog.StateSealed || !seg.LocalPath.Valid {
		http.Error(w, "segment not eligible for archive", http.StatusConflict)
		return
	}
	jobID, err := s.jobs.Run(r.Context(), jobs.CreatePayload{
		Type: jobs.TypeArchive, Payload: map[string]any{"segments": []string{id}}, TotalItems: 1,
	}, func(ctx context.Context, jid string) error {
		return s.runSegmentArchive(ctx, jid, seg)
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderHTML(w, "fragments/admin_job_row.html", jobRowView{
		SegmentID: id, JobID: jobID, State: "running", Progress: "archiving",
	}, http.StatusOK)
}

func (s *Server) handleAdminSegmentRehydrate(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if s.archiveBackend == nil || !s.archiveBackend.Capabilities().Retrieve {
		http.Error(w, "retrieve unsupported", http.StatusServiceUnavailable)
		return
	}
	if s.jobs == nil || s.catalog == nil {
		http.Error(w, "jobs manager not configured", http.StatusInternalServerError)
		return
	}
	seg, err := s.catalog.GetSegment(r.Context(), id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "segment not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !(seg.S3URL.Valid || seg.ArchiveRef.Valid) {
		http.Error(w, "segment has no archive copy", http.StatusConflict)
		return
	}
	jobID, err := s.jobs.Run(r.Context(), jobs.CreatePayload{
		Type: jobs.TypeRehydrate, Payload: map[string]any{"segments": []string{id}}, TotalItems: 1, TotalBytes: seg.SizeBytes,
	}, func(ctx context.Context, jid string) error {
		return s.runSegmentRehydrate(ctx, jid, seg)
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderHTML(w, "fragments/admin_job_row.html", jobRowView{
		SegmentID: id, JobID: jobID, State: "running", Progress: "rehydrating",
	}, http.StatusOK)
}

func (s *Server) handleAdminRehydratedDelete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if s.catalog == nil {
		http.Error(w, "catalog not configured", http.StatusInternalServerError)
		return
	}
	seg, err := s.catalog.GetSegment(r.Context(), id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "segment not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if seg.State != catalog.StateRehydrated {
		http.Error(w, "segment is not rehydrated", http.StatusConflict)
		return
	}
	if s.executor != nil && s.executor.IsSegmentBusy(id) {
		http.Error(w, "segment busy", http.StatusConflict)
		return
	}
	if err := evictRehydratedHelper(r.Context(), s.catalog, seg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Re-fetch and render the row in its new state.
	fresh, err := s.catalog.GetSegment(r.Context(), id)
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.renderHTML(w, "fragments/admin_segment_row.html", toSegmentRow(fresh), http.StatusOK)
}

// handleAdminJob renders a small fragment with the current progress of
// a job, suitable for HTMX polling at 2s intervals.
func (s *Server) handleAdminJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	segID := r.URL.Query().Get("segment_id")
	if s.jobs == nil {
		http.Error(w, "jobs unavailable", http.StatusServiceUnavailable)
		return
	}
	view, err := s.jobs.Get(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	row := jobRowView{
		SegmentID: segID,
		JobID:     id,
		State:     view.State,
		Done:      view.State == "completed",
		Failed:    view.State == "failed",
	}
	if view.Progress.Total > 0 {
		row.Progress = fmt.Sprintf("%d / %d", view.Progress.Completed, view.Progress.Total)
	} else {
		row.Progress = "running"
	}
	if view.Error != nil {
		if m, ok := view.Error.(map[string]any); ok {
			if msg, ok := m["message"].(string); ok {
				row.Error = msg
			}
		}
		if row.Error == "" {
			row.Error = fmt.Sprintf("%v", view.Error)
		}
	}
	// On completion, swap back to the refreshed segment row so the
	// table reflects the new state without a full page reload.
	if row.Done && segID != "" && s.catalog != nil {
		if seg, err := s.catalog.GetSegment(r.Context(), segID); err == nil {
			s.renderHTML(w, "fragments/admin_segment_row.html", toSegmentRow(seg), http.StatusOK)
			return
		}
	}
	s.renderHTML(w, "fragments/admin_job_row.html", row, http.StatusOK)
}

func (s *Server) handleAdminImport(w http.ResponseWriter, r *http.Request) {
	view := importResultView{}
	if s.catalog == nil || s.dataDir == "" {
		view.Error = "import not configured"
		s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
		return
	}
	persistent := r.URL.Query().Get("persistent") == "true"
	r.Body = http.MaxBytesReader(w, r.Body, 5<<30)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		view.Error = err.Error()
		s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
		return
	}
	rehydratedDir := filepath.Join(s.dataDir, "rehydrated")
	if err := os.MkdirAll(rehydratedDir, 0o755); err != nil {
		view.Error = err.Error()
		s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
		return
	}
	stage, err := os.MkdirTemp(rehydratedDir, ".import-")
	if err != nil {
		view.Error = err.Error()
		s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
		return
	}
	defer os.RemoveAll(stage)

	parquetTmp := filepath.Join(stage, "parquet")
	manifestTmp := filepath.Join(stage, "manifest.json")
	if err := saveFormFile(r, "parquet", parquetTmp); err != nil {
		view.Error = "parquet: " + err.Error()
		s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
		return
	}
	if err := saveFormFile(r, "manifest", manifestTmp); err != nil {
		view.Error = "manifest: " + err.Error()
		s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
		return
	}
	manifest, err := segments.ReadManifestFile(manifestTmp)
	if err != nil {
		view.Error = "manifest invalid: " + err.Error()
		s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
		return
	}
	sha, _, err := segments.ParquetSHA256(parquetTmp)
	if err != nil {
		view.Error = err.Error()
		s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
		return
	}
	if !strings.EqualFold(sha, manifest.ParquetSHA256) {
		view.Error = fmt.Sprintf("parquet sha mismatch: got %s want %s", sha, manifest.ParquetSHA256)
		s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
		return
	}
	if _, err := s.catalog.GetSegment(r.Context(), manifest.SegmentID); err == nil {
		view.Error = "segment already exists: " + manifest.SegmentID
		s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
		return
	} else if !errors.Is(err, sql.ErrNoRows) {
		view.Error = err.Error()
		s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
		return
	}
	overlapping, _ := s.catalog.SegmentsOverlapping(r.Context(), manifest.TimeStart, manifest.TimeEnd)
	for _, o := range overlapping {
		if o.ID == manifest.SegmentID {
			continue
		}
		view.Overlaps = append(view.Overlaps, importOverlapView{
			SegmentID: o.ID,
			TimeRange: o.TimeStart.UTC().Format("15:04") + "–" + o.TimeEnd.UTC().Format("15:04"),
		})
	}
	parquetFinal := filepath.Join(rehydratedDir, segments.ParquetFilename(manifest.SegmentID))
	manifestFinal := filepath.Join(rehydratedDir, segments.ManifestFilename(manifest.SegmentID))
	if err := os.Rename(parquetTmp, parquetFinal); err != nil {
		view.Error = err.Error()
		s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
		return
	}
	if err := os.Rename(manifestTmp, manifestFinal); err != nil {
		_ = os.Remove(parquetFinal)
		view.Error = err.Error()
		s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
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
		view.Error = err.Error()
		s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
		return
	}
	view.OK = true
	view.SegmentID = manifest.SegmentID
	view.State = catalog.StateRehydrated
	view.Rows = manifest.RowCount
	view.SizeBytes = manifest.SizeBytes
	view.Persistent = importPersistent
	if evict != nil {
		view.EvictAfter = evict.Format(time.RFC3339)
	}
	s.renderHTML(w, "fragments/admin_import_result.html", view, http.StatusOK)
}

func saveFormFile(r *http.Request, name, dst string) error {
	f, _, err := r.FormFile(name)
	if err != nil {
		return fmt.Errorf("missing %s part: %w", name, err)
	}
	defer f.Close()
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

// evictRehydratedHelper duplicates the body of api.evictRehydratedSegment
// so the UI package doesn't import api. The two paths share semantics by
// targeting the same catalog primitive.
func evictRehydratedHelper(ctx context.Context, cat *catalog.Catalog, seg catalog.Segment) error {
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

// runSegmentArchive performs the single-segment archive flow on the
// admin path. It mirrors api.runArchiveJob but accepts a single segment
// so we don't need to expose api's internals here.
func (s *Server) runSegmentArchive(ctx context.Context, jobID string, seg catalog.Segment) error {
	parquetSHA := ""
	if seg.ParquetSHA256.Valid {
		parquetSHA = seg.ParquetSHA256.String
	}
	ref := archive.SegmentRef{
		ID:            seg.ID,
		TimeStart:     seg.TimeStart,
		TimeEnd:       seg.TimeEnd,
		RowCount:      seg.RowCount,
		SizeBytes:     seg.SizeBytes,
		ParquetPath:   seg.LocalPath.String,
		ParquetSHA256: parquetSHA,
		SourceNames:   decodeSourceList(seg.SourceNames),
	}
	res, err := s.archiveBackend.Archive(ctx, ref)
	if res.Stdout != "" || res.Stderr != "" {
		_ = s.jobs.AttachScriptOutput(ctx, jobID, res.Stdout, res.Stderr)
	}
	if err != nil {
		return err
	}
	if err := s.catalog.MarkSegmentArchived(ctx, seg.ID, catalog.ArchiveUpdate{
		S3URL:          res.S3URL,
		ArchiveRef:     res.ArchiveRef,
		ManifestSHA256: res.ManifestSHA256,
		ParquetSHA256:  res.ParquetSHA256,
	}); err != nil {
		return err
	}
	return s.jobs.UpdateProgress(ctx, jobID, jobs.Progress{Completed: 1, Total: 1})
}

func (s *Server) runSegmentRehydrate(ctx context.Context, jobID string, seg catalog.Segment) error {
	rehydratedDir := filepath.Join(s.dataDir, "rehydrated")
	if err := os.MkdirAll(rehydratedDir, 0o755); err != nil {
		return err
	}
	parquetSHA := ""
	if seg.ParquetSHA256.Valid {
		parquetSHA = seg.ParquetSHA256.String
	}
	ref := archive.SegmentRef{
		ID:            seg.ID,
		TimeStart:     seg.TimeStart,
		TimeEnd:       seg.TimeEnd,
		RowCount:      seg.RowCount,
		SizeBytes:     seg.SizeBytes,
		ParquetSHA256: parquetSHA,
		SourceNames:   decodeSourceList(seg.SourceNames),
	}
	res, err := s.archiveBackend.Retrieve(ctx, ref, rehydratedDir)
	if res.Stdout != "" || res.Stderr != "" {
		_ = s.jobs.AttachScriptOutput(ctx, jobID, res.Stdout, res.Stderr)
	}
	if err != nil {
		return err
	}
	evict := time.Now().UTC().Add(s.rehydrationTTL)
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
		EvictAfter:     &evict,
	}); err != nil {
		return err
	}
	return s.jobs.UpdateProgress(ctx, jobID, jobs.Progress{Completed: 1, Total: 1, BytesDone: seg.SizeBytes, BytesTotal: seg.SizeBytes})
}

// humanBytes formats a byte count as a short string (KB/MB/GB).
func humanBytes(n int64) string {
	if n <= 0 {
		return "—"
	}
	const k = 1024
	switch {
	case n < k:
		return fmt.Sprintf("%d B", n)
	case n < k*k:
		return fmt.Sprintf("%.1f KB", float64(n)/k)
	case n < k*k*k:
		return fmt.Sprintf("%.1f MB", float64(n)/(k*k))
	default:
		return fmt.Sprintf("%.2f GB", float64(n)/(k*k*k))
	}
}

func humanCount(n int64) string {
	if n < 0 {
		return "-" + humanCount(-n)
	}
	if n < 1000 {
		return strconv.FormatInt(n, 10)
	}
	if n < 1_000_000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	if n < 1_000_000_000 {
		return fmt.Sprintf("%.2fM", float64(n)/1_000_000)
	}
	return fmt.Sprintf("%.2fB", float64(n)/1_000_000_000)
}

// SortSegmentsByTimeDesc is exported for tests that need to validate
// ordering before paginating. Production callers rely on
// catalog.ListSegments returning newest-first already.
func SortSegmentsByTimeDesc(segs []catalog.Segment) {
	sort.SliceStable(segs, func(i, j int) bool {
		return segs[i].TimeStart.After(segs[j].TimeStart)
	})
}
