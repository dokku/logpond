package api

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dokku/logpond/internal/catalog"
)

// handleListSegments backs GET /api/segments (PRD §13.8). Query
// parameters: state (repeatable), from, to, source, limit, offset.
func (s *Server) handleListSegments(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	states := map[string]struct{}{}
	for _, v := range q["state"] {
		if v == "" {
			continue
		}
		states[v] = struct{}{}
	}

	var from, to time.Time
	if v := q.Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_from", "from must be RFC3339", nil)
			return
		}
		from = t.UTC()
	}
	if v := q.Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_to", "to must be RFC3339", nil)
			return
		}
		to = t.UTC()
	}

	sourceFilter := q.Get("source")

	limit := 100
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "invalid_limit", "limit must be a positive integer", nil)
			return
		}
		if n > 1000 {
			n = 1000
		}
		limit = n
	}

	offset := 0
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "invalid_offset", "offset must be a non-negative integer", nil)
			return
		}
		offset = n
	}

	all, err := s.catalog.ListSegments(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}

	filtered := make([]catalog.Segment, 0, len(all))
	for _, seg := range all {
		if len(states) > 0 {
			if _, ok := states[seg.State]; !ok {
				continue
			}
		}
		if !from.IsZero() && seg.TimeEnd.Before(from) {
			continue
		}
		if !to.IsZero() && seg.TimeStart.After(to) {
			continue
		}
		if sourceFilter != "" {
			if !sourceMatches(seg.SourceNames.String, sourceFilter) {
				continue
			}
		}
		filtered = append(filtered, seg)
	}

	sort.SliceStable(filtered, func(i, j int) bool {
		return filtered[i].TimeStart.After(filtered[j].TimeStart)
	})

	total := len(filtered)
	end := offset + limit
	if offset > total {
		offset = total
	}
	if end > total {
		end = total
	}
	page := filtered[offset:end]

	out := segmentsResponse{
		Segments: make([]segmentView, 0, len(page)),
		Total:    total,
		Limit:    limit,
		Offset:   offset,
	}
	for _, seg := range page {
		out.Segments = append(out.Segments, viewSegment(seg))
	}

	writeJSON(w, http.StatusOK, out)
}

func sourceMatches(stored, want string) bool {
	if stored == "" {
		return false
	}
	for _, name := range strings.Split(stored, ",") {
		if strings.TrimSpace(name) == want {
			return true
		}
	}
	return false
}

type segmentsResponse struct {
	Segments []segmentView `json:"segments"`
	Total    int           `json:"total"`
	Limit    int           `json:"limit"`
	Offset   int           `json:"offset"`
}

type segmentView struct {
	ID             string   `json:"id"`
	State          string   `json:"state"`
	TimeStart      string   `json:"time_start"`
	TimeEnd        string   `json:"time_end"`
	RowCount       int64    `json:"row_count"`
	SizeBytes      int64    `json:"size_bytes"`
	SizeCompressed *int64   `json:"size_compressed"`
	LocalPath      *string  `json:"local_path"`
	S3URL          *string  `json:"s3_url"`
	ArchiveRef     *string  `json:"archive_ref"`
	SourceNames    []string `json:"source_names"`
	SealedAt       *string  `json:"sealed_at"`
	ArchivedAt     *string  `json:"archived_at"`
	RehydratedAt   *string  `json:"rehydrated_at"`
	EvictAfter     *string  `json:"evict_after"`
}

func viewSegment(s catalog.Segment) segmentView {
	v := segmentView{
		ID:        s.ID,
		State:     s.State,
		TimeStart: s.TimeStart.UTC().Format(time.RFC3339),
		TimeEnd:   s.TimeEnd.UTC().Format(time.RFC3339),
		RowCount:  s.RowCount,
		SizeBytes: s.SizeBytes,
	}
	if s.SizeCompressed.Valid {
		n := s.SizeCompressed.Int64
		v.SizeCompressed = &n
	}
	if s.LocalPath.Valid {
		p := s.LocalPath.String
		v.LocalPath = &p
	}
	if s.S3URL.Valid {
		p := s.S3URL.String
		v.S3URL = &p
	}
	if s.ArchiveRef.Valid {
		p := s.ArchiveRef.String
		v.ArchiveRef = &p
	}
	if s.SourceNames.Valid && s.SourceNames.String != "" {
		parts := strings.Split(s.SourceNames.String, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		v.SourceNames = out
	}
	if s.SealedAt.Valid {
		t := s.SealedAt.Time.UTC().Format(time.RFC3339)
		v.SealedAt = &t
	}
	if s.ArchivedAt.Valid {
		t := s.ArchivedAt.Time.UTC().Format(time.RFC3339)
		v.ArchivedAt = &t
	}
	if s.RehydratedAt.Valid {
		t := s.RehydratedAt.Time.UTC().Format(time.RFC3339)
		v.RehydratedAt = &t
	}
	if s.EvictAfter.Valid {
		t := s.EvictAfter.Time.UTC().Format(time.RFC3339)
		v.EvictAfter = &t
	}
	return v
}

