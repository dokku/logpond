package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestListSegments_FiltersAndPagination(t *testing.T) {
	srv, cat, _ := newArchiveServer(t)
	dir := t.TempDir()

	_ = insertSealedSegment(t, cat, "seg-old", 48*time.Hour, dir)
	_ = insertSealedSegment(t, cat, "seg-mid", 24*time.Hour, dir)
	_ = insertSealedSegment(t, cat, "seg-new", 1*time.Hour, dir)

	t.Run("returns all segments by default", func(t *testing.T) {
		rr := getJSON(t, srv, "/api/segments")
		if rr.Code != http.StatusOK {
			t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
		}
		var resp segmentsResponse
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.Total != 3 || len(resp.Segments) != 3 {
			t.Fatalf("expected 3 segments, got total=%d len=%d", resp.Total, len(resp.Segments))
		}
		// Sorted newest first.
		if resp.Segments[0].ID != "seg-new" {
			t.Fatalf("expected newest first; got %s", resp.Segments[0].ID)
		}
	})

	t.Run("state filter narrows results", func(t *testing.T) {
		rr := getJSON(t, srv, "/api/segments?state=sealed")
		var resp segmentsResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &resp)
		if resp.Total != 3 {
			t.Fatalf("expected 3 sealed; got %d", resp.Total)
		}

		rr = getJSON(t, srv, "/api/segments?state=archived")
		_ = json.Unmarshal(rr.Body.Bytes(), &resp)
		if resp.Total != 0 {
			t.Fatalf("expected 0 archived; got %d", resp.Total)
		}
	})

	t.Run("limit paginates", func(t *testing.T) {
		rr := getJSON(t, srv, "/api/segments?limit=2")
		var resp segmentsResponse
		_ = json.Unmarshal(rr.Body.Bytes(), &resp)
		if resp.Total != 3 || len(resp.Segments) != 2 {
			t.Fatalf("expected total=3 limit=2 page=2; got total=%d page=%d", resp.Total, len(resp.Segments))
		}

		rr = getJSON(t, srv, "/api/segments?limit=2&offset=2")
		_ = json.Unmarshal(rr.Body.Bytes(), &resp)
		if len(resp.Segments) != 1 || resp.Segments[0].ID != "seg-old" {
			t.Fatalf("expected one segment seg-old; got %+v", resp.Segments)
		}
	})

	t.Run("invalid from rejects with 400", func(t *testing.T) {
		rr := getJSON(t, srv, "/api/segments?from=not-a-date")
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400; got %d", rr.Code)
		}
	})
}

func getJSON(t *testing.T, srv *Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	return rr
}
