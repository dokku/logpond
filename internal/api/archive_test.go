package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dokku/logpond/internal/archive"
	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/jobs"
	"github.com/dokku/logpond/internal/metrics"
)

// fakeBackend mirrors the retention test fake but specialized for the
// archive endpoint expectations.
type fakeBackend struct {
	caps     archive.Capabilities
	archived []string
	failNext bool
	verify   archive.VerifyResult
}

func (f *fakeBackend) Capabilities() archive.Capabilities {
	if f.caps.Name == "" {
		return archive.Capabilities{Archive: true, Retrieve: true, Verify: true, Name: "s3"}
	}
	return f.caps
}

func (f *fakeBackend) Archive(_ context.Context, ref archive.SegmentRef) (archive.ArchiveResult, error) {
	if f.failNext {
		f.failNext = false
		return archive.ArchiveResult{}, errors.New("injected failure")
	}
	f.archived = append(f.archived, ref.ID)
	return archive.ArchiveResult{
		S3URL:         "s3://bkt/seg/" + ref.ID + ".parquet",
		ParquetSHA256: ref.ParquetSHA256,
	}, nil
}

func (f *fakeBackend) Retrieve(context.Context, archive.SegmentRef, string) (archive.RetrieveResult, error) {
	return archive.RetrieveResult{}, archive.ErrUnsupported
}

func (f *fakeBackend) Verify(context.Context, []string) (archive.VerifyResult, error) {
	if f.verify.Backend == "" {
		return archive.VerifyResult{Backend: "s3"}, nil
	}
	return f.verify, nil
}

func newArchiveServer(t *testing.T) (*Server, *catalog.Catalog, *fakeBackend) {
	t.Helper()
	dir := t.TempDir()
	cat, err := catalog.Open(context.Background(), filepath.Join(dir, "catalog.db"), nil)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })

	jm, err := jobs.New(jobs.Options{Catalog: cat})
	if err != nil {
		t.Fatalf("jobs: %v", err)
	}
	be := &fakeBackend{}
	buf := ingest.NewBuffer(100)
	m := metrics.New(metrics.Options{FillRatio: buf.FillRatio})

	srv := New(Options{
		Buffer:         buf,
		Metrics:        m,
		Extractors:     map[string]*ingest.Extractor{},
		Catalog:        cat,
		ArchiveBackend: be,
		Jobs:           jm,
	})
	return srv, cat, be
}

func insertSealedSegment(t *testing.T, c *catalog.Catalog, id string, age time.Duration, dir string) string {
	t.Helper()
	parquet := filepath.Join(dir, "segment-"+id+".parquet")
	if err := os.WriteFile(parquet, []byte("payload-"+id), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	now := time.Now().UTC()
	start := now.Add(-age - time.Hour)
	end := now.Add(-age)
	seg := catalog.Segment{
		ID:            id,
		State:         catalog.StateSealed,
		TimeStart:     start,
		TimeEnd:       end,
		RowCount:      1,
		SizeBytes:     int64(len("payload-" + id)),
		LocalPath:     sql.NullString{String: parquet, Valid: true},
		ParquetSHA256: sql.NullString{String: "abc", Valid: true},
		SourceNames:   sql.NullString{String: `["default"]`, Valid: true},
		CreatedAt:     start,
		SealedAt:      sql.NullTime{Time: end.Add(2 * time.Hour), Valid: true},
	}
	if err := c.InsertSegment(context.Background(), seg); err != nil {
		t.Fatalf("insert: %v", err)
	}
	return parquet
}

func TestArchive_BySegmentID_AcceptedAndExecutes(t *testing.T) {
	srv, cat, be := newArchiveServer(t)
	dir := t.TempDir()
	_ = insertSealedSegment(t, cat, "seg-1", 24*time.Hour, dir)

	rr := postJSON(t, srv, "/api/archive", `{"segment_id":"seg-1"}`)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp archiveResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Segments) != 1 || resp.Segments[0] != "seg-1" {
		t.Fatalf("segments: %v", resp.Segments)
	}

	// Wait for the background job to complete.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		seg, err := cat.GetSegment(context.Background(), "seg-1")
		if err == nil && seg.State == catalog.StateArchived {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(be.archived) != 1 {
		t.Fatalf("backend archived: %v", be.archived)
	}
	seg, _ := cat.GetSegment(context.Background(), "seg-1")
	if seg.State != catalog.StateArchived || !seg.S3URL.Valid {
		t.Fatalf("segment not marked archived: %+v", seg)
	}
}

func TestArchive_OlderThan_SelectsAgedSegments(t *testing.T) {
	srv, cat, be := newArchiveServer(t)
	dir := t.TempDir()
	_ = insertSealedSegment(t, cat, "old", 48*time.Hour, dir)
	_ = insertSealedSegment(t, cat, "fresh", time.Hour, dir)

	rr := postJSON(t, srv, "/api/archive", `{"older_than":"24h"}`)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp archiveResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if len(resp.Segments) != 1 || resp.Segments[0] != "old" {
		t.Fatalf("expected only 'old' selected, got %v", resp.Segments)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(be.archived) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(be.archived) != 1 {
		t.Fatalf("backend archived: %v", be.archived)
	}
}

func TestArchive_RejectsBothOrNeither(t *testing.T) {
	srv, _, _ := newArchiveServer(t)
	cases := []string{`{}`, `{"segment_id":"x","older_than":"24h"}`}
	for _, body := range cases {
		rr := postJSON(t, srv, "/api/archive", body)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("body %q status %d", body, rr.Code)
		}
	}
}

func TestArchive_UnknownSegment_404(t *testing.T) {
	srv, _, _ := newArchiveServer(t)
	rr := postJSON(t, srv, "/api/archive", `{"segment_id":"nope"}`)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestArchive_NoneBackend_501(t *testing.T) {
	srv, _, be := newArchiveServer(t)
	be.caps = archive.Capabilities{Name: "none"}
	rr := postJSON(t, srv, "/api/archive", `{"segment_id":"seg-x"}`)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status: %d", rr.Code)
	}
}

func TestArchiveVerify_PassesThroughBackend(t *testing.T) {
	srv, _, be := newArchiveServer(t)
	be.verify = archive.VerifyResult{
		Backend: "s3", Scanned: 10,
		OrphanedParquets: []string{"k1"},
	}
	rr := postJSON(t, srv, "/api/admin/archive/verify", `{}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
	var got archive.VerifyResult
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Scanned != 10 || len(got.OrphanedParquets) != 1 {
		t.Fatalf("verify body: %+v", got)
	}
}

func TestJobs_GetReturnsState(t *testing.T) {
	srv, cat, _ := newArchiveServer(t)
	dir := t.TempDir()
	_ = insertSealedSegment(t, cat, "seg-1", 24*time.Hour, dir)
	rr := postJSON(t, srv, "/api/archive", `{"segment_id":"seg-1"}`)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: %d", rr.Code)
	}
	var resp archiveResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)

	// Poll the jobs endpoint until the job completes.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		req := httptest.NewRequest(http.MethodGet, "/api/jobs/"+resp.JobID, nil)
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("status: %d body=%s", w.Code, w.Body.String())
		}
		var view jobs.JobView
		_ = json.Unmarshal(w.Body.Bytes(), &view)
		if view.State == catalog.JobStateCompleted {
			if view.Progress.Completed != 1 || view.Progress.Total != 1 {
				t.Fatalf("progress not recorded: %+v", view.Progress)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("job did not complete in time")
}

func TestJobs_UnknownID_404(t *testing.T) {
	srv, _, _ := newArchiveServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/jobs/nope", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status: %d", rr.Code)
	}
}

func postJSON(t *testing.T, srv *Server, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	return rr
}
