package api

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/metrics"
)

func newTestServer(t *testing.T, capacity int) (*Server, *ingest.Buffer) {
	t.Helper()
	buf := ingest.NewBuffer(capacity)
	m := metrics.New(metrics.Options{FillRatio: buf.FillRatio})
	src := ingest.SourceSpec{
		Name: "default",
		Extract: map[string][]string{
			"timestamp": {"timestamp"},
			"level":     {"level"},
			"message":   {"message"},
			"service":   {"service"},
			"host":      {"host"},
		},
	}
	srv := New(Options{
		Buffer:  buf,
		Metrics: m,
		Extractors: map[string]*ingest.Extractor{
			"default": ingest.NewExtractor(src),
		},
	})
	return srv, buf
}

func TestHealthz_ReturnsOK(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d", rr.Code)
	}
	var body struct {
		Status        string `json:"status"`
		UptimeSeconds int64  `json:"uptime_seconds"`
		Version       string `json:"version"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Status != "ok" {
		t.Errorf("status: %s", body.Status)
	}
	if body.Version == "" {
		t.Errorf("version missing")
	}
	if body.UptimeSeconds < 0 {
		t.Errorf("uptime: %d", body.UptimeSeconds)
	}
}

func TestIngest_HappyPath_202Accepted(t *testing.T) {
	srv, buf := newTestServer(t, 100)
	body := strings.Join([]string{
		`{"timestamp":"2026-05-12T14:32:01Z","level":"info","service":"api","message":"a"}`,
		`{"timestamp":"2026-05-12T14:32:02Z","level":"error","service":"api","message":"b"}`,
	}, "\n")
	rr := postIngest(srv, "default", "application/x-ndjson", "", body)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body)
	}
	var resp struct{ Accepted, Skipped int }
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Accepted != 2 || resp.Skipped != 0 {
		t.Errorf("counts: %+v", resp)
	}
	if buf.Len() != 2 {
		t.Errorf("buffer len: %d", buf.Len())
	}
}

func TestIngest_PartialBadBatch_AcceptedAndSkipped(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	body := strings.Join([]string{
		`{"timestamp":"2026-05-12T14:32:01Z","level":"info","message":"good"}`,
		`not json at all`,
		``,
		`{"timestamp":"2026-05-12T14:32:02Z","level":"info","message":"also good"}`,
	}, "\n")
	rr := postIngest(srv, "default", "application/x-ndjson", "", body)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: %d", rr.Code)
	}
	var resp struct{ Accepted, Skipped int }
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp.Accepted != 2 || resp.Skipped != 1 {
		t.Errorf("counts: %+v", resp)
	}
}

func TestIngest_UnknownSource_404(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	rr := postIngest(srv, "missing", "application/x-ndjson", "", `{"level":"info"}`)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status: %d", rr.Code)
	}
	mustErrorCode(t, rr, "unknown_source")
}

func TestIngest_UnsupportedContentType_415(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	rr := postIngest(srv, "default", "text/plain", "", "hi")
	if rr.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status: %d", rr.Code)
	}
}

func TestIngest_UnsupportedEncoding_415(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	rr := postIngest(srv, "default", "application/x-ndjson", "br", "hi")
	if rr.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status: %d", rr.Code)
	}
}

func TestIngest_GzipBody_Accepted(t *testing.T) {
	srv, buf := newTestServer(t, 100)
	plain := `{"timestamp":"2026-05-12T14:32:01Z","level":"info","message":"hello"}`
	var gz bytes.Buffer
	gw := gzip.NewWriter(&gz)
	if _, err := gw.Write([]byte(plain)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	rr := postIngestRaw(srv, "default", "application/x-ndjson", "gzip", gz.Bytes())
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body)
	}
	if buf.Len() != 1 {
		t.Errorf("buf len: %d", buf.Len())
	}
}

func TestIngest_PayloadTooLarge_413(t *testing.T) {
	srv, _ := newTestServer(t, 1024)
	// Build a body bigger than 32MB after decompression.
	big := bytes.Repeat([]byte("a"), maxDecompressedBody+10)
	rr := postIngestRaw(srv, "default", "application/x-ndjson", "", big)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status: %d", rr.Code)
	}
	mustErrorCode(t, rr, "payload_too_large")
}

func TestIngest_BufferFull_429WithRetryAfter(t *testing.T) {
	srv, buf := newTestServer(t, 1)
	// Fill the buffer first.
	if err := buf.Append([]ingest.Event{{Message: "filled"}}); err != nil {
		t.Fatalf("seed buffer: %v", err)
	}
	body := `{"timestamp":"2026-05-12T14:32:01Z","level":"info","message":"x"}`
	rr := postIngest(srv, "default", "application/x-ndjson", "", body)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("status: %d", rr.Code)
	}
	if got := rr.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After: got %q", got)
	}
	mustErrorCode(t, rr, "buffer_full")
}

func TestIngest_EmptyLinesNotCountedAsSkipped(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	body := "\n\n" + `{"timestamp":"2026-05-12T14:32:01Z","level":"info","message":"x"}` + "\n\n"
	rr := postIngest(srv, "default", "application/x-ndjson", "", body)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: %d", rr.Code)
	}
	var resp struct{ Accepted, Skipped int }
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp.Accepted != 1 || resp.Skipped != 0 {
		t.Errorf("counts: %+v", resp)
	}
}

func TestIngest_CRLFLineTerminator(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	body := `{"timestamp":"2026-05-12T14:32:01Z","level":"info","message":"a"}` + "\r\n" +
		`{"timestamp":"2026-05-12T14:32:02Z","level":"info","message":"b"}`
	rr := postIngest(srv, "default", "application/x-ndjson", "", body)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: %d", rr.Code)
	}
	var resp struct{ Accepted, Skipped int }
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp.Accepted != 2 {
		t.Errorf("accepted: %d", resp.Accepted)
	}
}

func postIngest(srv *Server, source, contentType, encoding, body string) *httptest.ResponseRecorder {
	return postIngestRaw(srv, source, contentType, encoding, []byte(body))
}

func postIngestRaw(srv *Server, source, contentType, encoding string, body []byte) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/ingest/"+source, io.NopCloser(bytes.NewReader(body)))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	srv.Handler().ServeHTTP(rr, req)
	return rr
}

func mustErrorCode(t *testing.T, rr *httptest.ResponseRecorder, want string) {
	t.Helper()
	var env errorEnvelope
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode error envelope: %v body=%s", err, rr.Body.String())
	}
	if env.Error.Code != want {
		t.Errorf("error code: got %q want %q", env.Error.Code, want)
	}
}
