package ui

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"time"
)

// staticHandler serves embedded assets with ETag + Cache-Control headers.
// Assets under /static/vendor/ are dependency-pinned (htmx, alpine,
// open-props) and get a long max-age; Logpond's own CSS/JS get a short
// max-age so dev edits show up immediately. Both paths use ETag so
// repeat requests skip the body entirely.
type staticServer struct {
	fs     fs.FS
	logger *slog.Logger
	etags  map[string]string
}

func newStaticHandler(f fs.FS, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	s := &staticServer{fs: f, logger: logger, etags: map[string]string{}}
	s.computeEtags()
	return s
}

func (s *staticServer) computeEtags() {
	_ = fs.WalkDir(s.fs, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		f, openErr := s.fs.Open(p)
		if openErr != nil {
			return nil
		}
		defer f.Close()
		h := sha256.New()
		if _, copyErr := io.Copy(h, f); copyErr != nil {
			return nil
		}
		s.etags[p] = `"` + hex.EncodeToString(h.Sum(nil))[:16] + `"`
		return nil
	})
}

func (s *staticServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	clean := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if clean == "" || strings.HasPrefix(clean, "..") {
		http.NotFound(w, r)
		return
	}

	f, err := s.fs.Open(clean)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil || info.IsDir() {
		http.NotFound(w, r)
		return
	}

	etag := s.etags[clean]
	if etag != "" {
		w.Header().Set("ETag", etag)
		if match := r.Header.Get("If-None-Match"); match != "" && strings.Contains(match, etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// Vendor assets are pinned by URL — safe to cache aggressively.
	// First-party assets (css/, js/) are short-cached so edits land fast.
	if strings.HasPrefix(clean, "vendor/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "public, max-age=300")
	}

	w.Header().Set("Content-Type", contentTypeFor(clean))
	rs, ok := f.(io.ReadSeeker)
	if !ok {
		// embed.FS files implement Seek; this branch is for tests with
		// in-memory FS that don't.
		http.ServeContent(w, r, clean, time.Time{}, readSeekerFallback(f))
		return
	}
	http.ServeContent(w, r, clean, time.Time{}, rs)
}

func contentTypeFor(p string) string {
	switch {
	case strings.HasSuffix(p, ".css"):
		return "text/css; charset=utf-8"
	case strings.HasSuffix(p, ".js"):
		return "application/javascript; charset=utf-8"
	case strings.HasSuffix(p, ".svg"):
		return "image/svg+xml"
	case strings.HasSuffix(p, ".html"):
		return "text/html; charset=utf-8"
	case strings.HasSuffix(p, ".json"):
		return "application/json"
	}
	return "application/octet-stream"
}

// readSeekerFallback buffers an fs.File into a bytes.Reader so
// http.ServeContent's Seek semantics work. embed.FS files already
// satisfy io.ReadSeeker; this exists for unit tests using a fstest.MapFS.
func readSeekerFallback(f fs.File) io.ReadSeeker {
	buf, _ := io.ReadAll(f)
	return &bytesSeeker{data: buf}
}

type bytesSeeker struct {
	data []byte
	pos  int64
}

func (b *bytesSeeker) Read(p []byte) (int, error) {
	if b.pos >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.pos:])
	b.pos += int64(n)
	return n, nil
}

func (b *bytesSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		b.pos = offset
	case io.SeekCurrent:
		b.pos += offset
	case io.SeekEnd:
		b.pos = int64(len(b.data)) + offset
	}
	return b.pos, nil
}
