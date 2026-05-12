// Package archive holds the off-local-disk persistence layer for
// sealed segments. It exposes a Backend interface and ships concrete
// implementations (S3 today; script in Phase 9). Retention and the
// /api/archive handler dispatch through this package.
package archive

import (
	"context"
	"errors"
	"time"

	"github.com/dokku/logpond/internal/segments"
)

// SegmentRef carries the catalog metadata an archive backend needs to
// describe and locate a sealed segment.
type SegmentRef struct {
	ID            string
	TimeStart     time.Time
	TimeEnd       time.Time
	RowCount      int64
	SizeBytes     int64
	ParquetPath   string
	ManifestPath  string // empty if the manifest hasn't been generated yet
	ParquetSHA256 string // empty if the seal pass didn't record one
	SourceNames   []string
	Persistent    bool
}

// ArchiveResult records what the backend stored. Catalog fields are
// populated from these values via Catalog.MarkSegmentArchived.
type ArchiveResult struct {
	S3URL          string // populated by the S3 backend
	ArchiveRef     string // populated by the script backend
	ManifestSHA256 string // optional - SHA of the manifest contents
	ParquetSHA256  string // mirrored from the segment ref or recomputed
	AlreadyPresent bool   // true when the backend short-circuited an existing copy
	Manifest       segments.Manifest
}

// VerifyResult is the union type returned by Verify across backends.
// Either ScanS3 or ScanScript will be populated depending on which
// backend produced the result.
type VerifyResult struct {
	Backend  string         `json:"backend"`
	Scanned  int            `json:"scanned"`
	Verified int            `json:"verified,omitempty"`
	Failed   []VerifyFailed `json:"failed,omitempty"`

	OrphanedParquets   []string `json:"orphaned_parquets,omitempty"`
	DanglingManifests  []string `json:"dangling_manifests,omitempty"`
	MissingParquets    []string `json:"missing_parquets,omitempty"`
	MismatchedChecksum []string `json:"mismatched_checksum,omitempty"`
}

// VerifyFailed is the per-segment failure shape for the script backend.
type VerifyFailed struct {
	SegmentID string `json:"segment_id"`
	ExitCode  int    `json:"exit_code,omitempty"`
	Note      string `json:"note,omitempty"`
}

// Capabilities reports which Backend methods are usable. The script
// backend may discover modes lazily at runtime; the S3 backend supports
// all three.
type Capabilities struct {
	Archive  bool
	Retrieve bool
	Verify   bool
	Name     string // "s3", "script", or "none"
}

// Backend is the off-disk archive contract. Implementations must be
// safe for concurrent calls; serialization (one invocation at a time)
// is the caller's responsibility when required.
type Backend interface {
	Capabilities() Capabilities
	Archive(ctx context.Context, ref SegmentRef) (ArchiveResult, error)
	Retrieve(ctx context.Context, ref SegmentRef, outDir string) (RetrieveResult, error)
	Verify(ctx context.Context, segIDs []string) (VerifyResult, error)
}

// RetrieveResult is the local-side outcome of a Retrieve call.
type RetrieveResult struct {
	ParquetPath  string
	ManifestPath string
	Manifest     segments.Manifest
}

// ErrUnsupported is returned by backends for modes they don't implement
// (PRD §7.9.3 exit-code 64 maps here; S3 never returns it).
var ErrUnsupported = errors.New("archive: operation not supported by backend")

// ErrAlreadyPresent indicates the backend short-circuited a duplicate
// upload because the destination already held a copy with the expected
// SHA-256.
var ErrAlreadyPresent = errors.New("archive: already present")

// NoneBackend is the default backend when archive.backend=none. It
// reports zero capabilities and returns ErrUnsupported for every call,
// which keeps the rest of the system uniform.
type NoneBackend struct{}

// Capabilities implements Backend.
func (NoneBackend) Capabilities() Capabilities {
	return Capabilities{Name: "none"}
}

// Archive implements Backend.
func (NoneBackend) Archive(context.Context, SegmentRef) (ArchiveResult, error) {
	return ArchiveResult{}, ErrUnsupported
}

// Retrieve implements Backend.
func (NoneBackend) Retrieve(context.Context, SegmentRef, string) (RetrieveResult, error) {
	return RetrieveResult{}, ErrUnsupported
}

// Verify implements Backend.
func (NoneBackend) Verify(context.Context, []string) (VerifyResult, error) {
	return VerifyResult{Backend: "none"}, ErrUnsupported
}
