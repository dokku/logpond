package segments

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ManifestVersion is the current manifest schema version (PRD §10.3).
const ManifestVersion = 1

// Manifest mirrors the JSON document described in PRD §10.3. It is
// produced at archive time when no manifest exists yet alongside a
// sealed Parquet, and is the commit marker for the S3 backend.
type Manifest struct {
	ManifestVersion  int       `json:"manifest_version"`
	SchemaVersion    int       `json:"schema_version"`
	SegmentID        string    `json:"segment_id"`
	TimeStart        time.Time `json:"time_start"`
	TimeEnd          time.Time `json:"time_end"`
	RowCount         int64     `json:"row_count"`
	SizeBytes        int64     `json:"size_bytes"`
	SizeCompressed   int64     `json:"size_compressed"`
	Compression      string    `json:"compression"`
	CompressionLevel int       `json:"compression_level"`
	SourceNames      []string  `json:"source_names"`
	ParquetFilename  string    `json:"parquet_filename"`
	ParquetSHA256    string    `json:"parquet_sha256"`
	Persistent       bool      `json:"persistent"`
}

// Encode renders the manifest as canonical pretty JSON. The same bytes
// are written to disk and uploaded to S3, so callers that need a SHA-256
// of the manifest can hash whatever Encode returns.
func (m Manifest) Encode() ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

// Validate enforces the invariants the rest of the codebase relies on
// when reading a manifest from disk or S3.
func (m Manifest) Validate() error {
	if m.ManifestVersion != ManifestVersion {
		return fmt.Errorf("unsupported manifest_version %d", m.ManifestVersion)
	}
	if m.SegmentID == "" {
		return fmt.Errorf("manifest missing segment_id")
	}
	if m.ParquetSHA256 == "" {
		return fmt.Errorf("manifest missing parquet_sha256")
	}
	if m.ParquetFilename == "" {
		return fmt.Errorf("manifest missing parquet_filename")
	}
	if m.TimeStart.IsZero() || m.TimeEnd.IsZero() {
		return fmt.Errorf("manifest missing time bounds")
	}
	return nil
}

// ManifestFilename returns the canonical manifest filename for a given
// segment id: segment-{id}.manifest.json.
func ManifestFilename(segmentID string) string {
	return "segment-" + segmentID + ".manifest.json"
}

// ParquetFilename returns the canonical parquet filename for a given
// segment id: segment-{id}.parquet.
func ParquetFilename(segmentID string) string {
	return "segment-" + segmentID + ".parquet"
}

// WriteManifestFile writes m to path atomically (tmp + rename + fsync).
// The directory is fsync'd too so the manifest is durable.
func WriteManifestFile(m Manifest, path string) error {
	buf, err := m.Encode()
	if err != nil {
		return fmt.Errorf("encoding manifest: %w", err)
	}
	dir := filepath.Dir(path)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", tmp, err)
	}
	if err := fsyncFile(tmp); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("fsync %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("renaming %s -> %s: %w", tmp, path, err)
	}
	if err := fsyncDir(dir); err != nil {
		return fmt.Errorf("fsync dir %s: %w", dir, err)
	}
	return nil
}

// ReadManifestFile parses the manifest at path, returning the decoded
// struct plus a validation error if the document is missing required
// fields.
func ReadManifestFile(path string) (Manifest, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("reading %s: %w", path, err)
	}
	var m Manifest
	if err := json.Unmarshal(buf, &m); err != nil {
		return Manifest{}, fmt.Errorf("parsing %s: %w", path, err)
	}
	if err := m.Validate(); err != nil {
		return Manifest{}, fmt.Errorf("%s: %w", path, err)
	}
	return m, nil
}

// BuildManifest assembles a Manifest from the segment row plus the local
// parquet path. parquetSHA, when empty, is recomputed from the file.
// The function fills sane defaults for compression/level (zstd-3, the
// sealer's setting) when the row carries no explicit value.
func BuildManifest(segmentID string, timeStart, timeEnd time.Time, rowCount, sizeBytes, sizeCompressed int64,
	sourceNames []string, parquetPath, parquetSHA string, persistent bool) (Manifest, error) {

	if parquetSHA == "" {
		sum, sz, err := hashAndSize(parquetPath)
		if err != nil {
			return Manifest{}, err
		}
		parquetSHA = sum
		if sizeCompressed == 0 {
			sizeCompressed = sz
		}
	}
	if sizeBytes == 0 {
		sizeBytes = sizeCompressed
	}
	m := Manifest{
		ManifestVersion:  ManifestVersion,
		SchemaVersion:    1,
		SegmentID:        segmentID,
		TimeStart:        timeStart.UTC(),
		TimeEnd:          timeEnd.UTC(),
		RowCount:         rowCount,
		SizeBytes:        sizeBytes,
		SizeCompressed:   sizeCompressed,
		Compression:      "zstd",
		CompressionLevel: 3,
		SourceNames:      append([]string{}, sourceNames...),
		ParquetFilename:  filepath.Base(parquetPath),
		ParquetSHA256:    parquetSHA,
		Persistent:       persistent,
	}
	if !strings.HasSuffix(m.ParquetFilename, ".parquet") {
		m.ParquetFilename = ParquetFilename(segmentID)
	}
	return m, nil
}

// ParquetSHA256 is a thin wrapper over hashAndSize exported for callers
// outside the segments package (archive backends). Returns the hex
// digest and the file size in bytes.
func ParquetSHA256(path string) (string, int64, error) {
	return hashAndSize(path)
}
