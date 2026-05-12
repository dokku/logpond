package segments

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestManifest_RoundTrip(t *testing.T) {
	m := Manifest{
		ManifestVersion:  1,
		SchemaVersion:    1,
		SegmentID:        "202605121400",
		TimeStart:        time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
		TimeEnd:          time.Date(2026, 5, 12, 15, 0, 0, 0, time.UTC),
		RowCount:         42,
		SizeBytes:        1024,
		SizeCompressed:   512,
		Compression:      "zstd",
		CompressionLevel: 3,
		SourceNames:      []string{"default"},
		ParquetFilename:  "segment-202605121400.parquet",
		ParquetSHA256:    "abcd",
		Persistent:       false,
	}
	buf, err := m.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got Manifest
	if err := json.Unmarshal(buf, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.SegmentID != m.SegmentID || got.ParquetSHA256 != m.ParquetSHA256 ||
		!got.TimeStart.Equal(m.TimeStart) || got.RowCount != m.RowCount {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, m)
	}
}

func TestManifest_Validate(t *testing.T) {
	cases := []struct {
		name    string
		m       Manifest
		wantErr bool
	}{
		{"happy", Manifest{ManifestVersion: 1, SegmentID: "x", ParquetSHA256: "h", ParquetFilename: "f.parquet",
			TimeStart: time.Now(), TimeEnd: time.Now().Add(time.Hour)}, false},
		{"version-mismatch", Manifest{ManifestVersion: 99, SegmentID: "x", ParquetSHA256: "h",
			ParquetFilename: "f.parquet", TimeStart: time.Now(), TimeEnd: time.Now()}, true},
		{"no-sha", Manifest{ManifestVersion: 1, SegmentID: "x", ParquetFilename: "f.parquet",
			TimeStart: time.Now(), TimeEnd: time.Now()}, true},
		{"no-id", Manifest{ManifestVersion: 1, ParquetSHA256: "h", ParquetFilename: "f.parquet",
			TimeStart: time.Now(), TimeEnd: time.Now()}, true},
		{"no-times", Manifest{ManifestVersion: 1, SegmentID: "x", ParquetSHA256: "h",
			ParquetFilename: "f.parquet"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.m.Validate()
			if c.wantErr != (err != nil) {
				t.Fatalf("Validate: wantErr=%v err=%v", c.wantErr, err)
			}
		})
	}
}

func TestBuildManifest_ComputesSHAFromFile(t *testing.T) {
	dir := t.TempDir()
	parquet := filepath.Join(dir, "segment-test.parquet")
	if err := os.WriteFile(parquet, []byte("not-really-parquet"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	m, err := BuildManifest("test",
		time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
		time.Date(2026, 5, 12, 15, 0, 0, 0, time.UTC),
		0, 0, 0, []string{"default"}, parquet, "", false)
	if err != nil {
		t.Fatalf("BuildManifest: %v", err)
	}
	if m.ParquetSHA256 == "" {
		t.Fatal("expected computed sha")
	}
	if m.ParquetFilename != "segment-test.parquet" {
		t.Fatalf("got filename %q", m.ParquetFilename)
	}
	if m.Compression != "zstd" || m.CompressionLevel != 3 {
		t.Fatal("expected zstd-3 defaults")
	}
}

func TestWriteManifestFile_AtomicRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "segment-x.manifest.json")
	want := Manifest{
		ManifestVersion:  1,
		SchemaVersion:    1,
		SegmentID:        "x",
		TimeStart:        time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
		TimeEnd:          time.Date(2026, 5, 12, 15, 0, 0, 0, time.UTC),
		ParquetSHA256:    "abcd",
		ParquetFilename:  "segment-x.parquet",
		Compression:      "zstd",
		CompressionLevel: 3,
		SourceNames:      []string{"default"},
	}
	if err := WriteManifestFile(want, path); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := ReadManifestFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.SegmentID != want.SegmentID || got.ParquetSHA256 != want.ParquetSHA256 {
		t.Fatalf("round-trip mismatch")
	}
}
