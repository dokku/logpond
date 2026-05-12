package segments

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// hashAndSize streams the file at path through sha256 and returns the
// hex digest plus the byte length. Used to fill the parquet_sha256 and
// size_compressed catalog fields when sealing.
func hashAndSize(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, fmt.Errorf("opening %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, fmt.Errorf("hashing %s: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// fsyncFile flushes a file's data to disk. Best-effort on platforms
// that don't expose fsync (none currently supported).
func fsyncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// fsyncDir flushes a directory entry so renames within it survive a
// crash on POSIX filesystems.
func fsyncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	// fsync on a directory is fine on Linux/macOS; ignore EINVAL from
	// platforms that don't support it.
	_ = d.Sync()
	return nil
}
