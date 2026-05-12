package archive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dokku/logpond/internal/segments"
)

// scriptFixture builds a small bash script at path that responds to the
// archive-backend contract based on env-driven knobs:
//
//	FIXTURE_EXIT          exit code to return (default 0)
//	FIXTURE_STDOUT        line(s) to echo to stdout
//	FIXTURE_STDERR        line(s) to echo to stderr
//	FIXTURE_SLEEP_SECONDS sleep before exiting
//	FIXTURE_WRITE_OUTPUTS when "true", retrieve-mode writes the
//	                       --output-parquet/--output-manifest files
//	                       with valid contents (used by the retrieve test)
//	FIXTURE_PROBE_RESPONSE per-mode --probe response, e.g.
//	                       "archive=0,verify=64,retrieve=64"
//	FIXTURE_LOG_FILE       append "mode start"/"mode end" to this file
//	                       (used to assert serialization)
//	FIXTURE_MIRROR_PARQUET when set in retrieve mode, copy the file at
//	                       this path to --output-parquet and write a
//	                       matching manifest.
func scriptFixture(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "fixture.sh")
	body := `#!/usr/bin/env bash
set -u

mode="$1"
shift

# --probe path: when --probe is present anywhere in args, honor
# FIXTURE_PROBE_RESPONSE (default exit 0).
for a in "$@"; do
  if [ "$a" = "--probe" ]; then
    if [ -n "${FIXTURE_PROBE_RESPONSE:-}" ]; then
      IFS=',' read -ra pairs <<< "$FIXTURE_PROBE_RESPONSE"
      for kv in "${pairs[@]}"; do
        key="${kv%%=*}"
        val="${kv##*=}"
        if [ "$key" = "$mode" ]; then
          exit "$val"
        fi
      done
    fi
    exit 0
  fi
done

# Optional serialization log.
if [ -n "${FIXTURE_LOG_FILE:-}" ]; then
  printf '%s start %s\n' "$mode" "$LOGPOND_INVOCATION_ID" >> "$FIXTURE_LOG_FILE"
fi

# Optional output (used to assert capture).
if [ -n "${FIXTURE_STDOUT:-}" ]; then
  printf '%s\n' "$FIXTURE_STDOUT"
fi
if [ -n "${FIXTURE_STDERR:-}" ]; then
  printf '%s\n' "$FIXTURE_STDERR" >&2
fi

# Optional sleep to exercise timeout / signal handling.
if [ -n "${FIXTURE_SLEEP_SECONDS:-}" ]; then
  # Use a sleep that ignores SIGTERM unless FIXTURE_OBEY_SIGTERM=1.
  if [ "${FIXTURE_OBEY_SIGTERM:-1}" = "1" ]; then
    sleep "$FIXTURE_SLEEP_SECONDS"
  else
    trap '' TERM
    sleep "$FIXTURE_SLEEP_SECONDS"
  fi
fi

# Retrieve mode: optionally write outputs.
if [ "$mode" = "retrieve" ] && [ "${FIXTURE_WRITE_OUTPUTS:-}" = "true" ]; then
  out_p=""
  out_m=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --output-parquet) out_p="$2"; shift 2;;
      --output-manifest) out_m="$2"; shift 2;;
      *) shift;;
    esac
  done
  if [ -n "${FIXTURE_MIRROR_PARQUET:-}" ]; then
    cp "$FIXTURE_MIRROR_PARQUET" "$out_p"
  fi
  if [ -n "${FIXTURE_MANIFEST_BODY:-}" ]; then
    printf '%s' "$FIXTURE_MANIFEST_BODY" > "$out_m"
  fi
fi

if [ -n "${FIXTURE_LOG_FILE:-}" ]; then
  printf '%s end %s\n' "$mode" "$LOGPOND_INVOCATION_ID" >> "$FIXTURE_LOG_FILE"
fi

exit "${FIXTURE_EXIT:-0}"
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}

func writeFakeParquet(t *testing.T, dir, id, content string) (path, sha string) {
	t.Helper()
	path = filepath.Join(dir, segments.ParquetFilename(id))
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	sum, _, err := segments.ParquetSHA256(path)
	if err != nil {
		t.Fatalf("sha: %v", err)
	}
	return path, sum
}

func newBackend(t *testing.T, scriptPath string, opts ScriptOptions) *ScriptBackend {
	t.Helper()
	opts.Path = scriptPath
	if opts.WorkDir == "" {
		opts.WorkDir = t.TempDir()
	}
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
	be, err := NewScriptBackend(opts)
	if err != nil {
		t.Fatalf("new script backend: %v", err)
	}
	return be
}

func segRef(id, path, sha string) SegmentRef {
	now := time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC)
	return SegmentRef{
		ID:            id,
		TimeStart:     now,
		TimeEnd:       now.Add(time.Hour),
		RowCount:      10,
		SizeBytes:     int64(len("payload-" + id)),
		ParquetPath:   path,
		ParquetSHA256: sha,
		SourceNames:   []string{"default"},
	}
}

// TestScript_ArchiveSuccess covers the happy path: exit 0, LOGPOND_ARCHIVE_REF
// parsing, env-var passthrough, manifest creation.
func TestScript_ArchiveSuccess(t *testing.T) {
	dir := t.TempDir()
	script := scriptFixture(t, dir)
	parquet, sha := writeFakeParquet(t, dir, "seg-1", "payload-seg-1")

	t.Setenv("FIXTURE_EXIT", "0")
	t.Setenv("FIXTURE_STDOUT", "uploading...\nLOGPOND_ARCHIVE_REF=restic:abc123")

	be := newBackend(t, script, ScriptOptions{
		Env: map[string]string{"RESTIC_REPOSITORY": "/tmp/restic"},
	})

	res, err := be.Archive(context.Background(), segRef("seg-1", parquet, sha))
	if err != nil {
		t.Fatalf("archive: %v\nstdout=%s\nstderr=%s", err, res.Stdout, res.Stderr)
	}
	if res.ArchiveRef != "restic:abc123" {
		t.Fatalf("archive_ref: %q", res.ArchiveRef)
	}
	if res.ParquetSHA256 != sha {
		t.Fatalf("sha: got %q want %q", res.ParquetSHA256, sha)
	}
	if res.ManifestSHA256 == "" {
		t.Fatal("manifest sha empty")
	}
	if !strings.Contains(res.Stdout, "uploading") {
		t.Fatalf("stdout missing uploading line: %q", res.Stdout)
	}
}

// TestScript_ArchiveExitCodes verifies the §7.9.3 exit-code table.
func TestScript_ArchiveExitCodes(t *testing.T) {
	cases := []struct {
		name      string
		exit      string
		expectErr bool
		isUnsup   bool
		message   string
	}{
		{"success", "0", false, false, ""},
		{"generic", "1", true, false, "exit 1"},
		{"unsupported", "64", true, true, ""},
		{"permanent", "65", true, false, "permanent failure"},
		{"temporary", "75", true, false, "temporary failure"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			script := scriptFixture(t, dir)
			parquet, sha := writeFakeParquet(t, dir, "seg-"+tc.name, "payload-"+tc.name)

			t.Setenv("FIXTURE_EXIT", tc.exit)
			t.Setenv("FIXTURE_STDERR", tc.message)

			be := newBackend(t, script, ScriptOptions{})
			_, err := be.Archive(context.Background(), segRef("seg-"+tc.name, parquet, sha))

			if !tc.expectErr {
				if err != nil {
					t.Fatalf("expected success, got: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error, got success")
			}
			if tc.isUnsup && !errors.Is(err, ErrUnsupported) {
				t.Fatalf("expected ErrUnsupported, got: %v", err)
			}
			if tc.message != "" && !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.message)
			}
		})
	}
}

// TestScript_ArchiveTimeoutKills exercises SIGTERM-then-SIGKILL.
func TestScript_ArchiveTimeoutKills(t *testing.T) {
	dir := t.TempDir()
	script := scriptFixture(t, dir)
	parquet, sha := writeFakeParquet(t, dir, "seg-t", "x")

	t.Setenv("FIXTURE_SLEEP_SECONDS", "30")
	t.Setenv("FIXTURE_OBEY_SIGTERM", "0") // ignore SIGTERM so WaitDelay kicks in

	be := newBackend(t, script, ScriptOptions{
		Timeout: 200 * time.Millisecond,
		terminationGrace: 150 * time.Millisecond,
	})

	start := time.Now()
	_, err := be.Archive(context.Background(), segRef("seg-t", parquet, sha))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("error %q does not mention timeout", err.Error())
	}
	// SIGTERM (200ms) + grace (150ms) + jitter; should be well below the
	// 30s sleep we asked the fixture to take.
	if elapsed > 5*time.Second {
		t.Fatalf("escalation took too long: %s", elapsed)
	}
}

// TestScript_ArchiveCapturesStdoutStderr ensures captured output round-
// trips through the result struct, including truncation.
func TestScript_ArchiveCapturesStdoutStderr(t *testing.T) {
	dir := t.TempDir()
	script := scriptFixture(t, dir)
	parquet, sha := writeFakeParquet(t, dir, "seg-c", "x")

	t.Setenv("FIXTURE_STDOUT", "alpha\nbeta")
	t.Setenv("FIXTURE_STDERR", "warn: foo")

	be := newBackend(t, script, ScriptOptions{})
	res, err := be.Archive(context.Background(), segRef("seg-c", parquet, sha))
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if !strings.Contains(res.Stdout, "alpha") || !strings.Contains(res.Stdout, "beta") {
		t.Fatalf("stdout: %q", res.Stdout)
	}
	if !strings.Contains(res.Stderr, "warn: foo") {
		t.Fatalf("stderr: %q", res.Stderr)
	}
}

// TestScript_ConcurrentArchiveSerialized confirms the mutex serializes
// concurrent calls per PRD §7.9.3 "one invocation at a time globally".
func TestScript_ConcurrentArchiveSerialized(t *testing.T) {
	dir := t.TempDir()
	script := scriptFixture(t, dir)
	logFile := filepath.Join(dir, "log.txt")
	t.Setenv("FIXTURE_LOG_FILE", logFile)
	t.Setenv("FIXTURE_SLEEP_SECONDS", "0.1")

	p1, sha1 := writeFakeParquet(t, dir, "seg-a", "a")
	p2, sha2 := writeFakeParquet(t, dir, "seg-b", "b")

	be := newBackend(t, script, ScriptOptions{})

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := be.Archive(context.Background(), segRef("seg-a", p1, sha1))
		errs <- err
	}()
	go func() {
		defer wg.Done()
		_, err := be.Archive(context.Background(), segRef("seg-b", p2, sha2))
		errs <- err
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("archive: %v", err)
		}
	}

	body, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 log lines, got %d: %q", len(lines), body)
	}
	// Lines should be paired start/end (no interleaving).
	if !strings.Contains(lines[0], "start") || !strings.Contains(lines[1], "end") {
		t.Fatalf("first invocation interleaved: %q", body)
	}
	if !strings.Contains(lines[2], "start") || !strings.Contains(lines[3], "end") {
		t.Fatalf("second invocation interleaved: %q", body)
	}
}

// TestScript_Probe parses --probe responses and caches them.
func TestScript_Probe(t *testing.T) {
	dir := t.TempDir()
	script := scriptFixture(t, dir)
	t.Setenv("FIXTURE_PROBE_RESPONSE", "archive=0,verify=64,retrieve=64")

	be := newBackend(t, script, ScriptOptions{})
	if err := be.Probe(context.Background()); err != nil {
		t.Fatalf("probe: %v", err)
	}

	d := be.CapabilityDetail()
	if d.Archive != "yes" {
		t.Fatalf("archive cap: %q", d.Archive)
	}
	if d.Verify != "no" {
		t.Fatalf("verify cap: %q", d.Verify)
	}
	if d.Retrieve != "no" {
		t.Fatalf("retrieve cap: %q", d.Retrieve)
	}
	caps := be.Capabilities()
	if !caps.Archive || caps.Verify || caps.Retrieve {
		t.Fatalf("caps: %+v", caps)
	}
}

// TestScript_ProbeMissingDefaults: when --probe exits non-zero with a
// generic code (e.g. 1, meaning probe isn't implemented), archive should
// default to "yes" while verify and retrieve stay "unknown" (PRD §7.9.3).
func TestScript_ProbeMissingDefaults(t *testing.T) {
	dir := t.TempDir()
	script := scriptFixture(t, dir)
	t.Setenv("FIXTURE_PROBE_RESPONSE", "archive=1,verify=1,retrieve=1")

	be := newBackend(t, script, ScriptOptions{})
	if err := be.Probe(context.Background()); err != nil {
		t.Fatalf("probe: %v", err)
	}
	d := be.CapabilityDetail()
	if d.Archive != "yes" {
		t.Fatalf("archive cap: %q (PRD default is yes)", d.Archive)
	}
	if d.Verify != "unknown" {
		t.Fatalf("verify cap: %q", d.Verify)
	}
	if d.Retrieve != "unknown" {
		t.Fatalf("retrieve cap: %q", d.Retrieve)
	}
}

// TestScript_VerifyAggregatesPerSegment runs the script per id and
// records failures with their exit codes.
func TestScript_VerifyAggregatesPerSegment(t *testing.T) {
	dir := t.TempDir()
	counter := filepath.Join(dir, "counter")
	wrap := filepath.Join(dir, "wrap.sh")
	wrapBody := `#!/usr/bin/env bash
n=$(cat "` + counter + `" 2>/dev/null || echo 0)
n=$((n + 1))
echo "$n" > "` + counter + `"
if [ "$n" = "2" ]; then
  echo "restic: snapshot missing" >&2
  exit 1
fi
exit 0
`
	if err := os.WriteFile(wrap, []byte(wrapBody), 0o755); err != nil {
		t.Fatalf("write wrap: %v", err)
	}
	be := newBackend(t, wrap, ScriptOptions{})
	res, err := be.Verify(context.Background(), []string{"seg-a", "seg-b", "seg-c"})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if res.Scanned != 3 {
		t.Fatalf("scanned: %d", res.Scanned)
	}
	if res.Verified != 2 {
		t.Fatalf("verified: %d", res.Verified)
	}
	if len(res.Failed) != 1 {
		t.Fatalf("failed: %+v", res.Failed)
	}
	f := res.Failed[0]
	if f.SegmentID != "seg-b" {
		t.Fatalf("failed seg: %q", f.SegmentID)
	}
	if f.ExitCode != 1 {
		t.Fatalf("failed exit: %d", f.ExitCode)
	}
	if !strings.Contains(f.Note, "snapshot missing") {
		t.Fatalf("failed note: %q", f.Note)
	}
}

// TestScript_VerifyUnsupportedReturnsErr surfaces ErrUnsupported when
// the script exits 64.
func TestScript_VerifyUnsupportedReturnsErr(t *testing.T) {
	dir := t.TempDir()
	script := scriptFixture(t, dir)
	t.Setenv("FIXTURE_EXIT", "64")
	be := newBackend(t, script, ScriptOptions{})
	_, err := be.Verify(context.Background(), nil)
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("expected ErrUnsupported, got: %v", err)
	}
	// And the cap should be cached as "no".
	d := be.CapabilityDetail()
	if d.Verify != "no" {
		t.Fatalf("verify cap not cached: %q", d.Verify)
	}
}

// TestScript_RetrieveValidatesSHA: when the script writes the parquet
// and manifest, the backend verifies SHA-256 against the manifest.
func TestScript_RetrieveValidatesSHA(t *testing.T) {
	dir := t.TempDir()
	script := scriptFixture(t, dir)
	parquet, sha := writeFakeParquet(t, dir, "seg-r", "payload-seg-r")

	manifest, err := segments.BuildManifest("seg-r",
		time.Now().UTC(), time.Now().UTC().Add(time.Hour),
		1, int64(len("payload-seg-r")), int64(len("payload-seg-r")),
		[]string{"default"}, parquet, sha, false)
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	body, err := manifest.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	t.Setenv("FIXTURE_WRITE_OUTPUTS", "true")
	t.Setenv("FIXTURE_MIRROR_PARQUET", parquet)
	t.Setenv("FIXTURE_MANIFEST_BODY", string(body))

	be := newBackend(t, script, ScriptOptions{})
	out := t.TempDir()
	res, err := be.Retrieve(context.Background(), segRef("seg-r", parquet, sha), out)
	if err != nil {
		t.Fatalf("retrieve: %v\nstdout=%q stderr=%q", err, res.Stdout, res.Stderr)
	}
	if _, err := os.Stat(res.ParquetPath); err != nil {
		t.Fatalf("parquet missing: %v", err)
	}
	if _, err := os.Stat(res.ManifestPath); err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	if res.Manifest.SegmentID != "seg-r" {
		t.Fatalf("manifest segment id: %q", res.Manifest.SegmentID)
	}
}

// TestScript_RetrieveRejectsSHAMismatch: a corrupted retrieve must
// produce a SHA mismatch error.
func TestScript_RetrieveRejectsSHAMismatch(t *testing.T) {
	dir := t.TempDir()
	script := scriptFixture(t, dir)
	parquet, sha := writeFakeParquet(t, dir, "seg-r2", "payload-seg-r2")

	// Build manifest with the right SHA, but mirror a DIFFERENT parquet
	// so the retrieved file doesn't match.
	otherParquet, _ := writeFakeParquet(t, dir, "seg-other", "different-payload")
	manifest, err := segments.BuildManifest("seg-r2",
		time.Now().UTC(), time.Now().UTC().Add(time.Hour),
		1, int64(len("payload-seg-r2")), int64(len("payload-seg-r2")),
		[]string{"default"}, parquet, sha, false)
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	body, err := manifest.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	t.Setenv("FIXTURE_WRITE_OUTPUTS", "true")
	t.Setenv("FIXTURE_MIRROR_PARQUET", otherParquet)
	t.Setenv("FIXTURE_MANIFEST_BODY", string(body))

	be := newBackend(t, script, ScriptOptions{})
	out := t.TempDir()
	_, err = be.Retrieve(context.Background(), segRef("seg-r2", parquet, sha), out)
	if err == nil || !strings.Contains(err.Error(), "sha mismatch") {
		t.Fatalf("expected sha mismatch error, got: %v", err)
	}
}

// TestScript_EnvVars confirms LOGPOND_* env vars are passed.
func TestScript_EnvVars(t *testing.T) {
	dir := t.TempDir()
	envProbe := filepath.Join(dir, "env-probe.sh")
	// Emit the requested env vars as JSON so the test can decode them.
	body := `#!/usr/bin/env bash
cat <<EOF
{"id":"$LOGPOND_SEGMENT_ID","start":"$LOGPOND_SEGMENT_TIME_START","mode":"$LOGPOND_MODE","manifest_ver":"$LOGPOND_MANIFEST_VERSION","custom":"$LOGPOND_CUSTOM_X","sha":"$LOGPOND_SEGMENT_PARQUET_SHA256"}
EOF
`
	if err := os.WriteFile(envProbe, []byte(body), 0o755); err != nil {
		t.Fatalf("write env probe: %v", err)
	}
	parquet, sha := writeFakeParquet(t, dir, "seg-env", "x")
	be := newBackend(t, envProbe, ScriptOptions{
		Env: map[string]string{"LOGPOND_CUSTOM_X": "custom-value"},
	})

	res, err := be.Archive(context.Background(), segRef("seg-env", parquet, sha))
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	type fields struct {
		ID, Start, Mode, ManifestVer, Custom, SHA string `json:"-"`
	}
	var got struct {
		ID          string `json:"id"`
		Start       string `json:"start"`
		Mode        string `json:"mode"`
		ManifestVer string `json:"manifest_ver"`
		Custom      string `json:"custom"`
		SHA         string `json:"sha"`
	}
	_ = fields{}
	out := strings.TrimSpace(res.Stdout)
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode env stdout: %v\nbody=%s", err, out)
	}
	if got.ID != "seg-env" {
		t.Fatalf("LOGPOND_SEGMENT_ID: %q", got.ID)
	}
	if got.Mode != "archive" {
		t.Fatalf("LOGPOND_MODE: %q", got.Mode)
	}
	if got.ManifestVer != "1" {
		t.Fatalf("LOGPOND_MANIFEST_VERSION: %q", got.ManifestVer)
	}
	if got.Custom != "custom-value" {
		t.Fatalf("custom passthrough: %q", got.Custom)
	}
	if got.SHA != sha {
		t.Fatalf("LOGPOND_SEGMENT_PARQUET_SHA256: %q want %q", got.SHA, sha)
	}
	if got.Start == "" {
		t.Fatal("LOGPOND_SEGMENT_TIME_START empty")
	}
}

// TestScript_ArchiveRefParsedFromLastLine: extractArchiveRef should
// pick the last LOGPOND_ARCHIVE_REF= line, not the first.
func TestScript_ArchiveRefParsedFromLastLine(t *testing.T) {
	got := extractArchiveRef("LOGPOND_ARCHIVE_REF=first\nLOGPOND_ARCHIVE_REF=second\n")
	if got != "second" {
		t.Fatalf("expected 'second', got %q", got)
	}
	if extractArchiveRef("no ref here") != "" {
		t.Fatal("expected empty when no marker")
	}
}

// TestScript_CappedBufferTruncates writes more than the limit and
// asserts the buffer caps with the truncation marker.
func TestScript_CappedBufferTruncates(t *testing.T) {
	buf := newCappedBuffer(16)
	if _, err := buf.Write([]byte("0123456789abcdefgggggg")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !strings.Contains(buf.String(), "...[truncated]") {
		t.Fatalf("missing truncation marker: %q", buf.String())
	}
}

// TestScript_MissingPath_NewBackendFails: constructing a backend
// against a non-existent script returns an error.
func TestScript_MissingPath_NewBackendFails(t *testing.T) {
	_, err := NewScriptBackend(ScriptOptions{Path: filepath.Join(t.TempDir(), "nope.sh")})
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestScript_RejectsRelativePath: the security audit (Phase 16 task 7)
// requires absolute, normalized script paths so an env-overlay can't
// point Logpond at a file outside the container layout.
func TestScript_RejectsRelativePath(t *testing.T) {
	_, err := NewScriptBackend(ScriptOptions{Path: "scripts/archive.sh"})
	if err == nil {
		t.Fatal("expected error for relative path")
	}
	if !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("error should mention absolute: %v", err)
	}
}

// TestScript_RejectsParentTraversal: paths containing `..` are rejected
// even when nominally absolute.
func TestScript_RejectsParentTraversal(t *testing.T) {
	_, err := NewScriptBackend(ScriptOptions{Path: "/etc/logpond/../passwd"})
	if err == nil {
		t.Fatal("expected error for traversal path")
	}
	if !strings.Contains(err.Error(), "normalized") {
		t.Fatalf("error should mention normalized: %v", err)
	}
}

// TestScript_ScrubsInheritedEnv: AWS_* and LOGPOND_* keys present on the
// parent process must not leak into the script's environment. The
// explicit LOGPOND_MODE / LOGPOND_INVOCATION_ID / LOGPOND_MANIFEST_VERSION
// values are restored deliberately by buildEnv.
func TestScript_ScrubsInheritedEnv(t *testing.T) {
	dir := t.TempDir()
	script := writeEnvDumpScript(t, dir)
	be := newBackend(t, script, ScriptOptions{Env: map[string]string{"PASSTHROUGH_VAR": "allowed"}})

	t.Setenv("AWS_SECRET_ACCESS_KEY", "should-not-leak")
	t.Setenv("LOGPOND_ARCHIVE_S3_SECRET_ACCESS_KEY", "should-not-leak")
	t.Setenv("CUSTOM_OPERATOR_VAR", "passes-through")

	env := be.buildEnv(ModeArchive, "inv-test", map[string]string{
		"LOGPOND_SEGMENT_ID": "seg-x",
	})

	for _, kv := range env {
		k := kv
		if i := strings.IndexByte(kv, '='); i > 0 {
			k = kv[:i]
		}
		if strings.HasPrefix(k, "AWS_") {
			t.Fatalf("AWS_* env leaked into script env: %s", kv)
		}
		if strings.HasPrefix(k, "LOGPOND_") {
			switch k {
			case "LOGPOND_MODE", "LOGPOND_INVOCATION_ID",
				"LOGPOND_MANIFEST_VERSION", "LOGPOND_SEGMENT_ID":
				// explicit contract; allowed
			default:
				t.Fatalf("non-contract LOGPOND_* env leaked: %s", kv)
			}
		}
	}

	if !containsKV(env, "PASSTHROUGH_VAR=allowed") {
		t.Fatalf("operator-supplied env var was dropped")
	}
	if !containsKV(env, "CUSTOM_OPERATOR_VAR=passes-through") {
		t.Fatalf("non-Logpond inherited env var was dropped")
	}
	if !containsKV(env, "LOGPOND_MODE=archive") {
		t.Fatalf("contract LOGPOND_MODE missing")
	}
	if !containsKV(env, "LOGPOND_SEGMENT_ID=seg-x") {
		t.Fatalf("contract LOGPOND_SEGMENT_ID missing")
	}
}

func writeEnvDumpScript(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "envdump.sh")
	body := "#!/usr/bin/env bash\nenv\nexit 0\n"
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("write env-dump script: %v", err)
	}
	return path
}

func containsKV(env []string, want string) bool {
	for _, kv := range env {
		if kv == want {
			return true
		}
	}
	return false
}

// TestScript_WaitDelayFiresWhenSIGTERMIgnored sanity-checks the helper
// when the fixture script refuses to exit on SIGTERM but the WaitDelay
// is short enough to fire.
func TestScript_WaitDelayFiresWhenSIGTERMIgnored(t *testing.T) {
	dir := t.TempDir()
	script := scriptFixture(t, dir)
	parquet, sha := writeFakeParquet(t, dir, "seg-w", "x")

	t.Setenv("FIXTURE_SLEEP_SECONDS", "10")
	t.Setenv("FIXTURE_OBEY_SIGTERM", "0")

	be := newBackend(t, script, ScriptOptions{
		Timeout:          100 * time.Millisecond,
		terminationGrace: 200 * time.Millisecond,
	})

	start := time.Now()
	_, err := be.Archive(context.Background(), segRef("seg-w", parquet, sha))
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if d := time.Since(start); d > 4*time.Second {
		t.Fatalf("waited too long: %s", d)
	}
}

// TestScript_NeedsBash verifies bash is present (skip otherwise so the
// suite can still run on minimal environments).
func TestScript_NeedsBash(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skipf("bash not available: %v", err)
	}
}

func init() {
	// On macOS/Linux some shells don't ship under /usr/bin/env reliably;
	// surface a clearer error than a confusing exec failure if bash is
	// genuinely missing.
	if _, err := exec.LookPath("bash"); err != nil {
		fmt.Fprintf(os.Stderr, "script_test.go: bash not on PATH; tests will skip\n")
	}
}
