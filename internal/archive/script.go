package archive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/dokku/logpond/internal/segments"
)

// DefaultScriptTimeout is the per-invocation timeout when ScriptOptions
// leaves Timeout zero (PRD §7.9.3: "Default 600s").
const DefaultScriptTimeout = 10 * time.Minute

// ScriptTerminationGrace is how long the supervisor waits after SIGTERM
// before SIGKILL (PRD §7.9.3).
const ScriptTerminationGrace = 30 * time.Second

// scriptOutputCap bounds captured stdout/stderr for any single
// invocation. The PRD doesn't pin a number; 1 MiB per stream keeps the
// jobs row well under SQLite's default page size threshold while
// preserving enough output to debug a misbehaving script.
const scriptOutputCap = 1 << 20

// Script exit codes used by the contract in PRD §7.9.3.
const (
	ExitSuccess          = 0
	ExitUnsupportedMode  = 64
	ExitPermanent        = 65
	ExitTemporary        = 75
	exitTimeoutSentinel  = -1
	exitStartupSentinel  = -2
	exitUnknownSentinel  = -3
)

// Script invocation modes.
const (
	ModeArchive  = "archive"
	ModeVerify   = "verify"
	ModeRetrieve = "retrieve"
)

// archiveRefPrefix marks the stdout line whose value is recorded as the
// segment's archive_ref (PRD §7.9.3).
const archiveRefPrefix = "LOGPOND_ARCHIVE_REF="

// ScriptOptions configures a ScriptBackend.
type ScriptOptions struct {
	Path    string
	Timeout time.Duration
	Env     map[string]string
	WorkDir string
	Logger  *slog.Logger

	// terminationGrace is exposed for tests; production uses the constant.
	terminationGrace time.Duration

	// nowFunc is exposed for tests; production uses time.Now.
	nowFunc func() time.Time
}

// ScriptBackend implements Backend by shelling out to a user-provided
// executable. One invocation is in flight at a time (PRD §7.9.3).
type ScriptBackend struct {
	path             string
	timeout          time.Duration
	env              map[string]string
	workDir          string
	logger           *slog.Logger
	terminationGrace time.Duration
	nowFunc          func() time.Time

	mu sync.Mutex // serializes script invocations

	capMu  sync.Mutex
	caps   probeState
}

// probeState records per-mode availability. The script backend caches
// this for the process lifetime and updates it lazily when a mode
// returns exit 64.
type probeState struct {
	archive  capabilityState
	retrieve capabilityState
	verify   capabilityState
}

type capabilityState int

const (
	capUnknown capabilityState = iota
	capYes
	capNo
)

// NewScriptBackend constructs a ScriptBackend. Path is required.
func NewScriptBackend(opts ScriptOptions) (*ScriptBackend, error) {
	if strings.TrimSpace(opts.Path) == "" {
		return nil, errors.New("script backend: Path is required")
	}
	if _, err := os.Stat(opts.Path); err != nil {
		return nil, fmt.Errorf("script backend: stat %s: %w", opts.Path, err)
	}
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultScriptTimeout
	}
	if opts.WorkDir == "" {
		opts.WorkDir = filepath.Join(os.TempDir(), "logpond-script-work")
	}
	if err := os.MkdirAll(opts.WorkDir, 0o755); err != nil {
		return nil, fmt.Errorf("script backend: ensure work dir %s: %w", opts.WorkDir, err)
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	grace := opts.terminationGrace
	if grace <= 0 {
		grace = ScriptTerminationGrace
	}
	now := opts.nowFunc
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &ScriptBackend{
		path:             opts.Path,
		timeout:          opts.Timeout,
		env:              cloneEnv(opts.Env),
		workDir:          opts.WorkDir,
		logger:           opts.Logger,
		terminationGrace: grace,
		nowFunc:          now,
	}, nil
}

// Capabilities reports the cached probe state. Unknown defaults to true
// so the operator can discover unsupported modes lazily (PRD §7.9.3).
func (b *ScriptBackend) Capabilities() Capabilities {
	b.capMu.Lock()
	defer b.capMu.Unlock()
	return Capabilities{
		Name:     "script",
		Archive:  capYesOrUnknown(b.caps.archive),
		Retrieve: capYesOrUnknown(b.caps.retrieve),
		Verify:   capYesOrUnknown(b.caps.verify),
	}
}

// CapabilityDetail reports the explicit per-mode probe state for the
// admin API ("yes"/"no"/"unknown").
func (b *ScriptBackend) CapabilityDetail() CapabilityDetail {
	b.capMu.Lock()
	defer b.capMu.Unlock()
	return CapabilityDetail{
		Backend:  "script",
		Archive:  capString(b.caps.archive),
		Retrieve: capString(b.caps.retrieve),
		Verify:   capString(b.caps.verify),
	}
}

// Probe invokes the script with each mode + "--probe" to discover
// supported modes. PRD §7.9.3: "If `--probe` isn't implemented (script
// exits non-0 with no specific code), default to archive=yes,
// verify=unknown, retrieve=unknown."
//
// Probe is idempotent and called once at startup.
func (b *ScriptBackend) Probe(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	state := probeState{}
	for _, mode := range []string{ModeArchive, ModeVerify, ModeRetrieve} {
		res := b.probeMode(ctx, mode)
		switch mode {
		case ModeArchive:
			state.archive = res
		case ModeVerify:
			state.verify = res
		case ModeRetrieve:
			state.retrieve = res
		}
	}

	b.capMu.Lock()
	b.caps = state
	b.capMu.Unlock()

	b.logger.Info("script backend: probe complete",
		"path", b.path,
		"archive", capString(state.archive),
		"verify", capString(state.verify),
		"retrieve", capString(state.retrieve),
	)
	return nil
}

func (b *ScriptBackend) probeMode(ctx context.Context, mode string) capabilityState {
	probeCtx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	out, err := b.exec(probeCtx, mode, []string{"--probe"}, nil)
	switch out.exitCode {
	case ExitSuccess:
		return capYes
	case ExitUnsupportedMode:
		return capNo
	}
	// PRD: any other status means --probe likely isn't implemented; default
	// archive to yes (the operator wouldn't pick script otherwise), the
	// other two stay unknown.
	if err != nil {
		b.logger.Debug("script backend: probe inconclusive",
			"mode", mode, "exit_code", out.exitCode, "err", err.Error())
	}
	if mode == ModeArchive {
		return capYes
	}
	return capUnknown
}

// Archive implements Backend. The flow:
//   - Build the manifest from the SegmentRef and write it to a temp dir.
//   - Invoke the script with `archive --segment-id ... --parquet-path ...
//     --manifest-path ...`.
//   - Capture stdout/stderr (bounded), parse LOGPOND_ARCHIVE_REF=.
//   - Map exit codes per PRD §7.9.3.
func (b *ScriptBackend) Archive(ctx context.Context, ref SegmentRef) (ArchiveResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if cached := b.cachedCap(ModeArchive); cached == capNo {
		return ArchiveResult{}, ErrUnsupported
	}

	if ref.ParquetPath == "" {
		return ArchiveResult{}, errors.New("script archive: ParquetPath required")
	}
	if _, err := os.Stat(ref.ParquetPath); err != nil {
		return ArchiveResult{}, fmt.Errorf("script archive: stat %s: %w", ref.ParquetPath, err)
	}
	sha := ref.ParquetSHA256
	if sha == "" {
		s, _, err := segments.ParquetSHA256(ref.ParquetPath)
		if err != nil {
			return ArchiveResult{}, err
		}
		sha = s
	}

	st, err := os.Stat(ref.ParquetPath)
	if err != nil {
		return ArchiveResult{}, err
	}
	manifest, err := segments.BuildManifest(
		ref.ID, ref.TimeStart, ref.TimeEnd,
		ref.RowCount, ref.SizeBytes, st.Size(),
		ref.SourceNames, ref.ParquetPath, sha, ref.Persistent,
	)
	if err != nil {
		return ArchiveResult{}, err
	}

	scratch, err := b.mkScratchDir()
	if err != nil {
		return ArchiveResult{}, err
	}
	defer os.RemoveAll(scratch)

	manifestPath := filepath.Join(scratch, segments.ManifestFilename(ref.ID))
	if err := segments.WriteManifestFile(manifest, manifestPath); err != nil {
		return ArchiveResult{}, err
	}
	manifestBytes, err := manifest.Encode()
	if err != nil {
		return ArchiveResult{}, err
	}
	manifestSHA, _, err := sha256Bytes(manifestBytes)
	if err != nil {
		return ArchiveResult{}, err
	}

	invocationCtx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	env := segmentEnv(ref)
	env["LOGPOND_SEGMENT_PARQUET_PATH"] = ref.ParquetPath
	env["LOGPOND_SEGMENT_MANIFEST_PATH"] = manifestPath
	out, err := b.exec(invocationCtx, ModeArchive, []string{
		"--segment-id", ref.ID,
		"--parquet-path", ref.ParquetPath,
		"--manifest-path", manifestPath,
	}, env)
	b.recordCap(ModeArchive, out.exitCode)

	if err := b.classifyExit(ModeArchive, out, err); err != nil {
		return ArchiveResult{Stdout: out.stdout, Stderr: out.stderr}, err
	}

	return ArchiveResult{
		ArchiveRef:     extractArchiveRef(out.stdout),
		ManifestSHA256: manifestSHA,
		ParquetSHA256:  sha,
		Manifest:       manifest,
		Stdout:         out.stdout,
		Stderr:         out.stderr,
	}, nil
}

// Retrieve implements Backend.
func (b *ScriptBackend) Retrieve(ctx context.Context, ref SegmentRef, outDir string) (RetrieveResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if cached := b.cachedCap(ModeRetrieve); cached == capNo {
		return RetrieveResult{}, ErrUnsupported
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return RetrieveResult{}, fmt.Errorf("script retrieve: mkdir %s: %w", outDir, err)
	}
	parquetPath := filepath.Join(outDir, segments.ParquetFilename(ref.ID))
	manifestPath := filepath.Join(outDir, segments.ManifestFilename(ref.ID))

	invocationCtx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	env := segmentEnv(ref)
	env["LOGPOND_OUTPUT_PARQUET"] = parquetPath
	env["LOGPOND_OUTPUT_MANIFEST"] = manifestPath
	out, err := b.exec(invocationCtx, ModeRetrieve, []string{
		"--segment-id", ref.ID,
		"--output-parquet", parquetPath,
		"--output-manifest", manifestPath,
	}, env)
	b.recordCap(ModeRetrieve, out.exitCode)

	if err := b.classifyExit(ModeRetrieve, out, err); err != nil {
		_ = os.Remove(parquetPath)
		_ = os.Remove(manifestPath)
		return RetrieveResult{Stdout: out.stdout, Stderr: out.stderr}, err
	}

	manifest, err := segments.ReadManifestFile(manifestPath)
	if err != nil {
		return RetrieveResult{Stdout: out.stdout, Stderr: out.stderr}, err
	}
	sha, _, err := segments.ParquetSHA256(parquetPath)
	if err != nil {
		return RetrieveResult{Stdout: out.stdout, Stderr: out.stderr}, err
	}
	if !strings.EqualFold(sha, manifest.ParquetSHA256) {
		return RetrieveResult{Stdout: out.stdout, Stderr: out.stderr},
			fmt.Errorf("script retrieve: parquet sha mismatch: got %s want %s", sha, manifest.ParquetSHA256)
	}

	return RetrieveResult{
		ParquetPath:  parquetPath,
		ManifestPath: manifestPath,
		Manifest:     manifest,
		Stdout:       out.stdout,
		Stderr:       out.stderr,
	}, nil
}

// Verify implements Backend. When segIDs is empty the script is invoked
// with `verify --all`; otherwise one invocation per id (PRD §13.22).
// The mutex serializes invocations as required.
func (b *ScriptBackend) Verify(ctx context.Context, segIDs []string) (VerifyResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	res := VerifyResult{Backend: "script"}
	if cached := b.cachedCap(ModeVerify); cached == capNo {
		return res, ErrUnsupported
	}

	if len(segIDs) == 0 {
		invocationCtx, cancel := context.WithTimeout(ctx, b.timeout)
		defer cancel()
		out, err := b.exec(invocationCtx, ModeVerify, []string{"--all"}, nil)
		b.recordCap(ModeVerify, out.exitCode)
		res.Scanned = 1
		res.Stdout = appendBlock(res.Stdout, "--all", out.stdout)
		res.Stderr = appendBlock(res.Stderr, "--all", out.stderr)
		if err := b.classifyExit(ModeVerify, out, err); err != nil {
			if errors.Is(err, ErrUnsupported) {
				return res, err
			}
			res.Failed = append(res.Failed, VerifyFailed{
				ExitCode: out.exitCode,
				Note:     firstLine(out.stderr),
			})
			return res, nil
		}
		res.Verified = 1
		return res, nil
	}

	for _, id := range segIDs {
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		default:
		}
		invocationCtx, cancel := context.WithTimeout(ctx, b.timeout)
		out, err := b.exec(invocationCtx, ModeVerify, []string{"--segment-id", id}, nil)
		cancel()
		b.recordCap(ModeVerify, out.exitCode)
		res.Scanned++
		res.Stdout = appendBlock(res.Stdout, id, out.stdout)
		res.Stderr = appendBlock(res.Stderr, id, out.stderr)

		if err := b.classifyExit(ModeVerify, out, err); err != nil {
			if errors.Is(err, ErrUnsupported) {
				return res, err
			}
			res.Failed = append(res.Failed, VerifyFailed{
				SegmentID: id,
				ExitCode:  out.exitCode,
				Note:      firstLine(out.stderr),
			})
			continue
		}
		res.Verified++
	}
	return res, nil
}

// execResult bundles a script invocation's outcome.
type execResult struct {
	stdout   string
	stderr   string
	exitCode int
	timedOut bool
}

// exec runs the script in `mode` with extra args and per-invocation env
// vars, capturing bounded stdout/stderr. SIGTERM-then-SIGKILL escalation
// is wired via cmd.Cancel + cmd.WaitDelay (PRD §7.9.3).
func (b *ScriptBackend) exec(ctx context.Context, mode string, args []string, extraEnv map[string]string) (execResult, error) {
	invocationID := uuid.NewString()
	fullArgs := append([]string{mode}, args...)
	cmd := exec.CommandContext(ctx, b.path, fullArgs...)
	cmd.Env = b.buildEnv(mode, invocationID, extraEnv)
	cmd.WaitDelay = b.terminationGrace
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		return cmd.Process.Signal(syscall.SIGTERM)
	}
	stdoutBuf := newCappedBuffer(scriptOutputCap)
	stderrBuf := newCappedBuffer(scriptOutputCap)
	cmd.Stdout = stdoutBuf
	cmd.Stderr = stderrBuf

	b.logger.Debug("script backend: invoking",
		"path", b.path, "mode", mode, "args", args,
		"invocation_id", invocationID, "timeout", b.timeout,
	)
	start := b.nowFunc()
	err := cmd.Run()
	duration := b.nowFunc().Sub(start)

	out := execResult{
		stdout: stdoutBuf.String(),
		stderr: stderrBuf.String(),
	}

	switch {
	case err == nil:
		out.exitCode = ExitSuccess
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		out.exitCode = exitTimeoutSentinel
		out.timedOut = true
	default:
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			out.exitCode = exitErr.ExitCode()
		} else {
			out.exitCode = exitStartupSentinel
		}
	}

	b.logger.Info("script backend: invocation",
		"path", b.path,
		"mode", mode,
		"invocation_id", invocationID,
		"exit_code", out.exitCode,
		"timed_out", out.timedOut,
		"duration_ms", duration.Milliseconds(),
	)

	if out.timedOut {
		return out, fmt.Errorf("script %s: timed out after %s", mode, b.timeout)
	}
	return out, err
}

// buildEnv assembles the environment passed to the script. PRD §7.9.3
// enumerates the LOGPOND_* variables and notes that any
// archive.script.env.* entries are passed through verbatim. extra
// overrides anything in b.env so callers can supply per-invocation
// values (e.g. LOGPOND_SEGMENT_*).
func (b *ScriptBackend) buildEnv(mode, invocationID string, extra map[string]string) []string {
	env := os.Environ()
	env = append(env, "LOGPOND_MODE="+mode)
	env = append(env, "LOGPOND_INVOCATION_ID="+invocationID)
	env = append(env, "LOGPOND_MANIFEST_VERSION=1")
	for k, v := range b.env {
		if _, override := extra[k]; override {
			continue
		}
		env = append(env, k+"="+v)
	}
	for k, v := range extra {
		env = append(env, k+"="+v)
	}
	return env
}

// segmentEnv returns the PRD §7.9.3 LOGPOND_SEGMENT_* env entries for
// ref. Empty values are omitted so the script sees only fields the
// catalog actually populated.
func segmentEnv(ref SegmentRef) map[string]string {
	out := map[string]string{}
	if ref.ID != "" {
		out["LOGPOND_SEGMENT_ID"] = ref.ID
	}
	if !ref.TimeStart.IsZero() {
		out["LOGPOND_SEGMENT_TIME_START"] = ref.TimeStart.UTC().Format(time.RFC3339)
	}
	if !ref.TimeEnd.IsZero() {
		out["LOGPOND_SEGMENT_TIME_END"] = ref.TimeEnd.UTC().Format(time.RFC3339)
	}
	if ref.RowCount != 0 {
		out["LOGPOND_SEGMENT_ROW_COUNT"] = fmt.Sprintf("%d", ref.RowCount)
	}
	if ref.ParquetSHA256 != "" {
		out["LOGPOND_SEGMENT_PARQUET_SHA256"] = ref.ParquetSHA256
	}
	return out
}

// classifyExit maps an execResult onto the PRD §7.9.3 exit-code table.
// Returns nil on success, ErrUnsupported on exit 64, and a wrapped error
// otherwise.
func (b *ScriptBackend) classifyExit(mode string, out execResult, runErr error) error {
	if out.timedOut {
		return runErr
	}
	switch out.exitCode {
	case ExitSuccess:
		return nil
	case ExitUnsupportedMode:
		return ErrUnsupported
	case ExitPermanent:
		return fmt.Errorf("script %s: permanent failure (exit 65): %s", mode, firstLine(out.stderr))
	case ExitTemporary:
		return fmt.Errorf("script %s: temporary failure (exit 75): %s", mode, firstLine(out.stderr))
	case exitStartupSentinel:
		return fmt.Errorf("script %s: failed to start: %v", mode, runErr)
	default:
		return fmt.Errorf("script %s: failed (exit %d): %s", mode, out.exitCode, firstLine(out.stderr))
	}
}

// recordCap updates the cached capability when a real invocation reports
// exit 64 (or refutes a prior "no" with a successful run).
func (b *ScriptBackend) recordCap(mode string, exitCode int) {
	b.capMu.Lock()
	defer b.capMu.Unlock()
	set := func(dst *capabilityState) {
		switch exitCode {
		case ExitSuccess:
			*dst = capYes
		case ExitUnsupportedMode:
			*dst = capNo
		}
	}
	switch mode {
	case ModeArchive:
		set(&b.caps.archive)
	case ModeVerify:
		set(&b.caps.verify)
	case ModeRetrieve:
		set(&b.caps.retrieve)
	}
}

func (b *ScriptBackend) cachedCap(mode string) capabilityState {
	b.capMu.Lock()
	defer b.capMu.Unlock()
	switch mode {
	case ModeArchive:
		return b.caps.archive
	case ModeVerify:
		return b.caps.verify
	case ModeRetrieve:
		return b.caps.retrieve
	}
	return capUnknown
}

func (b *ScriptBackend) mkScratchDir() (string, error) {
	return os.MkdirTemp(b.workDir, "inv-")
}

// cappedBuffer is an io.Writer that accepts up to limit bytes and then
// silently drops the rest. The first overflow triggers a single
// truncation marker appended to the buffer.
type cappedBuffer struct {
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func newCappedBuffer(limit int) *cappedBuffer { return &cappedBuffer{limit: limit} }

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if c.buf.Len() >= c.limit {
		if !c.truncated {
			c.truncated = true
			c.buf.WriteString("\n...[truncated]")
		}
		return len(p), nil
	}
	remaining := c.limit - c.buf.Len()
	if len(p) <= remaining {
		return c.buf.Write(p)
	}
	c.buf.Write(p[:remaining])
	c.truncated = true
	c.buf.WriteString("\n...[truncated]")
	return len(p), nil
}

func (c *cappedBuffer) String() string { return c.buf.String() }

// extractArchiveRef returns the value following the final
// LOGPOND_ARCHIVE_REF= line in stdout, trimmed. Empty string when no
// such line is present.
func extractArchiveRef(stdout string) string {
	last := ""
	for _, line := range strings.Split(stdout, "\n") {
		trimmed := strings.TrimRight(line, "\r")
		if !strings.HasPrefix(trimmed, archiveRefPrefix) {
			continue
		}
		last = strings.TrimSpace(strings.TrimPrefix(trimmed, archiveRefPrefix))
	}
	return last
}

func firstLine(s string) string {
	s = strings.TrimRight(s, "\n")
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}

func appendBlock(existing, label, addition string) string {
	if addition == "" {
		return existing
	}
	if existing == "" {
		return fmt.Sprintf("--- %s ---\n%s", label, addition)
	}
	return existing + fmt.Sprintf("\n--- %s ---\n%s", label, addition)
}

func capYesOrUnknown(c capabilityState) bool {
	return c != capNo
}

func capString(c capabilityState) string {
	switch c {
	case capYes:
		return "yes"
	case capNo:
		return "no"
	default:
		return "unknown"
	}
}

func cloneEnv(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

