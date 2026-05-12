// Package retention implements the local-disk retention policy
// described in PRD §7.8. The evaluator inspects sealed, archived (with
// a local file), and rehydrated segments and decides which can be
// removed under the configured max_age / max_size policy. Active
// segments are never touched, and segments sealed within the protected
// window (default one hour) are spared regardless of policy.
package retention

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/dokku/logpond/internal/catalog"
)

// Default tunables. ProtectedWindow matches §7.8 ("segments sealed
// within the last hour"). EvaluationInterval matches §7.8's "every 5
// minutes (configurable)" default.
const (
	DefaultProtectedWindow    = time.Hour
	DefaultEvaluationInterval = 5 * time.Minute
)

// Action verbs reported in Result.Actions. The wording matches the
// example in PRD §13.21.
const (
	ActionDelete            = "delete"
	ActionArchiveThenDelete = "archive_then_delete"
)

// Action describes what the evaluator decided to do (or would do, in
// dry-run mode) for a single segment.
type Action struct {
	SegmentID string    `json:"segment_id"`
	Action    string    `json:"action"`
	Reason    string    `json:"reason"`
	SizeBytes int64     `json:"size_bytes"`
	TimeEnd   time.Time `json:"time_end"`
	State     string    `json:"state"`
	NextState string    `json:"next_state,omitempty"`

	// Executed records whether the action was actually performed
	// during Run(); always false for dry runs and for actions that
	// require archival (which Phase 7 cannot perform).
	Executed bool   `json:"executed"`
	Error    string `json:"error,omitempty"`
}

// Result is the response envelope returned by Plan and Run.
type Result struct {
	Evaluated int      `json:"evaluated"`
	Actions   []Action `json:"actions"`
	DryRun    bool     `json:"dry_run"`
}

// Options configures an Evaluator.
type Options struct {
	Catalog *catalog.Catalog

	// MaxAge zero disables the age policy.
	MaxAge time.Duration
	// MaxSize zero disables the size policy. Bytes.
	MaxSize int64
	// ProtectedWindow defaults to one hour when zero (§7.8).
	ProtectedWindow time.Duration
	// ArchiveBeforeDelete mirrors the config field of the same name.
	// When true and a segment is not yet archived, the evaluator reports
	// `archive_then_delete` but does not delete — Phase 8 wires actual
	// archival.
	ArchiveBeforeDelete bool
	// BackendActive is true when an archive backend is configured (i.e.
	// `archive.backend` is not `none`). When false, archive_before_delete
	// is treated as best-effort: nothing gets archived, so the only safe
	// path is to keep the segments around. Plan() still emits
	// `archive_then_delete` actions so the operator sees the stuck state.
	BackendActive bool

	Now    func() time.Time
	Logger *slog.Logger
}

// Evaluator owns the retention policy state. New goroutines may call
// Plan/Run concurrently; the catalog mediates writes.
type Evaluator struct {
	opts Options
}

// New constructs an Evaluator. Catalog is required; other fields fall
// back to defaults documented on Options.
func New(opts Options) (*Evaluator, error) {
	if opts.Catalog == nil {
		return nil, errors.New("retention: Catalog is required")
	}
	if opts.ProtectedWindow <= 0 {
		opts.ProtectedWindow = DefaultProtectedWindow
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Evaluator{opts: opts}, nil
}

// Configured reports whether at least one policy axis (max_age or
// max_size) is set. When false, Plan returns an empty Result.
func (e *Evaluator) Configured() bool {
	return e.opts.MaxAge > 0 || e.opts.MaxSize > 0
}

// Plan computes the actions retention would take without changing any
// state. Suitable for dry-run reporting (§13.21).
func (e *Evaluator) Plan(ctx context.Context) (Result, error) {
	r, err := e.plan(ctx)
	if err != nil {
		return Result{}, err
	}
	r.DryRun = true
	return r, nil
}

// Run plans and then executes the deletions. When dryRun is true, the
// behaviour is equivalent to Plan. Actions that require archival
// (`archive_then_delete`) are reported but not performed in Phase 7.
func (e *Evaluator) Run(ctx context.Context, dryRun bool) (Result, error) {
	r, err := e.plan(ctx)
	if err != nil {
		return Result{}, err
	}
	r.DryRun = dryRun
	if dryRun {
		return r, nil
	}
	for i := range r.Actions {
		a := &r.Actions[i]
		if a.Action != ActionDelete {
			// archive_then_delete actions wait for Phase 8 to wire the
			// archive backend. The next retention cycle picks them up
			// once archived_at is populated.
			continue
		}
		if err := e.execute(ctx, a); err != nil {
			a.Error = err.Error()
			e.opts.Logger.Warn("retention: delete failed",
				"segment_id", a.SegmentID, "err", err)
			continue
		}
		a.Executed = true
	}
	return r, nil
}


func (e *Evaluator) plan(ctx context.Context) (Result, error) {
	segs, err := e.opts.Catalog.ListLocalSegments(ctx)
	if err != nil {
		return Result{}, err
	}
	now := e.opts.Now().UTC()
	protectedUntil := now.Add(-e.opts.ProtectedWindow)

	type candidate struct {
		seg       catalog.Segment
		protected bool
		ageReason string
	}
	candidates := make([]candidate, 0, len(segs))
	for _, s := range segs {
		c := candidate{seg: s}
		if s.State == catalog.StateSealed && s.SealedAt.Valid && s.SealedAt.Time.After(protectedUntil) {
			c.protected = true
		}
		if e.opts.MaxAge > 0 {
			ageCutoff := now.Add(-e.opts.MaxAge)
			if s.TimeEnd.Before(ageCutoff) {
				c.ageReason = fmt.Sprintf("age > %s", e.opts.MaxAge)
			}
		}
		candidates = append(candidates, c)
	}

	selected := map[string]string{} // segment_id -> reason
	for _, c := range candidates {
		if c.protected {
			continue
		}
		if c.ageReason != "" {
			selected[c.seg.ID] = c.ageReason
		}
	}

	if e.opts.MaxSize > 0 {
		var total int64
		for _, c := range candidates {
			total += c.seg.SizeBytes
		}
		if total > e.opts.MaxSize {
			// candidates is already oldest-first (ListLocalSegments
			// orders by time_end ASC). Evict oldest non-protected
			// segments until total drops under the cap.
			over := total - e.opts.MaxSize
			for _, c := range candidates {
				if over <= 0 {
					break
				}
				if c.protected {
					continue
				}
				if _, already := selected[c.seg.ID]; !already {
					selected[c.seg.ID] = fmt.Sprintf("size > %d bytes", e.opts.MaxSize)
				}
				over -= c.seg.SizeBytes
			}
		}
	}

	actions := make([]Action, 0, len(selected))
	for _, c := range candidates {
		reason, ok := selected[c.seg.ID]
		if !ok {
			continue
		}
		a := Action{
			SegmentID: c.seg.ID,
			Reason:    reason,
			SizeBytes: c.seg.SizeBytes,
			TimeEnd:   c.seg.TimeEnd,
			State:     c.seg.State,
		}
		a.Action, a.NextState = e.decide(c.seg)
		actions = append(actions, a)
	}

	return Result{
		Evaluated: len(segs),
		Actions:   actions,
	}, nil
}

// decide returns the verb and next state for a selected segment.
// sealed + archive_before_delete + no archive yet → archive_then_delete
// sealed + (no archive backend or archive_before_delete=false) → delete → lost
// archived (with local file) → delete → archived (file gone, archive remains)
// rehydrated → delete → archived (TTL expiry path).
func (e *Evaluator) decide(s catalog.Segment) (string, string) {
	archived := s.S3URL.Valid || s.ArchiveRef.Valid || s.State == catalog.StateArchived
	switch s.State {
	case catalog.StateSealed:
		if e.opts.ArchiveBeforeDelete && e.opts.BackendActive && !archived {
			return ActionArchiveThenDelete, catalog.StateSealed
		}
		return ActionDelete, catalog.StateLost
	case catalog.StateArchived:
		return ActionDelete, catalog.StateArchived
	case catalog.StateRehydrated:
		return ActionDelete, catalog.StateArchived
	default:
		// Should never appear in ListLocalSegments, but keep the
		// transition deterministic.
		return ActionDelete, catalog.StateLost
	}
}

func (e *Evaluator) execute(ctx context.Context, a *Action) error {
	seg, err := e.opts.Catalog.GetSegment(ctx, a.SegmentID)
	if err != nil {
		return fmt.Errorf("loading segment: %w", err)
	}
	if seg.LocalPath.Valid && seg.LocalPath.String != "" {
		if err := os.Remove(seg.LocalPath.String); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("removing %s: %w", seg.LocalPath.String, err)
		}
	}
	if err := e.opts.Catalog.ClearLocalFile(ctx, a.SegmentID, a.NextState); err != nil {
		return err
	}
	e.opts.Logger.Info("retention: deleted segment",
		"segment_id", a.SegmentID,
		"reason", a.Reason,
		"next_state", a.NextState,
	)
	return nil
}

// RunLoop blocks until ctx is cancelled, invoking Run(ctx, false) on
// each tick. interval defaults to DefaultEvaluationInterval.
func (e *Evaluator) RunLoop(ctx context.Context, interval time.Duration) {
	if !e.Configured() {
		e.opts.Logger.Info("retention: no policy configured; loop disabled")
		<-ctx.Done()
		return
	}
	if interval <= 0 {
		interval = DefaultEvaluationInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := e.Run(ctx, false); err != nil {
				e.opts.Logger.Warn("retention: cycle failed", "err", err)
			}
		}
	}
}
