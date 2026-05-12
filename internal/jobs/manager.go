// Package jobs persists long-running operations (archive, rehydrate,
// verify) so the HTTP layer can return 202 + a status URL while the
// work continues in the background. Records live in the catalog's jobs
// table (PRD §10.2).
package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/dokku/logpond/internal/catalog"
)

// Job types recorded in jobs.type.
const (
	TypeArchive   = "archive"
	TypeRehydrate = "rehydrate"
	TypeVerify    = "verify"
)

// Default retention for completed/failed jobs (PRD §7.9: 7 days).
const DefaultRetention = 7 * 24 * time.Hour

// Manager owns the in-process job lifecycle: creating new rows,
// streaming progress to the catalog, capturing the final result.
type Manager struct {
	catalog *catalog.Catalog
	logger  *slog.Logger
	now     func() time.Time

	mu       sync.Mutex
	cancels  map[string]context.CancelFunc // cancellable handles for active jobs
	retainer time.Duration
}

// Options configures a Manager.
type Options struct {
	Catalog   *catalog.Catalog
	Logger    *slog.Logger
	Now       func() time.Time
	Retention time.Duration
}

// New constructs a Manager. Catalog is required.
func New(opts Options) (*Manager, error) {
	if opts.Catalog == nil {
		return nil, errors.New("jobs: Catalog is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	if opts.Retention <= 0 {
		opts.Retention = DefaultRetention
	}
	return &Manager{
		catalog:  opts.Catalog,
		logger:   opts.Logger,
		now:      opts.Now,
		cancels:  map[string]context.CancelFunc{},
		retainer: opts.Retention,
	}, nil
}

// IDPrefix maps each job type to the short id prefix used in /api
// responses (PRD §13.9: "arc-7c1f", §13.10: "reh-3a02").
var idPrefix = map[string]string{
	TypeArchive:   "arc",
	TypeRehydrate: "reh",
	TypeVerify:    "ver",
}

// NewID returns a short, type-prefixed job id of the form `arc-<8 hex>`.
func NewID(jobType string) string {
	p, ok := idPrefix[jobType]
	if !ok {
		p = "job"
	}
	return p + "-" + uuid.NewString()[:8]
}

// Progress is the JSON-encoded progress payload (PRD §13.23).
type Progress struct {
	Completed  int   `json:"completed,omitempty"`
	Total      int   `json:"total,omitempty"`
	BytesDone  int64 `json:"bytes_done,omitempty"`
	BytesTotal int64 `json:"bytes_total,omitempty"`
}

// CreatePayload describes what a fresh job should track.
type CreatePayload struct {
	Type       string
	Payload    any
	TotalItems int
	TotalBytes int64
}

// Create persists a pending job and returns its id.
func (m *Manager) Create(ctx context.Context, p CreatePayload) (string, error) {
	id := NewID(p.Type)
	payloadJSON, err := json.Marshal(p.Payload)
	if err != nil {
		return "", fmt.Errorf("encoding job payload: %w", err)
	}
	progress := Progress{Total: p.TotalItems, BytesTotal: p.TotalBytes}
	progressJSON, err := json.Marshal(progress)
	if err != nil {
		return "", fmt.Errorf("encoding progress: %w", err)
	}
	if err := m.catalog.InsertJob(ctx, catalog.Job{
		ID:           id,
		Type:         p.Type,
		State:        catalog.JobStatePending,
		PayloadJSON:  string(payloadJSON),
		ProgressJSON: sql.NullString{String: string(progressJSON), Valid: true},
	}); err != nil {
		return "", err
	}
	return id, nil
}

// Start transitions the job to running and records started_at.
func (m *Manager) Start(ctx context.Context, id string) error {
	now := m.now().UTC()
	state := catalog.JobStateRunning
	return m.catalog.UpdateJob(ctx, id, catalog.JobUpdate{
		State:     &state,
		StartedAt: &now,
	})
}

// UpdateProgress merges the partial progress payload into the existing
// progress record. Total/BytesTotal of zero are interpreted as "leave
// unchanged"; Completed/BytesDone are overwritten.
func (m *Manager) UpdateProgress(ctx context.Context, id string, p Progress) error {
	existing, err := m.catalog.GetJob(ctx, id)
	if err != nil {
		return err
	}
	cur := Progress{}
	if existing.ProgressJSON.Valid {
		_ = json.Unmarshal([]byte(existing.ProgressJSON.String), &cur)
	}
	cur.Completed = p.Completed
	cur.BytesDone = p.BytesDone
	if p.Total > 0 {
		cur.Total = p.Total
	}
	if p.BytesTotal > 0 {
		cur.BytesTotal = p.BytesTotal
	}
	buf, err := json.Marshal(cur)
	if err != nil {
		return err
	}
	s := string(buf)
	return m.catalog.UpdateJob(ctx, id, catalog.JobUpdate{ProgressJSON: &s})
}

// AttachScriptOutput overwrites the script_stdout/script_stderr columns
// for the job row. Empty strings clear the corresponding column.
func (m *Manager) AttachScriptOutput(ctx context.Context, id, stdout, stderr string) error {
	return m.catalog.UpdateJob(ctx, id, catalog.JobUpdate{
		ScriptStdout: &stdout,
		ScriptStderr: &stderr,
	})
}

// Complete marks the job as finished successfully.
func (m *Manager) Complete(ctx context.Context, id string) error {
	now := m.now().UTC()
	state := catalog.JobStateCompleted
	m.clearCancel(id)
	return m.catalog.UpdateJob(ctx, id, catalog.JobUpdate{
		State:      &state,
		FinishedAt: &now,
	})
}

// Fail marks the job as failed and stores the error envelope.
func (m *Manager) Fail(ctx context.Context, id string, errMsg string) error {
	now := m.now().UTC()
	state := catalog.JobStateFailed
	envelope := map[string]string{"message": errMsg}
	buf, _ := json.Marshal(envelope)
	s := string(buf)
	m.clearCancel(id)
	return m.catalog.UpdateJob(ctx, id, catalog.JobUpdate{
		State:      &state,
		FinishedAt: &now,
		ErrorJSON:  &s,
	})
}

// Run executes fn in a background goroutine, tracking the lifecycle.
// The caller receives the job id and can poll Get/GetView; cancellation
// is wired through ctx.
func (m *Manager) Run(parent context.Context, p CreatePayload, fn func(ctx context.Context, id string) error) (string, error) {
	id, err := m.Create(parent, p)
	if err != nil {
		return "", err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.cancels[id] = cancel
	m.mu.Unlock()

	go func() {
		defer cancel()
		if err := m.Start(runCtx, id); err != nil {
			m.logger.Warn("jobs: starting", "id", id, "err", err)
		}
		if err := fn(runCtx, id); err != nil {
			if ferr := m.Fail(context.Background(), id, err.Error()); ferr != nil {
				m.logger.Warn("jobs: failing", "id", id, "err", ferr)
			}
			return
		}
		if err := m.Complete(context.Background(), id); err != nil {
			m.logger.Warn("jobs: completing", "id", id, "err", err)
		}
	}()
	return id, nil
}

// RunForeground executes fn synchronously while still tracking the
// lifecycle. Useful when the caller wants the result inline (e.g.
// verify endpoints) but still wants a row in the jobs table.
func (m *Manager) RunForeground(ctx context.Context, p CreatePayload, fn func(ctx context.Context, id string) error) (string, error) {
	id, err := m.Create(ctx, p)
	if err != nil {
		return "", err
	}
	if err := m.Start(ctx, id); err != nil {
		return id, err
	}
	if err := fn(ctx, id); err != nil {
		_ = m.Fail(ctx, id, err.Error())
		return id, err
	}
	return id, m.Complete(ctx, id)
}

// JobView is the API-facing shape of a job (PRD §13.23).
type JobView struct {
	ID           string    `json:"job_id"`
	Type         string    `json:"type"`
	State        string    `json:"state"`
	Progress     Progress  `json:"progress"`
	StartedAt    *time.Time `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at"`
	Error        any        `json:"error"`
	ScriptStdout *string    `json:"script_stdout"`
	ScriptStderr *string    `json:"script_stderr"`
}

// Get returns the JSON view of a job.
func (m *Manager) Get(ctx context.Context, id string) (JobView, error) {
	j, err := m.catalog.GetJob(ctx, id)
	if err != nil {
		return JobView{}, err
	}
	v := JobView{
		ID:    j.ID,
		Type:  j.Type,
		State: j.State,
	}
	if j.ProgressJSON.Valid {
		_ = json.Unmarshal([]byte(j.ProgressJSON.String), &v.Progress)
	}
	if j.StartedAt.Valid {
		t := j.StartedAt.Time.UTC()
		v.StartedAt = &t
	}
	if j.FinishedAt.Valid {
		t := j.FinishedAt.Time.UTC()
		v.FinishedAt = &t
	}
	if j.ErrorJSON.Valid {
		var raw any
		if err := json.Unmarshal([]byte(j.ErrorJSON.String), &raw); err == nil {
			v.Error = raw
		} else {
			v.Error = j.ErrorJSON.String
		}
	}
	if j.ScriptStdout.Valid {
		s := j.ScriptStdout.String
		v.ScriptStdout = &s
	}
	if j.ScriptStderr.Valid {
		s := j.ScriptStderr.String
		v.ScriptStderr = &s
	}
	return v, nil
}

// GC removes completed/failed jobs whose finished_at is older than the
// retention window. Returns the number of rows removed.
func (m *Manager) GC(ctx context.Context) (int64, error) {
	cutoff := m.now().UTC().Add(-m.retainer)
	return m.catalog.DeleteJobsOlderThan(ctx, cutoff)
}

// RunGCLoop blocks until ctx is cancelled, invoking GC on interval.
// interval defaults to 1 hour.
func (m *Manager) RunGCLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Hour
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := m.GC(ctx); err != nil {
				m.logger.Warn("jobs: gc", "err", err)
			} else if n > 0 {
				m.logger.Info("jobs: gc", "removed", n)
			}
		}
	}
}

func (m *Manager) clearCancel(id string) {
	m.mu.Lock()
	delete(m.cancels, id)
	m.mu.Unlock()
}

// Cancel cancels the run context for an in-flight job. No-op for
// finished or unknown ids.
func (m *Manager) Cancel(id string) {
	m.mu.Lock()
	cancel, ok := m.cancels[id]
	delete(m.cancels, id)
	m.mu.Unlock()
	if ok {
		cancel()
	}
}
