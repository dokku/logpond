package jobs

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dokku/logpond/internal/catalog"
)

func newTestCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	dir := t.TempDir()
	cat, err := catalog.Open(context.Background(), filepath.Join(dir, "catalog.db"), nil)
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	return cat
}

func TestManager_RunCompletes(t *testing.T) {
	cat := newTestCatalog(t)
	mgr, err := New(Options{Catalog: cat})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	done := make(chan struct{})
	id, err := mgr.Run(context.Background(), CreatePayload{Type: TypeArchive, TotalItems: 1},
		func(ctx context.Context, id string) error {
			defer close(done)
			return mgr.UpdateProgress(ctx, id, Progress{Completed: 1, Total: 1})
		})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	<-done
	// Poll briefly for completion.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		v, err := mgr.Get(context.Background(), id)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if v.State == catalog.JobStateCompleted {
			if v.Progress.Completed != 1 || v.Progress.Total != 1 {
				t.Fatalf("progress not recorded: %+v", v.Progress)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job did not complete in time")
}

func TestManager_RunFails(t *testing.T) {
	cat := newTestCatalog(t)
	mgr, err := New(Options{Catalog: cat})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	id, err := mgr.Run(context.Background(), CreatePayload{Type: TypeArchive},
		func(ctx context.Context, id string) error {
			return errors.New("boom")
		})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		v, err := mgr.Get(context.Background(), id)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if v.State == catalog.JobStateFailed {
			if v.Error == nil {
				t.Fatal("expected error envelope")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job did not fail in time")
}

func TestManager_GC_RemovesOldCompleted(t *testing.T) {
	cat := newTestCatalog(t)
	clock := time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC)
	mgr, err := New(Options{
		Catalog:   cat,
		Retention: 24 * time.Hour,
		Now:       func() time.Time { return clock },
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_, err = mgr.RunForeground(context.Background(), CreatePayload{Type: TypeArchive},
		func(ctx context.Context, id string) error { return nil })
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	// Default retention is 7d, the job just completed. GC should keep it.
	if n, err := mgr.GC(context.Background()); err != nil || n != 0 {
		t.Fatalf("expected 0 removed; got %d err=%v", n, err)
	}
	// Advance the clock past the retention window.
	clock = clock.Add(2 * 24 * time.Hour)
	n, err := mgr.GC(context.Background())
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 removed; got %d", n)
	}
}

func TestNewID_TypePrefixed(t *testing.T) {
	if got := NewID(TypeArchive); got[:4] != "arc-" {
		t.Fatalf("expected arc- prefix, got %s", got)
	}
	if got := NewID(TypeRehydrate); got[:4] != "reh-" {
		t.Fatalf("expected reh- prefix, got %s", got)
	}
	if got := NewID(TypeVerify); got[:4] != "ver-" {
		t.Fatalf("expected ver- prefix, got %s", got)
	}
}
