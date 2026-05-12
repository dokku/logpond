package metrics

import (
	"log/slog"
	"sync"
	"testing"
	"time"
)

type fakeShedder struct {
	mu         sync.Mutex
	count      int
	accepting  bool
	shedTotal  int
}

func (f *fakeShedder) ClientCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count
}

func (f *fakeShedder) ShedOldest() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.count == 0 {
		return false
	}
	f.count--
	f.shedTotal++
	return true
}

func (f *fakeShedder) SetAcceptingNewClients(ok bool) {
	f.mu.Lock()
	f.accepting = ok
	f.mu.Unlock()
}

func TestWatchdog_NoMemoryLimit_NoOp(t *testing.T) {
	shed := &fakeShedder{count: 5, accepting: true}
	w := NewWatchdog(WatchdogOptions{
		MemoryLimit:  0,
		LiveTail:     shed,
		Logger:       slog.Default(),
		SustainedFor: 0,
		PollInterval: 0,
	})
	// Disable the cap explicitly so tick is a no-op.
	w.memLimit = 0
	var since time.Time
	w.tick(&since, time.Now())
	if shed.shedTotal != 0 {
		t.Fatalf("shed without limit: %d", shed.shedTotal)
	}
}

func TestWatchdog_SustainedPressure_ShedsOldest(t *testing.T) {
	shed := &fakeShedder{count: 3, accepting: true}
	w := NewWatchdog(WatchdogOptions{
		LiveTail:     shed,
		Logger:       slog.Default(),
		SustainedFor: 10 * time.Millisecond,
		PollInterval: time.Millisecond,
	})
	// Force a small memory limit so the live process is always "over".
	w.memLimit = 1
	w.threshold = 0.5

	var since time.Time
	start := time.Now()
	w.tick(&since, start) // first observation marks pressure but doesn't shed
	if shed.shedTotal != 0 {
		t.Fatalf("shed before sustained window: %d", shed.shedTotal)
	}
	// Cross the sustained-for threshold.
	w.tick(&since, start.Add(20*time.Millisecond))
	if shed.shedTotal != 1 {
		t.Fatalf("expected one shed; got %d", shed.shedTotal)
	}
	if shed.accepting {
		t.Fatalf("watchdog should refuse new clients while shedding")
	}
}

func TestWatchdog_PressureRelieved_RestoresAccept(t *testing.T) {
	shed := &fakeShedder{count: 0, accepting: false}
	w := NewWatchdog(WatchdogOptions{
		LiveTail:     shed,
		Logger:       slog.Default(),
		SustainedFor: 10 * time.Millisecond,
		PollInterval: time.Millisecond,
	})
	w.memLimit = 1 << 50
	w.threshold = 0.95

	// Make overSince non-zero to mimic prior pressure.
	since := time.Now().Add(-time.Hour)
	w.tick(&since, time.Now())
	if !shed.accepting {
		t.Fatalf("expected accepting=true after pressure relieved")
	}
}
