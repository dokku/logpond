package ingest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestBuffer_AppendDrainFIFO(t *testing.T) {
	b := NewBuffer(10)
	events := []Event{{Message: "a"}, {Message: "b"}, {Message: "c"}}
	if err := b.Append(events); err != nil {
		t.Fatalf("append: %v", err)
	}
	if b.Len() != 3 {
		t.Fatalf("len: got %d", b.Len())
	}
	out := b.Drain(2)
	if len(out) != 2 || out[0].Message != "a" || out[1].Message != "b" {
		t.Errorf("drain order: %+v", out)
	}
	out = b.Drain(10)
	if len(out) != 1 || out[0].Message != "c" {
		t.Errorf("drain remainder: %+v", out)
	}
	if b.Len() != 0 {
		t.Errorf("len after drain: %d", b.Len())
	}
}

func TestBuffer_AtomicFullBatchRejection(t *testing.T) {
	b := NewBuffer(3)
	if err := b.Append([]Event{{Message: "x"}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err := b.Append([]Event{{Message: "y"}, {Message: "z"}, {Message: "w"}})
	if !errors.Is(err, ErrBufferFull) {
		t.Fatalf("expected ErrBufferFull, got %v", err)
	}
	// Existing event must remain; the rejected batch is not partial.
	if b.Len() != 1 {
		t.Errorf("expected 1 remaining, got %d", b.Len())
	}
}

func TestBuffer_FillRatio(t *testing.T) {
	b := NewBuffer(4)
	_ = b.Append([]Event{{Message: "a"}, {Message: "b"}})
	if got := b.FillRatio(); got != 0.5 {
		t.Errorf("fill ratio: got %v", got)
	}
	_ = b.Drain(10)
	if got := b.FillRatio(); got != 0 {
		t.Errorf("fill ratio after drain: got %v", got)
	}
}

func TestBuffer_AppendEmptyIsNoop(t *testing.T) {
	b := NewBuffer(2)
	if err := b.Append(nil); err != nil {
		t.Fatalf("nil append: %v", err)
	}
	if err := b.Append([]Event{}); err != nil {
		t.Fatalf("empty append: %v", err)
	}
}

func TestFlusher_DrainsOnInterval(t *testing.T) {
	b := NewBuffer(64)
	if err := b.Append([]Event{{Message: "x"}, {Message: "y"}}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var (
		mu   sync.Mutex
		seen [][]Event
	)
	fn := func(_ context.Context, batch []Event) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, batch)
	}
	f := NewFlusher(b, 20*time.Millisecond, 16, fn, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		f.Run(ctx)
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := len(seen)
		mu.Unlock()
		if got > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatalf("flusher never ran")
	}
	total := 0
	for _, batch := range seen {
		total += len(batch)
	}
	if total != 2 {
		t.Errorf("total flushed: %d", total)
	}
}

func TestFlusher_HalfFullTriggersImmediateDrain(t *testing.T) {
	b := NewBuffer(10)
	ch := make(chan []Event, 4)
	fn := func(_ context.Context, batch []Event) { ch <- batch }
	f := NewFlusher(b, 10*time.Second, 10, fn, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Run(ctx)

	// Filling to 5 (half of 10) must immediately wake the flusher.
	if err := b.Append(make([]Event, 5)); err != nil {
		t.Fatalf("append: %v", err)
	}
	select {
	case batch := <-ch:
		if len(batch) != 5 {
			t.Errorf("batch: got %d", len(batch))
		}
	case <-time.After(time.Second):
		t.Fatalf("flusher did not wake on half-full signal")
	}
}
