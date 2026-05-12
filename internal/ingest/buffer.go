package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ErrBufferFull is returned by Buffer.Append when the buffer has no
// capacity left. The ingest handler translates this into HTTP 429.
var ErrBufferFull = errors.New("ingest buffer full")

// Buffer is the in-memory ring of accepted events. Producers (HTTP
// handlers) call Append; a single flush goroutine drains in batches.
type Buffer struct {
	mu     sync.Mutex
	events []Event
	cap    int
	notify chan struct{}
	fill   atomic.Int64 // mirrors len(events) for the metric gauge
}

// NewBuffer constructs a buffer with the given event capacity. Capacity
// must be > 0.
func NewBuffer(capacity int) *Buffer {
	if capacity <= 0 {
		capacity = 1
	}
	return &Buffer{
		events: make([]Event, 0, capacity),
		cap:    capacity,
		notify: make(chan struct{}, 1),
	}
}

// Cap returns the configured capacity in events.
func (b *Buffer) Cap() int { return b.cap }

// Len returns the current event count.
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.events)
}

// FillRatio returns the current ratio of len/cap in [0, 1]. Cheap; safe
// to read from the metrics goroutine.
func (b *Buffer) FillRatio() float64 {
	if b.cap == 0 {
		return 0
	}
	return float64(b.fill.Load()) / float64(b.cap)
}

// Append adds events to the buffer. The whole batch lands atomically:
// if it would not fit, none of it is appended and ErrBufferFull is
// returned. This matches PRD §7.1's "don't accept partial batches"
// requirement for the ingest handler.
func (b *Buffer) Append(events []Event) error {
	if len(events) == 0 {
		return nil
	}
	b.mu.Lock()
	if len(b.events)+len(events) > b.cap {
		b.mu.Unlock()
		return ErrBufferFull
	}
	b.events = append(b.events, events...)
	n := len(b.events)
	b.fill.Store(int64(n))
	halfFull := n*2 >= b.cap
	b.mu.Unlock()

	if halfFull {
		b.signal()
	}
	return nil
}

// Drain pulls up to maxN events off the head of the buffer.
func (b *Buffer) Drain(maxN int) []Event {
	if maxN <= 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.events) == 0 {
		return nil
	}
	n := maxN
	if n > len(b.events) {
		n = len(b.events)
	}
	out := make([]Event, n)
	copy(out, b.events[:n])
	rest := len(b.events) - n
	if rest > 0 {
		copy(b.events, b.events[n:])
	}
	b.events = b.events[:rest]
	b.fill.Store(int64(rest))
	return out
}

func (b *Buffer) signal() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

// FlushFunc is called by the flush goroutine with each drained batch.
// Phase 3 will replace the no-op flush with a DuckDB writer.
type FlushFunc func(ctx context.Context, batch []Event)

// Flusher periodically drains the buffer into a FlushFunc.
type Flusher struct {
	buf      *Buffer
	interval time.Duration
	batch    int
	flush    FlushFunc
	logger   *slog.Logger
}

// NewFlusher returns a Flusher that drains buf every interval (or
// sooner if the buffer crosses half-full) and hands each batch to fn.
// batch caps the size of one drain pass.
func NewFlusher(buf *Buffer, interval time.Duration, batch int, fn FlushFunc, logger *slog.Logger) *Flusher {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = time.Second
	}
	if batch <= 0 {
		batch = buf.Cap()
	}
	if fn == nil {
		fn = func(ctx context.Context, events []Event) {
			logger.Info("would flush events", "count", len(events))
		}
	}
	return &Flusher{buf: buf, interval: interval, batch: batch, flush: fn, logger: logger}
}

// Run blocks until ctx is cancelled, draining the buffer on each tick
// or half-full signal. Any remaining events at shutdown are flushed
// one last time.
func (f *Flusher) Run(ctx context.Context) {
	t := time.NewTicker(f.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			f.drainAll(context.Background())
			return
		case <-t.C:
			f.drainOnce(ctx)
		case <-f.buf.notify:
			f.drainOnce(ctx)
		}
	}
}

func (f *Flusher) drainOnce(ctx context.Context) {
	batch := f.buf.Drain(f.batch)
	if len(batch) == 0 {
		return
	}
	f.flush(ctx, batch)
}

func (f *Flusher) drainAll(ctx context.Context) {
	for {
		batch := f.buf.Drain(f.batch)
		if len(batch) == 0 {
			return
		}
		f.flush(ctx, batch)
	}
}
