package ingest

import "sync"

// Fanout broadcasts ingested events to N live subscribers. It is the
// pub-sub seam between the ingest path and the WebSocket live-tail
// server (PRD §7.6). Each subscriber has its own bounded send queue;
// publish drops events for any queue that is full so a slow client
// can't slow the ingest path or the other subscribers.
type Fanout struct {
	mu          sync.RWMutex
	subscribers map[*Subscription]struct{}
}

// Subscription is a live-tail consumer's view onto the fan-out. The
// owner reads Events; Closed signals that Cancel was called and no
// further events will be delivered. The Dropped counter tracks events
// the fan-out skipped because the queue was full.
type Subscription struct {
	Events   chan Event
	Dropped  uint64
	mu       sync.Mutex
	closed   bool
	closedCh chan struct{}
	parent   *Fanout
}

// NewFanout returns an empty fan-out. The zero value is also usable.
func NewFanout() *Fanout {
	return &Fanout{subscribers: make(map[*Subscription]struct{})}
}

// Subscribe registers a new subscriber with the given send-queue
// capacity. Capacity below 1 is clamped to 1.
func (f *Fanout) Subscribe(capacity int) *Subscription {
	if capacity < 1 {
		capacity = 1
	}
	sub := &Subscription{
		Events:   make(chan Event, capacity),
		closedCh: make(chan struct{}),
		parent:   f,
	}
	f.mu.Lock()
	if f.subscribers == nil {
		f.subscribers = make(map[*Subscription]struct{})
	}
	f.subscribers[sub] = struct{}{}
	f.mu.Unlock()
	return sub
}

// Publish broadcasts events to every active subscriber. Subscribers
// whose queue is full record a drop and skip the event; they do not
// block the publisher. Safe for concurrent callers.
func (f *Fanout) Publish(events []Event) {
	if len(events) == 0 {
		return
	}
	f.mu.RLock()
	subs := make([]*Subscription, 0, len(f.subscribers))
	for s := range f.subscribers {
		subs = append(subs, s)
	}
	f.mu.RUnlock()

	for _, s := range subs {
		for _, ev := range events {
			s.publish(ev)
		}
	}
}

// SubscriberCount returns the number of live subscribers.
func (f *Fanout) SubscriberCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.subscribers)
}

func (f *Fanout) remove(sub *Subscription) {
	f.mu.Lock()
	delete(f.subscribers, sub)
	f.mu.Unlock()
}

func (s *Subscription) publish(ev Event) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	select {
	case s.Events <- ev:
	default:
		s.Dropped++
	}
	s.mu.Unlock()
}

// Cancel deregisters the subscription from its fan-out and closes the
// Events channel. Safe to call multiple times.
func (s *Subscription) Cancel() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	close(s.closedCh)
	close(s.Events)
	s.mu.Unlock()
	if s.parent != nil {
		s.parent.remove(s)
	}
}

// Done returns a channel closed when the subscription is cancelled.
func (s *Subscription) Done() <-chan struct{} { return s.closedCh }
