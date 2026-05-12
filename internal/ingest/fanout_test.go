package ingest

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFanout_DeliversToSubscribers(t *testing.T) {
	f := NewFanout()
	a := f.Subscribe(8)
	b := f.Subscribe(8)
	defer a.Cancel()
	defer b.Cancel()

	f.Publish([]Event{{Service: "x"}, {Service: "y"}})

	for _, sub := range []*Subscription{a, b} {
		for i, want := range []string{"x", "y"} {
			select {
			case ev := <-sub.Events:
				if ev.Service != want {
					t.Fatalf("event %d: got service %q, want %q", i, ev.Service, want)
				}
			case <-time.After(time.Second):
				t.Fatalf("subscriber timed out waiting for event %d", i)
			}
		}
	}
}

func TestFanout_DropsWhenQueueFull(t *testing.T) {
	f := NewFanout()
	sub := f.Subscribe(2)
	defer sub.Cancel()

	f.Publish([]Event{{Service: "a"}, {Service: "b"}, {Service: "c"}, {Service: "d"}})

	if got := sub.Dropped; got != 2 {
		t.Fatalf("Dropped=%d, want 2", got)
	}
	// Drain so the test doesn't leak.
	for len(sub.Events) > 0 {
		<-sub.Events
	}
}

func TestFanout_CancelStopsDelivery(t *testing.T) {
	f := NewFanout()
	sub := f.Subscribe(4)

	sub.Cancel()

	if got := f.SubscriberCount(); got != 0 {
		t.Fatalf("after cancel: SubscriberCount=%d, want 0", got)
	}
	// Publishing after cancel must not panic or deliver.
	f.Publish([]Event{{Service: "ignored"}})
	select {
	case <-sub.Done():
	default:
		t.Fatalf("Done channel not closed after Cancel")
	}
}

func TestFanout_ConcurrentPublishersAndSubscribers(t *testing.T) {
	f := NewFanout()
	const subscribers = 4
	subs := make([]*Subscription, subscribers)
	for i := range subs {
		subs[i] = f.Subscribe(1024)
		defer subs[i].Cancel()
	}

	var received [subscribers]atomic.Int64
	var wg sync.WaitGroup
	for i, s := range subs {
		wg.Add(1)
		go func(idx int, sub *Subscription) {
			defer wg.Done()
			deadline := time.NewTimer(2 * time.Second)
			defer deadline.Stop()
			for {
				select {
				case <-sub.Events:
					received[idx].Add(1)
				case <-deadline.C:
					return
				}
			}
		}(i, s)
	}

	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				f.Publish([]Event{{Service: "s"}})
			}
		}()
	}
	wg.Wait()

	for i := range received {
		if got := received[i].Load(); got == 0 {
			t.Fatalf("subscriber %d received nothing", i)
		}
	}
}
