package ws

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/dokku/logpond/internal/ingest"
)

func newTestServer(t *testing.T, opts Options) (*httptest.Server, *Server) {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Fanout == nil {
		opts.Fanout = ingest.NewFanout()
	}
	srv := New(opts)
	ts := httptest.NewServer(http.HandlerFunc(srv.Handle))
	t.Cleanup(ts.Close)
	return ts, srv
}

func dial(t *testing.T, ts *httptest.Server) (*websocket.Conn, *http.Response) {
	t.Helper()
	url := "ws" + strings.TrimPrefix(ts.URL, "http")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn, resp
}

func readJSON(t *testing.T, conn *websocket.Conn, ctx context.Context, into any) {
	t.Helper()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := json.Unmarshal(data, into); err != nil {
		t.Fatalf("unmarshal %q: %v", data, err)
	}
}

func sendJSON(t *testing.T, conn *websocket.Conn, ctx context.Context, msg any) {
	t.Helper()
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestWS_HappyPath(t *testing.T) {
	fan := ingest.NewFanout()
	ts, _ := newTestServer(t, Options{Fanout: fan})

	conn, _ := dial(t, ts)
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sendJSON(t, conn, ctx, map[string]string{"q": "service:api"})

	var ack statusMessage
	readJSON(t, conn, ctx, &ack)
	if !ack.Subscribed {
		t.Fatalf("expected subscribed=true, got %+v", ack)
	}

	// Wait until the connection is registered as a fanout subscriber so
	// publishing reaches it.
	waitFor(t, func() bool { return fan.SubscriberCount() >= 1 }, time.Second)
	fan.Publish([]ingest.Event{
		{Timestamp: time.Now().UTC(), Service: "web", Message: "should not match"},
		{Timestamp: time.Now().UTC(), Service: "api", Message: "yes please"},
	})

	var ev eventMessage
	readJSON(t, conn, ctx, &ev)
	if ev.Type != "event" {
		t.Fatalf("expected event message, got %+v", ev)
	}
	if ev.Event.Service == nil || *ev.Event.Service != "api" {
		t.Fatalf("expected api event, got %+v", ev.Event)
	}
}

func TestWS_EmptyFilterCloses4001(t *testing.T) {
	ts, _ := newTestServer(t, Options{})
	conn, _ := dial(t, ts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sendJSON(t, conn, ctx, map[string]string{})

	_, _, err := conn.Read(ctx)
	if err == nil {
		t.Fatal("expected close, got nil")
	}
	if code := websocket.CloseStatus(err); code != CloseFilterRequired {
		t.Fatalf("close code = %d, want %d", code, CloseFilterRequired)
	}
}

func TestWS_InvalidFilterCloses4002(t *testing.T) {
	ts, _ := newTestServer(t, Options{})
	conn, _ := dial(t, ts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Filter that fails query.Validate (missing field).
	raw := json.RawMessage(`{"op":"eq"}`)
	sendJSON(t, conn, ctx, map[string]any{"filter": raw})

	_, _, err := conn.Read(ctx)
	if err == nil {
		t.Fatal("expected close, got nil")
	}
	if code := websocket.CloseStatus(err); code != CloseInvalidFilter {
		t.Fatalf("close code = %d, want %d", code, CloseInvalidFilter)
	}
}

func TestWS_SubscriptionUpdateChangesFilter(t *testing.T) {
	fan := ingest.NewFanout()
	ts, _ := newTestServer(t, Options{Fanout: fan})
	conn, _ := dial(t, ts)
	defer conn.Close(websocket.StatusNormalClosure, "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sendJSON(t, conn, ctx, map[string]string{"q": "service:api"})
	var ack statusMessage
	readJSON(t, conn, ctx, &ack)
	waitFor(t, func() bool { return fan.SubscriberCount() >= 1 }, time.Second)

	// Update subscription to web.
	sendJSON(t, conn, ctx, map[string]string{"q": "service:web"})
	readJSON(t, conn, ctx, &ack) // status ack for the new subscription

	fan.Publish([]ingest.Event{
		{Timestamp: time.Now().UTC(), Service: "api", Message: "ignored"},
		{Timestamp: time.Now().UTC(), Service: "web", Message: "delivered"},
	})

	var ev eventMessage
	readJSON(t, conn, ctx, &ev)
	if ev.Event.Service == nil || *ev.Event.Service != "web" {
		t.Fatalf("expected web event, got %+v", ev.Event)
	}
}

func TestWS_RateCapDropsAndReports(t *testing.T) {
	fan := ingest.NewFanout()
	ts, _ := newTestServer(t, Options{Fanout: fan, RateCapPerSecond: 2})
	conn, _ := dial(t, ts)
	defer conn.Close(websocket.StatusNormalClosure, "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sendJSON(t, conn, ctx, map[string]string{"q": "service:api"})
	var ack statusMessage
	readJSON(t, conn, ctx, &ack)
	waitFor(t, func() bool { return fan.SubscriberCount() >= 1 }, time.Second)

	batch := make([]ingest.Event, 5)
	for i := range batch {
		batch[i] = ingest.Event{Timestamp: time.Now().UTC(), Service: "api", Message: "msg"}
	}
	fan.Publish(batch)

	// First two events come through; the third frame should be a status
	// message announcing rate_capped=true.
	for i := 0; i < 2; i++ {
		var ev eventMessage
		readJSON(t, conn, ctx, &ev)
		if ev.Type != "event" {
			t.Fatalf("frame %d: expected event, got %+v", i, ev)
		}
	}
	var status statusMessage
	readJSON(t, conn, ctx, &status)
	if !status.RateCapped {
		t.Fatalf("expected rate_capped=true, got %+v", status)
	}
	if status.EventsDropped == 0 {
		t.Fatalf("expected events_dropped > 0, got %d", status.EventsDropped)
	}
}

func TestWS_ConnectionCapCloses4004(t *testing.T) {
	fan := ingest.NewFanout()
	ts, _ := newTestServer(t, Options{Fanout: fan, MaxClients: 2})

	hold1, _ := dial(t, ts)
	defer hold1.Close(websocket.StatusNormalClosure, "")
	hold2, _ := dial(t, ts)
	defer hold2.Close(websocket.StatusNormalClosure, "")

	// Open initial subscriptions to keep the connections alive.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, c := range []*websocket.Conn{hold1, hold2} {
		sendJSON(t, c, ctx, map[string]string{"q": "service:api"})
		var ack statusMessage
		readJSON(t, c, ctx, &ack)
	}

	// Third connection should be rejected with 4004.
	extra, _ := dial(t, ts)
	_, _, err := extra.Read(ctx)
	if err == nil {
		t.Fatal("expected close, got nil")
	}
	if code := websocket.CloseStatus(err); code != CloseTooManyClients {
		t.Fatalf("close code = %d, want %d", code, CloseTooManyClients)
	}
}

func TestWS_SlowClientCloses4003(t *testing.T) {
	fan := ingest.NewFanout()
	closeCh := make(chan websocket.StatusCode, 1)
	onClose := func(code websocket.StatusCode) {
		select {
		case closeCh <- code:
		default:
		}
	}

	ts, _ := newTestServer(t, Options{
		Fanout:            fan,
		SlowClientGrace:   100 * time.Millisecond,
		SendQueueCapacity: 4,
		RateCapPerSecond:  10000,
		OnClose:           onClose,
	})

	conn, _ := dial(t, ts)
	defer conn.Close(websocket.StatusInternalError, "test done")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sendJSON(t, conn, ctx, map[string]string{"q": "service:api"})

	var ack statusMessage
	readJSON(t, conn, ctx, &ack)
	waitFor(t, func() bool { return fan.SubscriberCount() >= 1 }, time.Second)

	// Stop reading on the client side. Publish a high volume of large
	// events to back up the writer goroutine; once the write times out
	// the server fires onClose with 4003.
	stopPublish := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		bigRaw := strings.Repeat("x", 8192)
		batch := make([]ingest.Event, 64)
		for i := range batch {
			batch[i] = ingest.Event{
				Timestamp: time.Now().UTC(),
				Service:   "api",
				Message:   "msg",
				Raw:       bigRaw,
			}
		}
		for {
			select {
			case <-stopPublish:
				return
			default:
				fan.Publish(batch)
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()
	defer func() {
		close(stopPublish)
		wg.Wait()
	}()

	select {
	case code := <-closeCh:
		if code != CloseSlowClient {
			t.Fatalf("got close code %d, want %d", code, CloseSlowClient)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("server did not detect slow client within 4s")
	}
}

func TestWS_HeartbeatSent(t *testing.T) {
	fan := ingest.NewFanout()
	ts, _ := newTestServer(t, Options{Fanout: fan, Heartbeat: 100 * time.Millisecond})
	conn, _ := dial(t, ts)
	defer conn.Close(websocket.StatusNormalClosure, "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sendJSON(t, conn, ctx, map[string]string{"q": "service:api"})

	var ack statusMessage
	readJSON(t, conn, ctx, &ack)
	if ack.Type != "status" {
		t.Fatalf("expected first frame = status, got %+v", ack)
	}

	var hb statusMessage
	readJSON(t, conn, ctx, &hb)
	if hb.Type != "status" {
		t.Fatalf("expected heartbeat status, got %+v", hb)
	}
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
