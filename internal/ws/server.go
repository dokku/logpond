// Package ws implements Logpond's live-tail WebSocket endpoint
// (PRD §7.6, §13.7). Clients connect to /api/query/stream, send a
// subscription message describing the filter they want, and the
// server fans out matching events from the ingest pipeline as they
// arrive.
package ws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/dokku/logpond/internal/ingest"
	"github.com/dokku/logpond/internal/query"
	"github.com/dokku/logpond/internal/query/parser"
)

// Close codes defined by §13.7. The 4xxx range is reserved for
// application-defined close codes.
const (
	CloseFilterRequired websocket.StatusCode = 4001
	CloseInvalidFilter  websocket.StatusCode = 4002
	CloseSlowClient     websocket.StatusCode = 4003
	CloseTooManyClients websocket.StatusCode = 4004
)

// Defaults from PRD §7.6.
const (
	defaultRateCapPerSecond = 100
	defaultMaxClients       = 20
	defaultHeartbeat        = 30 * time.Second
	defaultSlowClientGrace  = 30 * time.Second
	defaultSendQueue        = 256
)

// Options bundles the wiring the server needs. Zero-valued knobs fall
// back to PRD defaults.
type Options struct {
	Fanout            *ingest.Fanout
	Logger            *slog.Logger
	MaxClients        int
	RateCapPerSecond  int
	Heartbeat         time.Duration
	SlowClientGrace   time.Duration
	SendQueueCapacity int

	// OnClose is invoked when the server initiates a close. Intended
	// for tests — the WebSocket close frame may not reach the client if
	// the underlying connection is wedged, so tests verify intent here.
	OnClose func(websocket.StatusCode)
}

// Server upgrades incoming HTTP requests to WebSocket connections and
// runs a per-connection goroutine for the lifetime of the stream.
type Server struct {
	fanout *ingest.Fanout
	logger *slog.Logger

	maxClients       int
	rateCapPerSecond int
	heartbeat        time.Duration
	slowClientGrace  time.Duration
	sendQueueCap     int
	onClose          func(websocket.StatusCode)

	clients atomic.Int64
}

// New constructs a Server. The fan-out must be non-nil; nil fan-out
// means the live-tail feature is disabled.
func New(opts Options) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.MaxClients <= 0 {
		opts.MaxClients = defaultMaxClients
	}
	if opts.RateCapPerSecond <= 0 {
		opts.RateCapPerSecond = defaultRateCapPerSecond
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = defaultHeartbeat
	}
	if opts.SlowClientGrace <= 0 {
		opts.SlowClientGrace = defaultSlowClientGrace
	}
	if opts.SendQueueCapacity <= 0 {
		opts.SendQueueCapacity = defaultSendQueue
	}
	return &Server{
		fanout:           opts.Fanout,
		logger:           opts.Logger,
		maxClients:       opts.MaxClients,
		rateCapPerSecond: opts.RateCapPerSecond,
		heartbeat:        opts.Heartbeat,
		slowClientGrace:  opts.SlowClientGrace,
		sendQueueCap:     opts.SendQueueCapacity,
		onClose:          opts.OnClose,
	}
}

// ClientCount returns the number of currently connected live-tail
// clients. Exposed for tests and admin diagnostics.
func (s *Server) ClientCount() int { return int(s.clients.Load()) }

// Handle upgrades the request and runs the connection loop. Mount on
// `GET /api/query/stream` (§13.7).
func (s *Server) Handle(w http.ResponseWriter, r *http.Request) {
	if s.fanout == nil {
		http.Error(w, "live tail disabled", http.StatusServiceUnavailable)
		return
	}

	// Per §7.6 the connection cap is global: bump the counter atomically
	// before the upgrade; over-cap connections still upgrade so the
	// client sees the 4004 close code.
	if int(s.clients.Add(1)) > s.maxClients {
		s.clients.Add(-1)
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
			OriginPatterns:     []string{"*"},
		})
		if err != nil {
			return
		}
		s.fireOnClose(CloseTooManyClients)
		_ = conn.Close(CloseTooManyClients, "connection cap reached")
		return
	}
	defer s.clients.Add(-1)

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
		OriginPatterns:     []string{"*"},
	})
	if err != nil {
		s.logger.Warn("ws accept failed", "err", err)
		return
	}

	s.runConnection(r.Context(), conn)
}

// statusMessage is the server's status frame (PRD §13.7).
type statusMessage struct {
	Type          string `json:"type"`
	Subscribed    bool   `json:"subscribed"`
	RateCapped    bool   `json:"rate_capped"`
	EventsDropped uint64 `json:"events_dropped"`
	FilterSummary string `json:"filter_summary"`
}

// eventMessage is the server's per-event frame.
type eventMessage struct {
	Type  string       `json:"type"`
	Event eventPayload `json:"event"`
}

// eventPayload is shaped like a row in /api/query's response so the
// browser-side renderer doesn't need a second formatter.
type eventPayload struct {
	Timestamp  string         `json:"timestamp"`
	Service    *string        `json:"service"`
	Level      *string        `json:"level"`
	Message    *string        `json:"message"`
	Host       *string        `json:"host"`
	Source     string         `json:"source"`
	Attributes map[string]any `json:"attributes"`
	Raw        string         `json:"raw"`
}

// subscribeMessage is the client → server subscription frame. At least
// one of q/filter/filters must be present, or search must be non-empty
// (§7.6).
type subscribeMessage struct {
	Q       string            `json:"q,omitempty"`
	Filter  json.RawMessage   `json:"filter,omitempty"`
	Filters []json.RawMessage `json:"filters,omitempty"`
	Search  string            `json:"search,omitempty"`
}

// subscription is the compiled, ready-to-match version of a
// subscribeMessage.
type subscription struct {
	root    query.Node
	search  string
	summary string
}

type subEvent struct {
	sub      *subscription
	parseErr error // non-nil = client sent malformed/invalid filter
	readErr  error // non-nil = network/protocol error
}

func (s *Server) runConnection(parent context.Context, conn *websocket.Conn) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	subCh := make(chan subEvent, 1)
	go s.readLoop(ctx, conn, subCh)

	// First message must arrive before we begin streaming.
	var current *subscription
	select {
	case msg := <-subCh:
		if msg.readErr != nil {
			s.closeWith(conn, websocket.StatusNormalClosure, "")
			return
		}
		if msg.parseErr != nil {
			s.closeWith(conn, CloseInvalidFilter, truncateReason(msg.parseErr.Error()))
			return
		}
		if msg.sub == nil {
			s.closeWith(conn, CloseFilterRequired, "subscription requires filter or search")
			return
		}
		current = msg.sub
	case <-ctx.Done():
		s.closeWith(conn, websocket.StatusNormalClosure, "")
		return
	}

	// Writer goroutine: decoupled from the main select loop so a slow
	// client doesn't block subscription updates or rate-limit checks.
	writeQueue := make(chan []byte, 16)
	writeErrCh := make(chan error, 1)
	var writerOnce sync.Once
	stopWriter := func() {
		writerOnce.Do(func() { close(writeQueue) })
	}
	defer stopWriter()
	go s.writeLoop(ctx, conn, writeQueue, writeErrCh)

	// Acknowledge subscription before delivering any events.
	if !s.enqueue(writeQueue, statusBytes(current, false, 0)) {
		s.closeWith(conn, CloseSlowClient, "slow client")
		return
	}

	stream := s.fanout.Subscribe(s.sendQueueCap)
	defer stream.Cancel()

	heartbeat := time.NewTicker(s.heartbeat)
	defer heartbeat.Stop()

	// Watch for write timeouts. The writer goroutine signals via
	// writeErrCh; main loop responds by closing 4003.
	rateLimiter := newRateLimiter(s.rateCapPerSecond, time.Second)
	var droppedTotal uint64
	rateCappedNow := false

	for {
		select {
		case <-ctx.Done():
			s.closeWith(conn, websocket.StatusNormalClosure, "")
			return

		case err := <-writeErrCh:
			if errors.Is(err, errSlowClient) {
				s.closeWith(conn, CloseSlowClient, "slow client")
				return
			}
			// Other write errors: connection is unusable; close normally.
			s.closeWith(conn, websocket.StatusNormalClosure, "")
			return

		case msg := <-subCh:
			if msg.readErr != nil {
				s.closeWith(conn, websocket.StatusNormalClosure, "")
				return
			}
			if msg.parseErr != nil {
				s.closeWith(conn, CloseInvalidFilter, truncateReason(msg.parseErr.Error()))
				return
			}
			if msg.sub == nil {
				s.closeWith(conn, CloseFilterRequired, "subscription requires filter or search")
				return
			}
			current = msg.sub
			if !s.enqueue(writeQueue, statusBytes(current, rateCappedNow, droppedTotal)) {
				s.closeWith(conn, CloseSlowClient, "slow client")
				return
			}

		case <-heartbeat.C:
			if !s.enqueue(writeQueue, statusBytes(current, rateCappedNow, droppedTotal)) {
				s.closeWith(conn, CloseSlowClient, "slow client")
				return
			}

		case ev, ok := <-stream.Events:
			if !ok {
				return
			}
			if !subscriptionMatches(current, ev) {
				continue
			}
			if !rateLimiter.allow() {
				droppedTotal++
				if !rateCappedNow {
					rateCappedNow = true
					if !s.enqueue(writeQueue, statusBytes(current, rateCappedNow, droppedTotal)) {
						s.closeWith(conn, CloseSlowClient, "slow client")
						return
					}
				}
				continue
			}
			if rateCappedNow {
				rateCappedNow = false
			}
			if !s.enqueue(writeQueue, eventBytes(ev)) {
				s.closeWith(conn, CloseSlowClient, "slow client")
				return
			}
		}
	}
}

// enqueue attempts a non-blocking send into the writer queue. Returns
// false when the queue is full — the caller treats that as a
// slow-client signal because the writer can't keep up.
func (s *Server) enqueue(queue chan<- []byte, data []byte) bool {
	if data == nil {
		return true
	}
	select {
	case queue <- data:
		return true
	default:
	}
	// Give the writer a brief moment to catch up before declaring the
	// client slow. The grace bounds how long the publisher will wait.
	timer := time.NewTimer(s.slowClientGrace)
	defer timer.Stop()
	select {
	case queue <- data:
		return true
	case <-timer.C:
		return false
	}
}

func (s *Server) readLoop(ctx context.Context, conn *websocket.Conn, out chan<- subEvent) {
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			select {
			case out <- subEvent{readErr: err}:
			case <-ctx.Done():
			}
			return
		}
		sub, perr := parseSubscribe(data)
		select {
		case out <- subEvent{sub: sub, parseErr: perr}:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) writeLoop(ctx context.Context, conn *websocket.Conn, in <-chan []byte, errCh chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-in:
			if !ok {
				return
			}
			writeCtx, cancel := context.WithTimeout(ctx, s.slowClientGrace)
			err := conn.Write(writeCtx, websocket.MessageText, data)
			cancel()
			if err == nil {
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) {
				select {
				case errCh <- errSlowClient:
				case <-ctx.Done():
				}
				return
			}
			select {
			case errCh <- err:
			case <-ctx.Done():
			}
			return
		}
	}
}

func (s *Server) closeWith(conn *websocket.Conn, code websocket.StatusCode, reason string) {
	if code == CloseSlowClient || code == CloseFilterRequired || code == CloseInvalidFilter || code == CloseTooManyClients {
		s.fireOnClose(code)
	}
	// Bound how long the close frame can take. On a wedged connection
	// the underlying Close still releases the conn after this returns.
	done := make(chan struct{})
	go func() {
		_ = conn.Close(code, truncateReason(reason))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
	}
}

func (s *Server) fireOnClose(code websocket.StatusCode) {
	if s.onClose != nil {
		s.onClose(code)
	}
}

func parseSubscribe(data []byte) (*subscription, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var msg subscribeMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	forms := 0
	if strings.TrimSpace(msg.Q) != "" {
		forms++
	}
	if len(msg.Filter) > 0 {
		forms++
	}
	if len(msg.Filters) > 0 {
		forms++
	}
	if forms > 1 {
		return nil, fmt.Errorf("q, filter, and filters are mutually exclusive")
	}

	var root query.Node
	search := msg.Search
	summary := ""
	switch {
	case strings.TrimSpace(msg.Q) != "":
		pres, err := parser.Parse(msg.Q)
		if err != nil {
			return nil, err
		}
		if pres.Filter != nil {
			root = pres.Filter
		}
		if pres.Search != "" {
			if search != "" {
				search = search + " " + pres.Search
			} else {
				search = pres.Search
			}
		}
		summary = strings.TrimSpace(msg.Q)
	case len(msg.Filter) > 0:
		n, err := query.UnmarshalNode(msg.Filter)
		if err != nil {
			return nil, err
		}
		root = n
		summary = "filter:tree"
	case len(msg.Filters) > 0:
		nodes := make([]query.Node, 0, len(msg.Filters))
		for i, raw := range msg.Filters {
			n, err := query.UnmarshalNode(raw)
			if err != nil {
				return nil, fmt.Errorf("filters[%d]: %w", i, err)
			}
			nodes = append(nodes, n)
		}
		root = query.Group{Op: query.OpAnd, Children: nodes}
		summary = "filter:tree"
	}

	if root != nil {
		if err := query.Validate(root); err != nil {
			return nil, err
		}
	}
	if search != "" && summary == "" {
		summary = "search:" + search
	}
	if root == nil && strings.TrimSpace(search) == "" {
		return nil, nil
	}
	return &subscription{root: root, search: search, summary: summary}, nil
}

func subscriptionMatches(sub *subscription, ev ingest.Event) bool {
	if sub == nil {
		return false
	}
	if sub.root != nil && !query.Match(sub.root, ev) {
		return false
	}
	if sub.search != "" && !query.MatchSearch(sub.search, ev) {
		return false
	}
	return true
}

func statusBytes(sub *subscription, rateCapped bool, dropped uint64) []byte {
	msg := statusMessage{
		Type:          "status",
		Subscribed:    sub != nil,
		RateCapped:    rateCapped,
		EventsDropped: dropped,
	}
	if sub != nil {
		msg.FilterSummary = sub.summary
	}
	data, _ := json.Marshal(msg)
	return data
}

func eventBytes(ev ingest.Event) []byte {
	msg := eventMessage{Type: "event", Event: eventToPayload(ev)}
	data, _ := json.Marshal(msg)
	return data
}

var errSlowClient = errors.New("slow client write timeout")

func eventToPayload(ev ingest.Event) eventPayload {
	out := eventPayload{
		Timestamp:  ev.Timestamp.UTC().Format(time.RFC3339Nano),
		Source:     ev.Source,
		Raw:        ev.Raw,
		Attributes: ev.Attributes,
	}
	if ev.Service != "" {
		v := ev.Service
		out.Service = &v
	}
	if ev.Level != "" {
		v := ev.Level
		out.Level = &v
	}
	if ev.Message != "" {
		v := ev.Message
		out.Message = &v
	}
	if ev.Host != "" {
		v := ev.Host
		out.Host = &v
	}
	if out.Attributes == nil {
		out.Attributes = map[string]any{}
	}
	return out
}

// rateLimiter is a small fixed-window counter. PRD §7.6 specifies the
// rate cap as "per second" so a token-bucket would be overkill.
type rateLimiter struct {
	mu        sync.Mutex
	limit     int
	window    time.Duration
	windowEnd time.Time
	count     int
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	return &rateLimiter{limit: limit, window: window}
}

// truncateReason caps a close-frame reason at 123 bytes — the WebSocket
// protocol limits the reason string to 125 bytes including the two-byte
// status code.
func truncateReason(s string) string {
	const max = 123
	if len(s) <= max {
		return s
	}
	return s[:max]
}

func (r *rateLimiter) allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if now.After(r.windowEnd) {
		r.windowEnd = now.Add(r.window)
		r.count = 0
	}
	if r.count >= r.limit {
		return false
	}
	r.count++
	return true
}
