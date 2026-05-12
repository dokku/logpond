package query

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/dokku/logpond/internal/catalog"
	"github.com/dokku/logpond/internal/storage/duckdb"
)

// Default and ceiling for the user-facing limit (§7.3.4).
const (
	DefaultLimit = 100
	MaxLimit     = 1000
)

// SortableFields lists the columns acceptable in `sort` (§7.3.4).
var SortableFields = map[string]bool{
	"timestamp": true,
	"service":   true,
	"level":     true,
	"host":      true,
	"source":    true,
}

// SortKey is one entry in the `sort` array. Order is "asc" or "desc".
type SortKey struct {
	Field string `json:"field"`
	Order string `json:"order"`
}

// Request bundles the inputs to a query (post Form-A/B/C normalisation).
type Request struct {
	From         time.Time
	To           time.Time
	Filter       Node // canonical tree; may be nil
	Search       string
	Sort         []SortKey
	Limit        int
	Cursor       string
	MaxTimeRange time.Duration
}

// ResultEvent is a single row in the response.
type ResultEvent struct {
	Timestamp  time.Time
	Service    sql.NullString
	Level      sql.NullString
	Message    sql.NullString
	Host       sql.NullString
	Source     string
	Attributes string // raw JSON; caller decodes if needed
	Raw        string

	SegmentID string
}

// Stats mirrors §13.4's `stats` block.
type Stats struct {
	RowsScanned  int64
	RowsReturned int64
	SegmentsRead int
	DurationMs   int64
}

// Response groups the executor output.
type Response struct {
	Events     []ResultEvent
	NextCursor string
	Stats      Stats
}

// ActiveSegmentSource is the slice of segments.Manager used by the
// executor. Extracted as an interface to keep the package free of a
// direct dependency on the manager for tests.
type ActiveSegmentSource interface {
	QueryActive(ctx context.Context, id, sqlText string, args ...any) (*sql.Rows, bool, error)
}

// Executor orchestrates a single query across active and sealed
// segments.
type Executor struct {
	cat    *catalog.Catalog
	active ActiveSegmentSource
	logger *slog.Logger
}

// NewExecutor constructs an Executor. The active source may be nil if
// the caller wants sealed-only queries (e.g., during a recovery test).
func NewExecutor(cat *catalog.Catalog, active ActiveSegmentSource, logger *slog.Logger) *Executor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Executor{cat: cat, active: active, logger: logger}
}

// Error codes returned by the executor. The HTTP handler maps these
// onto the §13.1 error envelope.
var (
	ErrInvalidTimeRange  = errors.New("invalid_time_range")
	ErrTimeRangeTooLarge = errors.New("time_range_too_large")
	ErrCursorInvalidated = errors.New("cursor_invalidated")
	ErrInvalidFilter     = errors.New("invalid_filter")
	ErrInvalidSort       = errors.New("invalid_sort")
)

// Run executes the query and returns the response.
func (e *Executor) Run(ctx context.Context, req Request) (Response, error) {
	start := time.Now()
	if !req.To.After(req.From) {
		return Response{}, ErrInvalidTimeRange
	}
	if req.MaxTimeRange > 0 && req.To.Sub(req.From) > req.MaxTimeRange {
		return Response{}, ErrTimeRangeTooLarge
	}

	if req.Filter != nil {
		if err := Validate(req.Filter); err != nil {
			if IsFilterTooDeep(err) {
				return Response{}, err
			}
			return Response{}, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
		}
	}

	sortKeys := req.Sort
	if len(sortKeys) == 0 {
		sortKeys = []SortKey{{Field: "timestamp", Order: "desc"}}
	}
	for _, sk := range sortKeys {
		if !SortableFields[sk.Field] {
			return Response{}, fmt.Errorf("%w: %s", ErrInvalidSort, sk.Field)
		}
		if sk.Order != "asc" && sk.Order != "desc" {
			return Response{}, fmt.Errorf("%w: order=%q", ErrInvalidSort, sk.Order)
		}
	}

	limit := req.Limit
	if limit <= 0 {
		limit = DefaultLimit
	}
	if limit > MaxLimit {
		limit = MaxLimit
	}

	var filterC Compiled
	if req.Filter != nil {
		c, err := Compile(req.Filter)
		if err != nil {
			return Response{}, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
		}
		filterC = c
	}
	searchC := CompileSearch(req.Search)
	timeC := Compiled{
		SQL:  `"timestamp" >= ? AND "timestamp" <= ?`,
		Args: []any{req.From.UTC(), req.To.UTC()},
	}
	cursorC, cursor, err := compileCursor(req.Cursor, sortKeys)
	if err != nil {
		return Response{}, err
	}

	segs, err := e.cat.SegmentsInRange(ctx, req.From, req.To)
	if err != nil {
		return Response{}, fmt.Errorf("listing segments: %w", err)
	}
	if cursor != nil {
		ok := false
		for _, s := range segs {
			if s.ID == cursor.SegmentID {
				ok = true
				break
			}
		}
		if !ok {
			return Response{}, ErrCursorInvalidated
		}
	}

	where := CombineAnd(timeC, filterC, searchC, cursorC)
	orderBy := orderBySQL(sortKeys)

	segs = orderSegments(segs, sortKeys)

	needed := limit + 1
	resp := Response{}
	resp.Events = make([]ResultEvent, 0, needed)

	var rowsScanned int64
	var segsTouched int
	for _, s := range segs {
		if len(resp.Events) >= needed {
			break
		}
		batchLimit := needed - len(resp.Events)
		evs, scanned, err := e.querySegment(ctx, s, where, orderBy, batchLimit)
		if err != nil {
			return Response{}, fmt.Errorf("segment %s: %w", s.ID, err)
		}
		rowsScanned += scanned
		if len(evs) > 0 {
			segsTouched++
		}
		resp.Events = append(resp.Events, evs...)
	}

	sortEvents(resp.Events, sortKeys)

	if len(resp.Events) > limit {
		last := resp.Events[limit-1]
		resp.NextCursor = encodeCursor(cursorState{
			Timestamp: last.Timestamp,
			SegmentID: last.SegmentID,
		})
		resp.Events = resp.Events[:limit]
	}
	resp.Stats = Stats{
		RowsScanned:  rowsScanned,
		RowsReturned: int64(len(resp.Events)),
		SegmentsRead: segsTouched,
		DurationMs:   time.Since(start).Milliseconds(),
	}
	return resp, nil
}

func (e *Executor) querySegment(ctx context.Context, s catalog.Segment, where Compiled, orderBy string, limit int) ([]ResultEvent, int64, error) {
	if s.State == catalog.StateActive {
		if e.active == nil {
			return nil, 0, nil
		}
		return e.queryActive(ctx, s, where, orderBy, limit)
	}
	if !s.LocalPath.Valid {
		return nil, 0, nil
	}
	return e.querySealed(ctx, s, where, orderBy, limit)
}

const selectColumns = `"timestamp", service, level, message, host, source, attributes, raw`

func (e *Executor) queryActive(ctx context.Context, s catalog.Segment, where Compiled, orderBy string, limit int) ([]ResultEvent, int64, error) {
	sqlText := "SELECT " + selectColumns + " FROM events"
	if where.SQL != "" {
		sqlText += " WHERE " + where.SQL
	}
	sqlText += " ORDER BY " + orderBy + fmt.Sprintf(" LIMIT %d", limit)

	rows, open, err := e.active.QueryActive(ctx, s.ID, sqlText, where.Args...)
	if err != nil {
		return nil, 0, err
	}
	if !open {
		return nil, 0, nil
	}
	defer rows.Close()
	return scanEvents(rows, s.ID)
}

func (e *Executor) querySealed(ctx context.Context, s catalog.Segment, where Compiled, orderBy string, limit int) ([]ResultEvent, int64, error) {
	conn, err := duckdb.Open(ctx, "", "")
	if err != nil {
		return nil, 0, fmt.Errorf("opening sealed reader: %w", err)
	}
	defer conn.Close()

	src := fmt.Sprintf("read_parquet('%s')", escapeSingleQuotes(s.LocalPath.String))
	sqlText := "SELECT " + selectColumns + " FROM " + src
	if where.SQL != "" {
		sqlText += " WHERE " + where.SQL
	}
	sqlText += " ORDER BY " + orderBy + fmt.Sprintf(" LIMIT %d", limit)

	rows, err := conn.DB().QueryContext(ctx, sqlText, where.Args...)
	if err != nil {
		return nil, 0, fmt.Errorf("querying sealed: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows, s.ID)
}

func scanEvents(rows *sql.Rows, segmentID string) ([]ResultEvent, int64, error) {
	var out []ResultEvent
	var n int64
	for rows.Next() {
		var ev ResultEvent
		var attrs sql.NullString
		var raw sql.NullString
		if err := rows.Scan(
			&ev.Timestamp,
			&ev.Service,
			&ev.Level,
			&ev.Message,
			&ev.Host,
			&ev.Source,
			&attrs,
			&raw,
		); err != nil {
			return nil, n, fmt.Errorf("scan: %w", err)
		}
		if attrs.Valid {
			ev.Attributes = attrs.String
		} else {
			ev.Attributes = "{}"
		}
		if raw.Valid {
			ev.Raw = raw.String
		}
		ev.SegmentID = segmentID
		ev.Timestamp = ev.Timestamp.UTC()
		out = append(out, ev)
		n++
	}
	if err := rows.Err(); err != nil {
		return out, n, err
	}
	return out, n, nil
}

// orderBySQL converts SortKey list to a SQL ORDER BY clause body.
func orderBySQL(keys []SortKey) string {
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		dir := "ASC"
		if strings.EqualFold(k.Order, "desc") {
			dir = "DESC"
		}
		parts = append(parts, quoteIdent(k.Field)+" "+dir)
	}
	return strings.Join(parts, ", ")
}

func sortEvents(evs []ResultEvent, keys []SortKey) {
	sort.SliceStable(evs, func(i, j int) bool {
		return compareEvents(evs[i], evs[j], keys) < 0
	})
}

func compareEvents(a, b ResultEvent, keys []SortKey) int {
	for _, k := range keys {
		cmp := compareField(a, b, k.Field)
		if cmp == 0 {
			continue
		}
		if strings.EqualFold(k.Order, "desc") {
			return -cmp
		}
		return cmp
	}
	return 0
}

func compareField(a, b ResultEvent, field string) int {
	switch field {
	case "timestamp":
		switch {
		case a.Timestamp.Before(b.Timestamp):
			return -1
		case a.Timestamp.After(b.Timestamp):
			return 1
		}
		return 0
	case "service":
		return strings.Compare(a.Service.String, b.Service.String)
	case "level":
		return strings.Compare(a.Level.String, b.Level.String)
	case "host":
		return strings.Compare(a.Host.String, b.Host.String)
	case "source":
		return strings.Compare(a.Source, b.Source)
	}
	return 0
}

// orderSegments sorts the catalog list so that querying segments in
// this order yields results in (roughly) the requested sort order
// without needing a global re-sort across a huge set.
func orderSegments(segs []catalog.Segment, keys []SortKey) []catalog.Segment {
	desc := len(keys) > 0 && strings.EqualFold(keys[0].Order, "desc") && keys[0].Field == "timestamp"
	asc := len(keys) > 0 && !strings.EqualFold(keys[0].Order, "desc") && keys[0].Field == "timestamp"
	out := make([]catalog.Segment, len(segs))
	copy(out, segs)
	sort.SliceStable(out, func(i, j int) bool {
		if desc {
			return out[i].TimeStart.After(out[j].TimeStart)
		}
		if asc {
			return out[i].TimeStart.Before(out[j].TimeStart)
		}
		return false
	})
	return out
}

// cursorState is the structure encoded into the opaque cursor token.
type cursorState struct {
	Timestamp time.Time `json:"ts"`
	SegmentID string    `json:"seg"`
}

func encodeCursor(c cursorState) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (*cursorState, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, ErrCursorInvalidated
	}
	var c cursorState
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, ErrCursorInvalidated
	}
	c.Timestamp = c.Timestamp.UTC()
	return &c, nil
}

// compileCursor turns the cursor string into an extra WHERE fragment
// and returns the decoded state for catalog cross-checks. Empty cursor
// → empty Compiled, nil state.
func compileCursor(token string, keys []SortKey) (Compiled, *cursorState, error) {
	if token == "" {
		return Compiled{}, nil, nil
	}
	c, err := decodeCursor(token)
	if err != nil {
		return Compiled{}, nil, err
	}
	if len(keys) == 0 || keys[0].Field != "timestamp" {
		return Compiled{}, c, nil
	}
	desc := strings.EqualFold(keys[0].Order, "desc")
	if desc {
		return Compiled{
			SQL:  `("timestamp" < ? OR ("timestamp" = ? AND ? > ?))`,
			Args: []any{c.Timestamp, c.Timestamp, c.SegmentID, c.SegmentID},
		}, c, nil
	}
	return Compiled{
		SQL:  `("timestamp" > ? OR ("timestamp" = ? AND ? > ?))`,
		Args: []any{c.Timestamp, c.Timestamp, c.SegmentID, c.SegmentID},
	}, c, nil
}
