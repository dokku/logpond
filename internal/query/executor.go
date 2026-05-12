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

	// Facets is the resolved list of facets to compute. Empty disables
	// facet aggregation; the API layer expands "all enabled" via the
	// registry before calling Run.
	Facets []FacetSpec
	// FacetSampleSize bounds the number of most-recent overlapping
	// segments aggregated for facet values (PRD §7.4.3, default 24).
	FacetSampleSize int
}

// FacetSpec is the executor-side view of a facet definition. The API
// layer fills it in from facets.Definition before calling Run.
type FacetSpec struct {
	Name           string
	Field          string
	CardinalityCap int
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
	Facets     map[string]FacetResult
}

// FacetResult is a single facet's contribution to the response, shaped
// to match §13.4's `facets.<name>` block.
type FacetResult struct {
	Values         []FacetValue
	CardinalityCap int
	Truncated      bool
	TruncatedCount int
	SampleSegments int
}

// FacetValue is one (value, count) entry inside FacetResult.Values.
type FacetValue struct {
	Value string
	Count int64
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

	if len(req.Facets) > 0 {
		baseFilter := CombineAnd(timeC, filterC, searchC)
		sample := selectFacetSample(segs, req.FacetSampleSize)
		facets, err := e.computeFacets(ctx, sample, baseFilter, req.Facets)
		if err != nil {
			return Response{}, fmt.Errorf("computing facets: %w", err)
		}
		resp.Facets = facets
	}

	resp.Stats = Stats{
		RowsScanned:  rowsScanned,
		RowsReturned: int64(len(resp.Events)),
		SegmentsRead: segsTouched,
		DurationMs:   time.Since(start).Milliseconds(),
	}
	return resp, nil
}

// Count executes the request as a count-only query, returning (count,
// exact, segmentsRead, durationMs). When the matched row count reaches
// or exceeds limitGuard, the call short-circuits and reports exact=false
// (PRD §13.5).
func (e *Executor) Count(ctx context.Context, req Request, limitGuard int) (int64, bool, int, int64, error) {
	start := time.Now()
	if !req.To.After(req.From) {
		return 0, false, 0, 0, ErrInvalidTimeRange
	}
	if req.MaxTimeRange > 0 && req.To.Sub(req.From) > req.MaxTimeRange {
		return 0, false, 0, 0, ErrTimeRangeTooLarge
	}
	if req.Filter != nil {
		if err := Validate(req.Filter); err != nil {
			if IsFilterTooDeep(err) {
				return 0, false, 0, 0, err
			}
			return 0, false, 0, 0, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
		}
	}

	var filterC Compiled
	if req.Filter != nil {
		c, err := Compile(req.Filter)
		if err != nil {
			return 0, false, 0, 0, fmt.Errorf("%w: %v", ErrInvalidFilter, err)
		}
		filterC = c
	}
	searchC := CompileSearch(req.Search)
	timeC := Compiled{
		SQL:  `"timestamp" >= ? AND "timestamp" <= ?`,
		Args: []any{req.From.UTC(), req.To.UTC()},
	}
	where := CombineAnd(timeC, filterC, searchC)

	segs, err := e.cat.SegmentsInRange(ctx, req.From, req.To)
	if err != nil {
		return 0, false, 0, 0, fmt.Errorf("listing segments: %w", err)
	}

	if limitGuard <= 0 {
		limitGuard = 10001
	}

	var total int64
	segsTouched := 0
	for _, s := range segs {
		if total >= int64(limitGuard) {
			break
		}
		remaining := int64(limitGuard) - total
		n, err := e.countSegment(ctx, s, where, remaining)
		if err != nil {
			return 0, false, 0, 0, fmt.Errorf("counting segment %s: %w", s.ID, err)
		}
		if n > 0 {
			segsTouched++
		}
		total += n
	}
	exact := total < int64(limitGuard)
	if !exact {
		total = int64(limitGuard) - 1
	}
	return total, exact, segsTouched, time.Since(start).Milliseconds(), nil
}

func (e *Executor) countSegment(ctx context.Context, s catalog.Segment, where Compiled, limit int64) (int64, error) {
	whereSQL := ""
	if where.SQL != "" {
		whereSQL = " WHERE " + where.SQL
	}
	// SELECT 1 ... LIMIT N + COUNT in Go gives the executor cheap early
	// termination without DuckDB-specific tricks.
	if s.State == catalog.StateActive {
		if e.active == nil {
			return 0, nil
		}
		sqlText := fmt.Sprintf(`SELECT 1 FROM events%s LIMIT %d`, whereSQL, limit)
		rows, open, err := e.active.QueryActive(ctx, s.ID, sqlText, where.Args...)
		if err != nil {
			return 0, err
		}
		if !open {
			return 0, nil
		}
		defer rows.Close()
		return drainCount(rows)
	}
	if !s.LocalPath.Valid {
		return 0, nil
	}
	conn, err := duckdb.Open(ctx, "", "")
	if err != nil {
		return 0, fmt.Errorf("opening sealed reader: %w", err)
	}
	defer conn.Close()
	src := fmt.Sprintf("read_parquet('%s')", escapeSingleQuotes(s.LocalPath.String))
	sqlText := fmt.Sprintf(`SELECT 1 FROM %s%s LIMIT %d`, src, whereSQL, limit)
	rows, err := conn.DB().QueryContext(ctx, sqlText, where.Args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	return drainCount(rows)
}

func drainCount(rows *sql.Rows) (int64, error) {
	var n int64
	var sink int
	for rows.Next() {
		if err := rows.Scan(&sink); err != nil {
			return n, err
		}
		n++
	}
	return n, rows.Err()
}

// selectFacetSample picks the N most-recent overlapping segments per
// §7.4.3. Segments without a queryable backing (no local_path, not
// active) are skipped so we don't issue read_parquet against nothing.
func selectFacetSample(segs []catalog.Segment, n int) []catalog.Segment {
	if n <= 0 {
		n = 24
	}
	candidates := make([]catalog.Segment, 0, len(segs))
	for _, s := range segs {
		if s.State == catalog.StateActive || s.LocalPath.Valid {
			candidates = append(candidates, s)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].TimeStart.After(candidates[j].TimeStart)
	})
	if len(candidates) > n {
		candidates = candidates[:n]
	}
	return candidates
}

func (e *Executor) computeFacets(ctx context.Context, sample []catalog.Segment, base Compiled, specs []FacetSpec) (map[string]FacetResult, error) {
	out := make(map[string]FacetResult, len(specs))
	for _, spec := range specs {
		fieldExpr, err := facetFieldExpr(spec.Field)
		if err != nil {
			return nil, err
		}
		// Aggregate full GROUP BY across the sample, then truncate in Go.
		// We cap the per-segment scan to limit memory blowup on
		// pathological cardinalities; the extra+1 lets us tell whether
		// we exceeded the truncation threshold.
		counts := map[string]int64{}
		sampleHit := 0
		for _, s := range sample {
			rows, err := e.queryFacetSegment(ctx, s, fieldExpr, base)
			if err != nil {
				return nil, fmt.Errorf("facet %s segment %s: %w", spec.Name, s.ID, err)
			}
			any := false
			for _, r := range rows {
				counts[r.Value] += r.Count
				any = true
			}
			if any {
				sampleHit++
			}
		}
		fr := buildFacetResult(spec, counts, sampleHit)
		out[spec.Name] = fr
	}
	return out, nil
}

type facetRow struct {
	Value string
	Count int64
}

func (e *Executor) queryFacetSegment(ctx context.Context, s catalog.Segment, fieldExpr string, base Compiled) ([]facetRow, error) {
	whereSQL := ""
	if base.SQL != "" {
		whereSQL = " WHERE " + base.SQL
	}
	// Filter NULL values out at the SQL level so "(none)" doesn't bloat
	// the facet list. Callers can still query the field's absence via
	// the explicit exists predicate.
	if whereSQL == "" {
		whereSQL = " WHERE " + fieldExpr + " IS NOT NULL"
	} else {
		whereSQL += " AND " + fieldExpr + " IS NOT NULL"
	}
	sqlText := fmt.Sprintf(`SELECT %s AS v, COUNT(*) AS c FROM %%s%s GROUP BY %s ORDER BY c DESC`,
		fieldExpr, whereSQL, fieldExpr)

	var rows *sql.Rows
	if s.State == catalog.StateActive {
		if e.active == nil {
			return nil, nil
		}
		formatted := fmt.Sprintf(sqlText, "events")
		r, open, err := e.active.QueryActive(ctx, s.ID, formatted, base.Args...)
		if err != nil {
			return nil, err
		}
		if !open {
			return nil, nil
		}
		rows = r
	} else {
		if !s.LocalPath.Valid {
			return nil, nil
		}
		conn, err := duckdb.Open(ctx, "", "")
		if err != nil {
			return nil, fmt.Errorf("opening sealed reader: %w", err)
		}
		defer conn.Close()
		src := fmt.Sprintf("read_parquet('%s')", escapeSingleQuotes(s.LocalPath.String))
		formatted := fmt.Sprintf(sqlText, src)
		r, err := conn.DB().QueryContext(ctx, formatted, base.Args...)
		if err != nil {
			return nil, err
		}
		rows = r
	}
	defer rows.Close()
	var out []facetRow
	for rows.Next() {
		var v sql.NullString
		var c int64
		if err := rows.Scan(&v, &c); err != nil {
			return nil, fmt.Errorf("scanning facet row: %w", err)
		}
		if !v.Valid {
			continue
		}
		out = append(out, facetRow{Value: v.String, Count: c})
	}
	return out, rows.Err()
}

func facetFieldExpr(field string) (string, error) {
	if strings.HasPrefix(field, AttributePrefix) {
		path := field[len(AttributePrefix):]
		return fmt.Sprintf("json_extract_string(attributes, '$.%s')", escapeSingleQuotes(path)), nil
	}
	if !CoreColumns[field] {
		return "", fmt.Errorf("unknown facet field %q", field)
	}
	return quoteIdent(field), nil
}

func buildFacetResult(spec FacetSpec, counts map[string]int64, sampleHit int) FacetResult {
	values := make([]FacetValue, 0, len(counts))
	for v, c := range counts {
		values = append(values, FacetValue{Value: v, Count: c})
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].Count != values[j].Count {
			return values[i].Count > values[j].Count
		}
		return values[i].Value < values[j].Value
	})
	total := len(values)
	cap := spec.CardinalityCap
	if cap <= 0 {
		cap = 50
	}
	truncated := total > cap
	if truncated {
		values = values[:cap]
	}
	res := FacetResult{
		Values:         values,
		CardinalityCap: cap,
		Truncated:      truncated,
		SampleSegments: sampleHit,
	}
	if truncated {
		res.TruncatedCount = total
	}
	return res
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
