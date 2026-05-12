package ingest

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
)

// SourceSpec describes how a single ingest source maps raw JSON fields
// onto the normalized Event columns. It mirrors config.Source but stays
// independent so the ingest package has no dependency on config.
type SourceSpec struct {
	Name    string
	Extract map[string][]string // field name -> ordered candidate JSON paths
}

// Extractor turns a single decoded JSON object into a normalized Event
// per PRD §7.1.
type Extractor struct {
	Source SourceSpec
	// Now returns the ingest-time timestamp. Override in tests for
	// deterministic fallbacks.
	Now func() time.Time
}

// NewExtractor returns an Extractor configured for source.
func NewExtractor(source SourceSpec) *Extractor {
	return &Extractor{Source: source, Now: time.Now}
}

// ErrInvalidJSON signals that the input was not a JSON object. The
// handler counts these in the "skipped" total.
var ErrInvalidJSON = errors.New("not a JSON object")

// Extract parses line into an Event. If line is not a JSON object the
// returned error is ErrInvalidJSON.
func (e *Extractor) Extract(line []byte) (Event, error) {
	var obj map[string]any
	dec := json.NewDecoder(strings.NewReader(string(line)))
	dec.UseNumber()
	if err := dec.Decode(&obj); err != nil {
		return Event{}, ErrInvalidJSON
	}
	if obj == nil {
		return Event{}, ErrInvalidJSON
	}

	// Clone obj into a mutable attributes map. Extracted fields are
	// removed from it before attaching to the Event.
	attrs := make(map[string]any, len(obj))
	for k, v := range obj {
		attrs[k] = v
	}

	ev := Event{
		Source: e.Source.Name,
		Raw:    string(line),
	}

	ts, tsPath, tsOK := e.resolveTimestamp(obj)
	if tsOK {
		ev.Timestamp = ts
		removePath(attrs, tsPath)
	} else {
		ev.Timestamp = e.now().UTC()
		attrs["logpond_timestamp_fallback"] = true
	}

	lvl, lvlPath, lvlOK, lvlOriginal, lvlUnrecognized := e.resolveLevel(obj)
	if lvlOK {
		ev.Level = lvl
		removePath(attrs, lvlPath)
		if lvlOriginal != "" {
			attrs["logpond_level_original"] = lvlOriginal
		}
		if lvlUnrecognized != "" {
			attrs["logpond_level_unrecognized"] = lvlUnrecognized
		}
	} else {
		ev.Level = "info"
		attrs["logpond_level_fallback"] = true
	}

	if msg, path, ok := e.resolveMessage(obj); ok {
		ev.Message = msg
		removePath(attrs, path)
	}
	if svc, path, ok := e.resolveString(obj, "service"); ok {
		ev.Service = svc
		removePath(attrs, path)
	}
	if host, path, ok := e.resolveString(obj, "host"); ok {
		ev.Host = host
		removePath(attrs, path)
	}

	ev.Attributes = attrs
	return ev, nil
}

func (e *Extractor) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

func (e *Extractor) resolveTimestamp(obj map[string]any) (time.Time, string, bool) {
	for _, path := range e.Source.Extract["timestamp"] {
		v, ok := lookup(obj, path)
		if !ok || v == nil {
			continue
		}
		if t, ok := coerceTimestamp(v); ok {
			return t, path, true
		}
	}
	return time.Time{}, "", false
}

// coerceTimestamp accepts an RFC 3339 string, a Unix-seconds number, or
// a Unix-milliseconds number distinguished by magnitude.
func coerceTimestamp(v any) (time.Time, bool) {
	switch x := v.(type) {
	case string:
		if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
			return t.UTC(), true
		}
		if t, err := time.Parse(time.RFC3339, x); err == nil {
			return t.UTC(), true
		}
		return time.Time{}, false
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return unixToTime(i), true
		}
		if f, err := x.Float64(); err == nil {
			return unixToTime(int64(f)), true
		}
		return time.Time{}, false
	case float64:
		return unixToTime(int64(x)), true
	case int64:
		return unixToTime(x), true
	case int:
		return unixToTime(int64(x)), true
	}
	return time.Time{}, false
}

// unixToTime distinguishes seconds vs. milliseconds by magnitude:
// values larger than 10^12 (i.e., year 33,658+ in seconds) are treated
// as milliseconds, matching PRD §7.1.
func unixToTime(n int64) time.Time {
	if n >= 1_000_000_000_000 {
		return time.UnixMilli(n).UTC()
	}
	return time.Unix(n, 0).UTC()
}

func (e *Extractor) resolveLevel(obj map[string]any) (norm, path string, ok bool, original, unrecognized string) {
	for _, p := range e.Source.Extract["level"] {
		v, found := lookup(obj, p)
		if !found || v == nil {
			continue
		}
		switch x := v.(type) {
		case string:
			n, orig, unk := normalizeLevelString(x)
			return n, p, true, orig, unk
		case json.Number:
			if i, err := x.Int64(); err == nil {
				n, orig, unk := normalizeLevelString(strconv.FormatInt(i, 10))
				return n, p, true, orig, unk
			}
		case float64:
			n, orig, unk := normalizeLevelString(strconv.FormatInt(int64(x), 10))
			return n, p, true, orig, unk
		case bool, []any, map[string]any:
			continue
		}
	}
	return "", "", false, "", ""
}

var levelMap = map[string]string{
	"trace":       "debug",
	"debug":       "debug",
	"dbg":         "debug",
	"0":           "debug",
	"1":           "debug",
	"info":        "info",
	"information": "info",
	"notice":      "info",
	"2":           "info",
	"3":           "info",
	"warn":        "warn",
	"warning":     "warn",
	"4":           "warn",
	"error":       "error",
	"err":         "error",
	"critical":    "error",
	"crit":        "error",
	"fatal":       "error",
	"emerg":       "error",
	"alert":       "error",
	"panic":       "error",
	"5":           "error",
	"6":           "error",
	"7":           "error",
}

// normalizeLevelString maps a raw level token to one of the four
// canonical values. The second return value carries the original token
// when normalization changed it; the third holds the original token
// when no entry in the table matched (the operator's escape hatch).
func normalizeLevelString(raw string) (norm, original, unrecognized string) {
	trimmed := strings.TrimSpace(raw)
	lower := strings.ToLower(trimmed)
	if n, ok := levelMap[lower]; ok {
		if n != trimmed {
			return n, trimmed, ""
		}
		return n, "", ""
	}
	return "info", "", trimmed
}

func (e *Extractor) resolveMessage(obj map[string]any) (string, string, bool) {
	for _, p := range e.Source.Extract["message"] {
		v, ok := lookup(obj, p)
		if !ok || v == nil {
			continue
		}
		switch x := v.(type) {
		case string:
			return x, p, true
		case json.Number:
			return x.String(), p, true
		case bool:
			return strconv.FormatBool(x), p, true
		case float64:
			return strconv.FormatFloat(x, 'g', -1, 64), p, true
		default:
			b, err := json.Marshal(v)
			if err == nil {
				return string(b), p, true
			}
		}
	}
	return "", "", false
}

func (e *Extractor) resolveString(obj map[string]any, field string) (string, string, bool) {
	for _, p := range e.Source.Extract[field] {
		v, ok := lookup(obj, p)
		if !ok || v == nil {
			continue
		}
		if s, ok := v.(string); ok && s != "" {
			return s, p, true
		}
	}
	return "", "", false
}

// lookup resolves a dotted JSON path against an object. Numeric segments
// index into arrays.
func lookup(obj map[string]any, path string) (any, bool) {
	if path == "" {
		return nil, false
	}
	parts := strings.Split(path, ".")
	var cur any = obj
	for _, part := range parts {
		switch x := cur.(type) {
		case map[string]any:
			next, ok := x[part]
			if !ok {
				return nil, false
			}
			cur = next
		case []any:
			i, err := strconv.Atoi(part)
			if err != nil || i < 0 || i >= len(x) {
				return nil, false
			}
			cur = x[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

// removePath deletes the leaf reachable via path from the attributes
// map. Intermediate empty maps are left in place; the extractor's job
// is to avoid duplicating an extracted scalar, not to garbage-collect
// the user's object structure.
func removePath(attrs map[string]any, path string) {
	parts := strings.Split(path, ".")
	if len(parts) == 1 {
		delete(attrs, parts[0])
		return
	}
	cur := any(attrs)
	for _, part := range parts[:len(parts)-1] {
		m, ok := cur.(map[string]any)
		if !ok {
			return
		}
		next, ok := m[part]
		if !ok {
			return
		}
		cur = next
	}
	if m, ok := cur.(map[string]any); ok {
		delete(m, parts[len(parts)-1])
	}
}

// SourceFromConfig is a small adapter for callers that have a
// config.Source. Kept here so internal/api doesn't need to know the
// internal layout of an Extractor.
func SourceFromConfig(name string, extract map[string][]string) SourceSpec {
	return SourceSpec{Name: name, Extract: extract}
}
