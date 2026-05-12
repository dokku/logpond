package query

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dokku/logpond/internal/ingest"
)

// Match evaluates a filter tree against an in-memory event. It mirrors
// the SQL compiler's semantics (PRD §7.3.4) so a row that lives in
// memory matches the same filter as it would after sealing. The
// matcher is the live-tail equivalent of the executor's SQL pass.
//
// An empty/nil node matches every event (the live-tail server rejects
// empty subscriptions before they reach here).
func Match(n Node, ev ingest.Event) bool {
	if n == nil {
		return true
	}
	switch x := n.(type) {
	case Group:
		return matchGroup(x, ev)
	case Leaf:
		return matchLeaf(x, ev)
	default:
		return false
	}
}

// MatchSearch implements the free-text predicate (PRD §7.3.2 top-level
// search): case-insensitive substring across `message` and `raw`.
// Empty search matches everything.
func MatchSearch(search string, ev ingest.Event) bool {
	search = strings.TrimSpace(search)
	if search == "" {
		return true
	}
	needle := strings.ToLower(search)
	if strings.Contains(strings.ToLower(ev.Message), needle) {
		return true
	}
	if strings.Contains(strings.ToLower(ev.Raw), needle) {
		return true
	}
	return false
}

func matchGroup(g Group, ev ingest.Event) bool {
	if len(g.Children) == 0 {
		return true
	}
	result := g.Op != OpOr // AND starts true; OR starts false
	for _, c := range g.Children {
		ok := Match(c, ev)
		if g.Op == OpOr {
			result = result || ok
			if result {
				break
			}
		} else {
			result = result && ok
			if !result {
				break
			}
		}
	}
	if g.Not {
		return !result
	}
	return result
}

func matchLeaf(l Leaf, ev ingest.Event) bool {
	isAttr := strings.HasPrefix(l.Field, AttributePrefix)
	if !isAttr && !CoreColumns[l.Field] {
		return false
	}
	switch l.Op {
	case OpExists:
		return fieldExists(l.Field, isAttr, ev)
	case OpEq:
		return matchEq(l, isAttr, ev, false)
	case OpNeq:
		return matchEq(l, isAttr, ev, true)
	case OpIn:
		return matchIn(l, isAttr, ev, false)
	case OpNotIn:
		return matchIn(l, isAttr, ev, true)
	case OpContains:
		return matchSubstring(l, isAttr, ev, true)
	case OpStartsWith:
		return matchSubstring(l, isAttr, ev, false)
	case OpGt, OpLt, OpGte, OpLte:
		return matchRange(l, isAttr, ev)
	}
	return false
}

func fieldExists(field string, isAttr bool, ev ingest.Event) bool {
	if !isAttr {
		v, present := coreFieldValue(field, ev)
		_ = v
		return present
	}
	path := field[len(AttributePrefix):]
	_, present := attrValue(path, ev.Attributes)
	return present
}

func matchEq(l Leaf, isAttr bool, ev ingest.Event, negate bool) bool {
	got, present := fieldString(l.Field, isAttr, ev)
	wantValue, isNil, _ := scalarValue(l.Value)
	if isNil {
		// IS NULL / IS NOT NULL semantics.
		if negate {
			return present
		}
		return !present
	}
	if !present {
		return negate
	}
	want := scalarString(wantValue)
	left, right := got, want
	if shouldLower(l) {
		left = strings.ToLower(left)
		right = strings.ToLower(right)
	}
	eq := left == right
	if negate {
		return !eq
	}
	return eq
}

func matchIn(l Leaf, isAttr bool, ev ingest.Event, negate bool) bool {
	arr, ok := l.Value.([]any)
	if !ok {
		return false
	}
	got, present := fieldString(l.Field, isAttr, ev)
	matched := false
	hasNull := false
	for _, v := range arr {
		val, isNil, _ := scalarValue(v)
		if isNil {
			hasNull = true
			continue
		}
		if !present {
			continue
		}
		left, right := got, scalarString(val)
		if shouldLower(l) {
			left = strings.ToLower(left)
			right = strings.ToLower(right)
		}
		if left == right {
			matched = true
			break
		}
	}
	if hasNull {
		if !present {
			matched = true
		}
	}
	if negate {
		// NOT IN with NULL in list = match only when present and no value matches.
		if hasNull && !present {
			return false
		}
		return !matched
	}
	return matched
}

func matchSubstring(l Leaf, isAttr bool, ev ingest.Event, anywhere bool) bool {
	got, present := fieldString(l.Field, isAttr, ev)
	if !present {
		return false
	}
	val, isNil, _ := scalarValue(l.Value)
	if isNil {
		return false
	}
	needle := scalarString(val)
	if shouldLower(l) {
		got = strings.ToLower(got)
		needle = strings.ToLower(needle)
	}
	if anywhere {
		return strings.Contains(got, needle)
	}
	return strings.HasPrefix(got, needle)
}

func matchRange(l Leaf, isAttr bool, ev ingest.Event) bool {
	val, isNil, _ := scalarValue(l.Value)
	if isNil {
		return false
	}

	// Timestamp is comparable as time.Time.
	if !isAttr && l.Field == "timestamp" {
		wantTime, ok := parseTimeValue(val)
		if !ok {
			return false
		}
		return compareTimes(l.Op, ev.Timestamp, wantTime)
	}

	got, present := fieldString(l.Field, isAttr, ev)
	if !present {
		return false
	}
	if isAttr {
		if a, ok := toFloat(got); ok {
			if b, ok := toFloat(val); ok {
				return compareNumbers(l.Op, a, b)
			}
		}
	}
	want := scalarString(val)
	return compareStrings(l.Op, got, want)
}

func compareNumbers(op Op, a, b float64) bool {
	switch op {
	case OpGt:
		return a > b
	case OpLt:
		return a < b
	case OpGte:
		return a >= b
	case OpLte:
		return a <= b
	}
	return false
}

func compareStrings(op Op, a, b string) bool {
	switch op {
	case OpGt:
		return a > b
	case OpLt:
		return a < b
	case OpGte:
		return a >= b
	case OpLte:
		return a <= b
	}
	return false
}

func compareTimes(op Op, a, b time.Time) bool {
	switch op {
	case OpGt:
		return a.After(b)
	case OpLt:
		return a.Before(b)
	case OpGte:
		return !a.Before(b)
	case OpLte:
		return !a.After(b)
	}
	return false
}

// coreFieldValue returns the field's string representation and whether
// it is present (non-empty / non-zero).
func coreFieldValue(field string, ev ingest.Event) (string, bool) {
	switch field {
	case "timestamp":
		if ev.Timestamp.IsZero() {
			return "", false
		}
		return ev.Timestamp.UTC().Format(time.RFC3339Nano), true
	case "service":
		return ev.Service, ev.Service != ""
	case "level":
		return ev.Level, ev.Level != ""
	case "message":
		return ev.Message, ev.Message != ""
	case "host":
		return ev.Host, ev.Host != ""
	case "source":
		return ev.Source, ev.Source != ""
	case "raw":
		return ev.Raw, ev.Raw != ""
	}
	return "", false
}

func attrValue(path string, attrs map[string]any) (any, bool) {
	if len(attrs) == 0 || path == "" {
		return nil, false
	}
	parts := strings.Split(path, ".")
	var cur any = attrs
	for _, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		v, ok := m[p]
		if !ok {
			return nil, false
		}
		cur = v
	}
	return cur, cur != nil
}

func fieldString(field string, isAttr bool, ev ingest.Event) (string, bool) {
	if !isAttr {
		return coreFieldValue(field, ev)
	}
	v, ok := attrValue(field[len(AttributePrefix):], ev.Attributes)
	if !ok {
		return "", false
	}
	return scalarToString(v)
}

func scalarToString(v any) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "", false
	case string:
		return x, true
	case bool:
		if x {
			return "true", true
		}
		return "false", true
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10), true
		}
		return strconv.FormatFloat(x, 'f', -1, 64), true
	case int:
		return strconv.FormatInt(int64(x), 10), true
	case int64:
		return strconv.FormatInt(x, 10), true
	case json.Number:
		return x.String(), true
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return "", false
		}
		return string(b), true
	}
}

// scalarString formats a scalar (already passed through scalarValue) as
// a string for comparison. Mirrors the way the SQL compiler binds args:
// numbers, bools, and strings end up textual on the comparison side.
func scalarString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func parseTimeValue(v any) (time.Time, bool) {
	switch x := v.(type) {
	case time.Time:
		return x, true
	case string:
		if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
			return t, true
		}
		if t, err := time.Parse(time.RFC3339, x); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
