package query

import (
	"testing"
	"time"

	"github.com/dokku/logpond/internal/ingest"
)

func makeEvent() ingest.Event {
	return ingest.Event{
		Timestamp: time.Date(2026, 5, 12, 14, 0, 0, 0, time.UTC),
		Service:   "api",
		Level:     "error",
		Message:   "request failed for user 42",
		Host:      "host-1",
		Source:    "default",
		Raw:       "raw payload",
		Attributes: map[string]any{
			"user_id": float64(42),
			"trace": map[string]any{
				"id":   "abc",
				"slow": true,
			},
			"latency": "1500",
		},
	}
}

func TestMatch_LeafEq(t *testing.T) {
	ev := makeEvent()
	if !Match(Leaf{Field: "service", Op: OpEq, Value: "api"}, ev) {
		t.Fatal("service:api should match")
	}
	if !Match(Leaf{Field: "service", Op: OpEq, Value: "API"}, ev) {
		t.Fatal("case-insensitive match expected")
	}
	if Match(Leaf{Field: "service", Op: OpEq, Value: "web"}, ev) {
		t.Fatal("service:web should NOT match")
	}
	if !Match(Leaf{Field: "service", Op: OpNeq, Value: "web"}, ev) {
		t.Fatal("neq web should match")
	}
}

func TestMatch_LeafAttributes(t *testing.T) {
	ev := makeEvent()
	cases := []struct {
		name string
		leaf Leaf
		want bool
	}{
		{"user_id eq 42", Leaf{Field: "attributes.user_id", Op: OpEq, Value: float64(42)}, true},
		{"user_id eq 43", Leaf{Field: "attributes.user_id", Op: OpEq, Value: float64(43)}, false},
		{"trace.id eq abc", Leaf{Field: "attributes.trace.id", Op: OpEq, Value: "abc"}, true},
		{"trace.slow exists", Leaf{Field: "attributes.trace.slow", Op: OpExists}, true},
		{"missing.field exists", Leaf{Field: "attributes.missing.field", Op: OpExists}, false},
		{"latency >= 1000", Leaf{Field: "attributes.latency", Op: OpGte, Value: float64(1000)}, true},
		{"latency > 9999", Leaf{Field: "attributes.latency", Op: OpGt, Value: float64(9999)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Match(tc.leaf, ev); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMatch_LeafContainsStartsWith(t *testing.T) {
	ev := makeEvent()
	if !Match(Leaf{Field: "message", Op: OpContains, Value: "FAILED"}, ev) {
		t.Fatal("contains FAILED (case-insensitive) should match")
	}
	if !Match(Leaf{Field: "message", Op: OpStartsWith, Value: "request"}, ev) {
		t.Fatal("starts_with request should match")
	}
	if Match(Leaf{Field: "message", Op: OpStartsWith, Value: "user"}, ev) {
		t.Fatal("starts_with user should NOT match")
	}
}

func TestMatch_LeafIn(t *testing.T) {
	ev := makeEvent()
	in := Leaf{Field: "level", Op: OpIn, Value: []any{"error", "warn"}}
	if !Match(in, ev) {
		t.Fatal("level in error/warn should match")
	}
	notIn := Leaf{Field: "level", Op: OpNotIn, Value: []any{"info", "debug"}}
	if !Match(notIn, ev) {
		t.Fatal("level not_in info/debug should match")
	}
}

func TestMatch_Group(t *testing.T) {
	ev := makeEvent()
	and := Group{Op: OpAnd, Children: []Node{
		Leaf{Field: "service", Op: OpEq, Value: "api"},
		Leaf{Field: "level", Op: OpEq, Value: "error"},
	}}
	if !Match(and, ev) {
		t.Fatal("AND group should match")
	}
	or := Group{Op: OpOr, Children: []Node{
		Leaf{Field: "service", Op: OpEq, Value: "web"},
		Leaf{Field: "level", Op: OpEq, Value: "error"},
	}}
	if !Match(or, ev) {
		t.Fatal("OR group should match (level matches)")
	}
	notG := Group{Op: OpAnd, Not: true, Children: []Node{
		Leaf{Field: "service", Op: OpEq, Value: "web"},
	}}
	if !Match(notG, ev) {
		t.Fatal("NOT(service:web) should match")
	}
}

func TestMatchSearch(t *testing.T) {
	ev := makeEvent()
	if !MatchSearch("FAILED", ev) {
		t.Fatal("search FAILED should match (case-insensitive)")
	}
	if !MatchSearch("raw payload", ev) {
		t.Fatal("search against raw should match")
	}
	if MatchSearch("nope", ev) {
		t.Fatal("search nope should NOT match")
	}
	if !MatchSearch("", ev) {
		t.Fatal("empty search matches everything")
	}
}

func TestMatch_NilTreeMatches(t *testing.T) {
	if !Match(nil, makeEvent()) {
		t.Fatal("nil filter should match")
	}
}
