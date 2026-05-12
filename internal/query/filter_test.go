package query_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dokku/logpond/internal/query"
)

func TestUnmarshalNode_LeafAndGroup(t *testing.T) {
	raw := []byte(`{
        "op": "and",
        "children": [
            {"field": "service", "op": "eq", "value": "api"},
            {"field": "level",   "op": "in", "value": ["error","warn"]}
        ]
    }`)
	n, err := query.UnmarshalNode(raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	g, ok := n.(query.Group)
	if !ok {
		t.Fatalf("expected group, got %T", n)
	}
	if g.Op != query.OpAnd {
		t.Errorf("op = %q", g.Op)
	}
	if len(g.Children) != 2 {
		t.Fatalf("children = %d", len(g.Children))
	}
	if _, ok := g.Children[0].(query.Leaf); !ok {
		t.Errorf("children[0] not leaf")
	}
}

func TestValidate_DepthLimit(t *testing.T) {
	// Build a tree of depth 33 (over the cap).
	var node query.Node = query.Leaf{Field: "service", Op: query.OpEq, Value: "x"}
	for i := 0; i < 33; i++ {
		node = query.Group{Op: query.OpAnd, Children: []query.Node{node}}
	}
	err := query.Validate(node)
	if err == nil || !query.IsFilterTooDeep(err) {
		t.Fatalf("expected filter_too_deep, got %v", err)
	}

	// Depth 32 still passes.
	node = query.Leaf{Field: "service", Op: query.OpEq, Value: "x"}
	for i := 0; i < 31; i++ {
		node = query.Group{Op: query.OpAnd, Children: []query.Node{node}}
	}
	if err := query.Validate(node); err != nil {
		t.Errorf("expected depth-32 pass, got %v", err)
	}
}

func TestValidate_OpValueShape(t *testing.T) {
	cases := []struct {
		name string
		leaf query.Leaf
		ok   bool
	}{
		{"in needs array", query.Leaf{Field: "level", Op: query.OpIn, Value: "error"}, false},
		{"in ok", query.Leaf{Field: "level", Op: query.OpIn, Value: []any{"error"}}, true},
		{"exists no value", query.Leaf{Field: "attributes.user_id", Op: query.OpExists}, true},
		{"unknown op", query.Leaf{Field: "level", Op: "fuzzy", Value: "x"}, false},
		{"eq needs value", query.Leaf{Field: "service", Op: query.OpEq}, false},
		{"missing field", query.Leaf{Op: query.OpEq, Value: "x"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := query.Validate(tc.leaf)
			gotOK := err == nil
			if gotOK != tc.ok {
				t.Errorf("validate err=%v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestCompile_CoreFields(t *testing.T) {
	cases := []struct {
		name     string
		node     query.Node
		wantSQL  string // substring match
		wantArgs int
	}{
		{
			name:     "eq case-insensitive",
			node:     query.Leaf{Field: "service", Op: query.OpEq, Value: "API"},
			wantSQL:  `LOWER("service") = LOWER(?)`,
			wantArgs: 1,
		},
		{
			name:     "eq case-sensitive",
			node:     query.Leaf{Field: "service", Op: query.OpEq, Value: "API", CaseSensitive: true},
			wantSQL:  `"service" = ?`,
			wantArgs: 1,
		},
		{
			name:     "in list",
			node:     query.Leaf{Field: "level", Op: query.OpIn, Value: []any{"error", "warn"}},
			wantSQL:  `LOWER("level") IN (LOWER(?), LOWER(?))`,
			wantArgs: 2,
		},
		{
			name:     "contains",
			node:     query.Leaf{Field: "message", Op: query.OpContains, Value: "refused"},
			wantSQL:  `LIKE LOWER(?)`,
			wantArgs: 1,
		},
		{
			name:     "exists attribute",
			node:     query.Leaf{Field: "attributes.user_id", Op: query.OpExists},
			wantSQL:  `json_extract(attributes, '$.user_id') IS NOT NULL`,
			wantArgs: 0,
		},
		{
			name:     "gt attribute numeric coercion",
			node:     query.Leaf{Field: "attributes.duration_ms", Op: query.OpGt, Value: float64(1000)},
			wantSQL:  `CAST(json_extract_string(attributes, '$.duration_ms') AS DOUBLE) > ?`,
			wantArgs: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := query.Compile(tc.node)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			if !strings.Contains(c.SQL, tc.wantSQL) {
				t.Errorf("sql = %q, want substr %q", c.SQL, tc.wantSQL)
			}
			if len(c.Args) != tc.wantArgs {
				t.Errorf("args = %v, want %d", c.Args, tc.wantArgs)
			}
		})
	}
}

func TestCompile_AndGroup(t *testing.T) {
	node := query.Group{Op: query.OpAnd, Children: []query.Node{
		query.Leaf{Field: "service", Op: query.OpEq, Value: "api", CaseSensitive: true},
		query.Leaf{Field: "level", Op: query.OpEq, Value: "error", CaseSensitive: true},
	}}
	c, err := query.Compile(node)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if !strings.Contains(c.SQL, " AND ") {
		t.Errorf("expected AND in %q", c.SQL)
	}
}

func TestUnmarshalNode_ValueArrayPreserved(t *testing.T) {
	// Sanity: the unmarshaler keeps Value as []any so Validate's array
	// check still works.
	raw := []byte(`{"field":"level","op":"in","value":["a","b"]}`)
	n, err := query.UnmarshalNode(raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	l := n.(query.Leaf)
	arr, ok := l.Value.([]any)
	if !ok {
		t.Fatalf("value not []any (was %T)", l.Value)
	}
	if len(arr) != 2 {
		t.Errorf("len=%d, want 2", len(arr))
	}
	// Round-trip through JSON.
	b, err := json.Marshal(l)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"in"`) {
		t.Errorf("marshal output missing op: %s", b)
	}
}
