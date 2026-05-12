package parser

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/dokku/logpond/internal/query"
)

// canonical re-serializes a node through the API's JSON shape so the
// comparison ignores whitespace and Go map ordering differences.
func canonical(t *testing.T, n any) string {
	t.Helper()
	b, err := json.Marshal(n)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Round-trip through map[string]any to normalize key ordering.
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("normalize unmarshal: %v", err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("normalize marshal: %v", err)
	}
	return string(out)
}

func equalJSON(t *testing.T, got, want any) {
	t.Helper()
	g := canonical(t, got)
	w := canonical(t, want)
	if g != w {
		t.Errorf("filter mismatch\n got:  %s\n want: %s", g, w)
	}
}

func TestParse_PRDExamples_RoundTrip(t *testing.T) {
	cases := []struct {
		name       string
		input      string
		wantTree   string // JSON of the expected filter tree
		wantSearch string
	}{
		{
			name:  "simple_eq",
			input: "service:api",
			wantTree: `{"op":"and","children":[
				{"field":"service","op":"eq","value":"api"}
			]}`,
		},
		{
			name:  "implicit_and",
			input: "service:api level:error",
			wantTree: `{"op":"and","children":[
				{"field":"service","op":"eq","value":"api"},
				{"field":"level","op":"eq","value":"error"}
			]}`,
		},
		{
			name:  "value_list_or",
			input: "level:(error OR warn)",
			wantTree: `{"op":"and","children":[
				{"field":"level","op":"in","value":["error","warn"]}
			]}`,
		},
		{
			name:  "value_list_comma",
			input: "level:(error, warn)",
			wantTree: `{"op":"and","children":[
				{"field":"level","op":"in","value":["error","warn"]}
			]}`,
		},
		{
			name:  "negation_dash",
			input: "service:api -level:debug",
			wantTree: `{"op":"and","children":[
				{"field":"service","op":"eq","value":"api"},
				{"op":"and","not":true,"children":[
					{"field":"level","op":"eq","value":"debug"}
				]}
			]}`,
		},
		{
			name:  "negation_NOT_keyword",
			input: "service:api NOT level:debug",
			wantTree: `{"op":"and","children":[
				{"field":"service","op":"eq","value":"api"},
				{"op":"and","not":true,"children":[
					{"field":"level","op":"eq","value":"debug"}
				]}
			]}`,
		},
		{
			name:  "attribute_path_numeric",
			input: "@user.id:42",
			wantTree: `{"op":"and","children":[
				{"field":"attributes.user.id","op":"eq","value":42}
			]}`,
		},
		{
			name:  "attribute_path_quoted_string",
			input: `@user.id:"42"`,
			wantTree: `{"op":"and","children":[
				{"field":"attributes.user.id","op":"eq","value":"42"}
			]}`,
		},
		{
			name:       "free_text_only",
			input:      `"connection refused"`,
			wantTree:   ``, // nil filter
			wantSearch: "connection refused",
		},
		{
			name:  "mixed_predicate_and_free_text",
			input: `service:api "connection refused"`,
			wantTree: `{"op":"and","children":[
				{"field":"service","op":"eq","value":"api"}
			]}`,
			wantSearch: "connection refused",
		},
		{
			name:  "trailing_wildcard_starts_with",
			input: "service:api-*",
			wantTree: `{"op":"and","children":[
				{"field":"service","op":"starts_with","value":"api-"}
			]}`,
		},
		{
			name:  "range_inclusive",
			input: "@duration_ms:[1000 TO 5000]",
			wantTree: `{"op":"and","children":[
				{"op":"and","children":[
					{"field":"attributes.duration_ms","op":"gte","value":1000},
					{"field":"attributes.duration_ms","op":"lte","value":5000}
				]}
			]}`,
		},
		{
			name:  "range_exclusive",
			input: "@duration_ms:{1000 TO 5000}",
			wantTree: `{"op":"and","children":[
				{"op":"and","children":[
					{"field":"attributes.duration_ms","op":"gt","value":1000},
					{"field":"attributes.duration_ms","op":"lt","value":5000}
				]}
			]}`,
		},
		{
			name:  "range_open_upper",
			input: "@duration_ms:[1000 TO *]",
			wantTree: `{"op":"and","children":[
				{"field":"attributes.duration_ms","op":"gte","value":1000}
			]}`,
		},
		{
			name:  "exists",
			input: "@user.id:*",
			wantTree: `{"op":"and","children":[
				{"field":"attributes.user.id","op":"exists"}
			]}`,
		},
		{
			name:  "comparison_shortcut_gt",
			input: "@duration_ms:>1000",
			wantTree: `{"op":"and","children":[
				{"field":"attributes.duration_ms","op":"gt","value":1000}
			]}`,
		},
		{
			name:  "nested_or_and_grouping",
			input: "service:api AND (level:error OR (level:warn AND @duration_ms:>1000))",
			wantTree: `{"op":"and","children":[
				{"field":"service","op":"eq","value":"api"},
				{"op":"or","children":[
					{"field":"level","op":"eq","value":"error"},
					{"op":"and","children":[
						{"field":"level","op":"eq","value":"warn"},
						{"field":"attributes.duration_ms","op":"gt","value":1000}
					]}
				]}
			]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.input, err)
			}
			if tc.wantTree == "" {
				if res.Filter != nil {
					t.Errorf("Filter = %v, want nil", res.Filter)
				}
			} else {
				var want any
				if err := json.Unmarshal([]byte(tc.wantTree), &want); err != nil {
					t.Fatalf("decoding want JSON: %v", err)
				}
				equalJSON(t, res.Filter, want)
			}
			if res.Search != tc.wantSearch {
				t.Errorf("Search = %q, want %q", res.Search, tc.wantSearch)
			}
		})
	}
}

func TestParse_OperatorPrecedence_AndBindsTighterThanOr(t *testing.T) {
	res, err := Parse("service:a AND level:b OR host:c")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	want := `{"op":"and","children":[
		{"op":"or","children":[
			{"op":"and","children":[
				{"field":"service","op":"eq","value":"a"},
				{"field":"level","op":"eq","value":"b"}
			]},
			{"field":"host","op":"eq","value":"c"}
		]}
	]}`
	var w any
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("decoding want JSON: %v", err)
	}
	equalJSON(t, res.Filter, w)
}

func TestParse_ImplicitAndEqualsExplicitAnd(t *testing.T) {
	a, err := Parse("service:a service:b service:c")
	if err != nil {
		t.Fatalf("Parse implicit: %v", err)
	}
	b, err := Parse("service:a AND service:b AND service:c")
	if err != nil {
		t.Fatalf("Parse explicit: %v", err)
	}
	if canonical(t, a.Filter) != canonical(t, b.Filter) {
		t.Errorf("implicit AND tree differs from explicit AND tree\nimplicit: %s\nexplicit: %s",
			canonical(t, a.Filter), canonical(t, b.Filter))
	}
}

func TestParse_NotKeywordEqualsDashPrefix(t *testing.T) {
	a, err := Parse("NOT level:debug")
	if err != nil {
		t.Fatalf("Parse NOT: %v", err)
	}
	b, err := Parse("-level:debug")
	if err != nil {
		t.Fatalf("Parse dash: %v", err)
	}
	if canonical(t, a.Filter) != canonical(t, b.Filter) {
		t.Errorf("NOT tree differs from -prefix tree\nNOT:  %s\n-:    %s",
			canonical(t, a.Filter), canonical(t, b.Filter))
	}
}

func TestParse_QuoteEscaping_Backslash(t *testing.T) {
	res, err := Parse(`message:"he said \"hi\""`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	want := `{"op":"and","children":[
		{"field":"message","op":"eq","value":"he said \"hi\""}
	]}`
	var w any
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("decoding want JSON: %v", err)
	}
	equalJSON(t, res.Filter, w)
}

func TestParse_EmptyInput_ReturnsEmptyResult(t *testing.T) {
	res, err := Parse("")
	if err != nil {
		t.Fatalf("Parse empty: %v", err)
	}
	if res.Filter != nil {
		t.Errorf("Filter = %v, want nil", res.Filter)
	}
	if res.Search != "" {
		t.Errorf("Search = %q, want empty", res.Search)
	}
}

func TestParse_WhitespaceOnly_ReturnsEmptyResult(t *testing.T) {
	res, err := Parse("   \t  ")
	if err != nil {
		t.Fatalf("Parse whitespace: %v", err)
	}
	if res.Filter != nil || res.Search != "" {
		t.Errorf("got filter=%v search=%q, want empty", res.Filter, res.Search)
	}
}

func TestParse_NonPrefixWildcard_EmitsWarning(t *testing.T) {
	res, err := Parse("service:*-api")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(res.Warnings) == 0 {
		t.Errorf("expected slow-wildcard warning, got none")
	}
	// Verify the leaf is a contains, not starts_with.
	g, ok := res.Filter.(query.Group)
	if !ok {
		t.Fatalf("filter is not a group: %T", res.Filter)
	}
	leaf, ok := g.Children[0].(query.Leaf)
	if !ok {
		t.Fatalf("child is not a leaf: %T", g.Children[0])
	}
	if leaf.Op != query.OpContains {
		t.Errorf("op = %s, want contains", leaf.Op)
	}
}

func TestParse_FreeTextInsideOr_BecomesMessageContains(t *testing.T) {
	res, err := Parse("foo OR bar")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	want := `{"op":"and","children":[
		{"op":"or","children":[
			{"field":"message","op":"contains","value":"foo"},
			{"field":"message","op":"contains","value":"bar"}
		]}
	]}`
	var w any
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("decoding want JSON: %v", err)
	}
	equalJSON(t, res.Filter, w)
	if len(res.Warnings) == 0 {
		t.Errorf("expected warnings for nested free-text, got none")
	}
}

func TestParse_MalformedInputs_ReturnsParseError(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"unmatched_paren", "service:api ("},
		{"unmatched_paren_close", "service:api )"},
		{"dangling_colon", "service:"},
		{"unclosed_quote", `service:"api`},
		{"missing_to_in_range", "@duration_ms:[1000 5000]"},
		{"missing_range_close", "@duration_ms:[1000 TO 5000"},
		{"at_without_path", "@:value"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.input)
			if err == nil {
				t.Fatalf("Parse(%q) succeeded, want error", tc.input)
			}
			var pe ParseError
			if !errors.As(err, &pe) {
				t.Fatalf("err = %v, want ParseError", err)
			}
			if pe.Col <= 0 {
				t.Errorf("Col = %d, want positive", pe.Col)
			}
		})
	}
}

func TestParse_NumericVsQuotedString_DistinctValueTypes(t *testing.T) {
	// Bare numeric → number.
	numRes, err := Parse("@user.id:42")
	if err != nil {
		t.Fatalf("Parse numeric: %v", err)
	}
	numLeaf := mustFirstLeaf(t, numRes.Filter)
	if _, ok := numLeaf.Value.(int64); !ok {
		t.Errorf("bare 42 value type = %T, want int64", numLeaf.Value)
	}
	// Quoted "42" → string.
	strRes, err := Parse(`@user.id:"42"`)
	if err != nil {
		t.Fatalf("Parse quoted: %v", err)
	}
	strLeaf := mustFirstLeaf(t, strRes.Filter)
	if s, ok := strLeaf.Value.(string); !ok || s != "42" {
		t.Errorf(`quoted "42" value = %v (%T), want "42" string`, strLeaf.Value, strLeaf.Value)
	}
}

func TestParse_KeywordsAreCaseInsensitive(t *testing.T) {
	a, _ := Parse("service:a and service:b")
	b, _ := Parse("service:a AND service:b")
	if canonical(t, a.Filter) != canonical(t, b.Filter) {
		t.Errorf("case-insensitive AND mismatch: %s vs %s", canonical(t, a.Filter), canonical(t, b.Filter))
	}
	c, _ := Parse("service:a or service:b")
	d, _ := Parse("service:a OR service:b")
	if canonical(t, c.Filter) != canonical(t, d.Filter) {
		t.Errorf("case-insensitive OR mismatch: %s vs %s", canonical(t, c.Filter), canonical(t, d.Filter))
	}
}

func TestParse_UnknownBareField_EmitsWarning(t *testing.T) {
	res, err := Parse("custom_field:value")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(res.Warnings) == 0 {
		t.Errorf("expected warning about non-core bare field, got none")
	}
}

func TestParse_TrailingTokensAfterValidExpression_ReturnsError(t *testing.T) {
	_, err := Parse("service:api ]")
	if err == nil {
		t.Fatalf("expected error on trailing junk")
	}
}

func TestParseError_FormatsCol(t *testing.T) {
	_, err := Parse(`"abc`)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "col ") {
		t.Errorf("err = %q, expected 'col' prefix", err.Error())
	}
}

// mustFirstLeaf grabs the first leaf out of the outer-AND wrapper.
func mustFirstLeaf(t *testing.T, n query.Node) query.Leaf {
	t.Helper()
	g, ok := n.(query.Group)
	if !ok {
		t.Fatalf("not a group: %T", n)
	}
	if len(g.Children) == 0 {
		t.Fatalf("group has no children")
	}
	leaf, ok := g.Children[0].(query.Leaf)
	if !ok {
		t.Fatalf("first child is not a leaf: %T", g.Children[0])
	}
	return leaf
}
