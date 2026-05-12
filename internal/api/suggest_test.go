package api

import (
	"testing"
)

func TestClassifyContext_Cases(t *testing.T) {
	cases := []struct {
		name    string
		q       string
		pos     int
		kind    contextKind
		field   string
		partial string
	}{
		{name: "empty", q: "", pos: 0, kind: ctxField, partial: ""},
		{name: "partial-field", q: "ser", pos: 3, kind: ctxField, partial: "ser"},
		{name: "after-colon", q: "service:", pos: 8, kind: ctxValue, field: "service", partial: ""},
		{name: "partial-value", q: "service:api", pos: 11, kind: ctxValue, field: "service", partial: "api"},
		{name: "value-list-first", q: "level:(", pos: 7, kind: ctxValue, field: "level", partial: ""},
		{name: "value-list-after-comma", q: "level:(info,", pos: 12, kind: ctxValue, field: "level", partial: ""},
		{name: "attr-after-colon", q: "@user.id:", pos: 9, kind: ctxValue, field: "attributes.user.id", partial: ""},
		{name: "combinator-after-predicate", q: "level:error ", pos: 12, kind: ctxCombinator, partial: ""},
		{name: "after-AND", q: "level:error AND ", pos: 16, kind: ctxField, partial: ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := classifyContext(c.q, c.pos)
			if got.kind != c.kind {
				t.Errorf("kind = %s, want %s", got.kind, c.kind)
			}
			if got.field != c.field {
				t.Errorf("field = %q, want %q", got.field, c.field)
			}
			if got.partial != c.partial {
				t.Errorf("partial = %q, want %q", got.partial, c.partial)
			}
		})
	}
}
