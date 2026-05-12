// Package query implements Logpond's filter tree, SQL compiler, and
// query executor. Reference sections in the PRD: §7.3.1 (canonical
// filter tree), §7.3.4 (semantics), §13.4 (POST /api/query).
package query

import (
	"encoding/json"
	"errors"
	"fmt"
)

// MaxFilterDepth is the cap on nested groups in a filter tree. Going
// deeper returns the `filter_too_deep` error code (§7.3.1).
const MaxFilterDepth = 32

// Op enumerates the leaf-predicate operators supported by Logpond
// (§7.3.1).
type Op string

const (
	OpEq         Op = "eq"
	OpNeq        Op = "neq"
	OpIn         Op = "in"
	OpNotIn      Op = "not_in"
	OpContains   Op = "contains"
	OpStartsWith Op = "starts_with"
	OpGt         Op = "gt"
	OpLt         Op = "lt"
	OpGte        Op = "gte"
	OpLte        Op = "lte"
	OpExists     Op = "exists"
)

// GroupOp enumerates boolean combinators for group nodes.
type GroupOp string

const (
	OpAnd GroupOp = "and"
	OpOr  GroupOp = "or"
)

// Node is implemented by both Group and Leaf. The interface is small on
// purpose; compilation pattern-matches against the concrete types.
type Node interface {
	isNode()
}

// Group combines child nodes with AND or OR, optionally negated.
type Group struct {
	Op       GroupOp `json:"op"`
	Not      bool    `json:"not,omitempty"`
	Children []Node  `json:"children"`
}

func (Group) isNode() {}

// Leaf is a single field predicate.
type Leaf struct {
	Field         string `json:"field"`
	Op            Op     `json:"op"`
	Value         any    `json:"value,omitempty"`
	CaseSensitive bool   `json:"case_sensitive,omitempty"`
}

func (Leaf) isNode() {}

// UnmarshalNode parses a single node (group or leaf). The dispatch key
// is presence of `children` (group) vs `field` (leaf).
func UnmarshalNode(raw json.RawMessage) (Node, error) {
	var probe struct {
		Children json.RawMessage `json:"children"`
		Field    *string         `json:"field"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("decoding node: %w", err)
	}
	if probe.Children != nil {
		return unmarshalGroup(raw)
	}
	if probe.Field != nil {
		return unmarshalLeaf(raw)
	}
	return nil, fmt.Errorf("node has neither 'children' nor 'field'")
}

func unmarshalGroup(raw json.RawMessage) (Group, error) {
	var aux struct {
		Op       GroupOp           `json:"op"`
		Not      bool              `json:"not"`
		Children []json.RawMessage `json:"children"`
	}
	if err := json.Unmarshal(raw, &aux); err != nil {
		return Group{}, fmt.Errorf("decoding group: %w", err)
	}
	g := Group{Op: aux.Op, Not: aux.Not, Children: make([]Node, 0, len(aux.Children))}
	for i, c := range aux.Children {
		n, err := UnmarshalNode(c)
		if err != nil {
			return Group{}, fmt.Errorf("children[%d]: %w", i, err)
		}
		g.Children = append(g.Children, n)
	}
	return g, nil
}

func unmarshalLeaf(raw json.RawMessage) (Leaf, error) {
	var l Leaf
	if err := json.Unmarshal(raw, &l); err != nil {
		return Leaf{}, fmt.Errorf("decoding leaf: %w", err)
	}
	return l, nil
}

// MarshalJSON tags group nodes so the JSON output is round-trip safe.
func (g Group) MarshalJSON() ([]byte, error) {
	aux := struct {
		Op       GroupOp `json:"op"`
		Not      bool    `json:"not,omitempty"`
		Children []Node  `json:"children"`
	}{g.Op, g.Not, g.Children}
	return json.Marshal(aux)
}

// Validate walks the tree, checking depth, operator legality, and the
// op/value shape contract (§7.3.1 leaf rules).
func Validate(n Node) error {
	return validate(n, 1)
}

func validate(n Node, depth int) error {
	switch x := n.(type) {
	case Group:
		if depth > MaxFilterDepth {
			return errFilterTooDeep
		}
		if x.Op != OpAnd && x.Op != OpOr {
			return fmt.Errorf("invalid group op %q (want %q or %q)", x.Op, OpAnd, OpOr)
		}
		if len(x.Children) == 0 {
			return fmt.Errorf("group has no children")
		}
		for i, c := range x.Children {
			if err := validate(c, depth+1); err != nil {
				return fmt.Errorf("children[%d]: %w", i, err)
			}
		}
		return nil
	case Leaf:
		return validateLeaf(x)
	default:
		return fmt.Errorf("unknown node type %T", n)
	}
}

func validateLeaf(l Leaf) error {
	if l.Field == "" {
		return fmt.Errorf("leaf missing field")
	}
	switch l.Op {
	case OpEq, OpNeq, OpContains, OpStartsWith, OpGt, OpLt, OpGte, OpLte:
		if l.Value == nil {
			return fmt.Errorf("op %q requires value", l.Op)
		}
	case OpIn, OpNotIn:
		arr, ok := l.Value.([]any)
		if !ok || len(arr) == 0 {
			return fmt.Errorf("op %q requires non-empty array value", l.Op)
		}
	case OpExists:
		// value not required
	default:
		return fmt.Errorf("invalid op %q", l.Op)
	}
	return nil
}

// ErrFilterTooDeep is returned when the tree nests beyond MaxFilterDepth.
// The handler maps it to the `filter_too_deep` error code.
var ErrFilterTooDeep = errFilterTooDeep

var errFilterTooDeep = filterTooDeepErr{}

type filterTooDeepErr struct{}

func (filterTooDeepErr) Error() string { return "filter_too_deep" }

// IsFilterTooDeep reports whether err originated from depth enforcement.
func IsFilterTooDeep(err error) bool {
	var target filterTooDeepErr
	return errors.As(err, &target)
}

// AsAnd wraps n in an implicit-AND group if it is a leaf, otherwise
// returns it unchanged. Used to normalise Form C (flat filters) into a
// canonical tree before compilation.
func AsAnd(n Node) Group {
	if g, ok := n.(Group); ok {
		return g
	}
	return Group{Op: OpAnd, Children: []Node{n}}
}

// LeavesFromFlat wraps a flat filter list (Form C) as an AND group.
func LeavesFromFlat(leaves []Leaf) Group {
	g := Group{Op: OpAnd, Children: make([]Node, 0, len(leaves))}
	for _, l := range leaves {
		g.Children = append(g.Children, l)
	}
	return g
}
