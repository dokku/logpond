package parser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dokku/logpond/internal/query"
)

// Result is the output of Parse.
//
// Filter is nil when the input contained only free-text or was empty.
// Search is the concatenation of free-text terms in source order; "" if
// none. Warnings collects non-fatal observations (slow wildcards,
// non-core bare fields, free-text terms compiled into message-contains
// leaves).
type Result struct {
	Filter   query.Node
	Search   string
	Warnings []string
}

// ParseError is returned for malformed inputs. Col is the 1-based
// column at which the error was detected; the API layer maps this onto
// `details.column` in the `invalid_query_syntax` error envelope.
type ParseError struct {
	Col int
	Msg string
}

func (e ParseError) Error() string { return fmt.Sprintf("col %d: %s", e.Col, e.Msg) }

// Parse parses a Datadog-style search bar string. An empty or
// whitespace-only input yields the zero-value Result.
func Parse(input string) (Result, error) {
	toks, err := tokenize(input)
	if err != nil {
		var le lexErr
		if errors.As(err, &le) {
			return Result{}, ParseError{Col: le.Col, Msg: le.Msg}
		}
		return Result{}, ParseError{Col: 1, Msg: err.Error()}
	}
	p := &parser{toks: toks}
	if p.peek().kind == tkEOF {
		return Result{Warnings: []string{}}, nil
	}
	root, err := p.parseDisjunction()
	if err != nil {
		return Result{}, err
	}
	if p.peek().kind != tkEOF {
		t := p.peek()
		return Result{}, ParseError{Col: t.col, Msg: fmt.Sprintf("unexpected token %s", describeToken(t))}
	}
	filter, search := p.finalize(root)
	warnings := p.warnings
	if warnings == nil {
		warnings = []string{}
	}
	return Result{Filter: filter, Search: search, Warnings: warnings}, nil
}

// pNode is the parser's internal tree. It carries pTerm (a free-text
// term) alongside pGroup and pLeaf so the final lifting pass can decide
// whether each term belongs in the `search` field or compiled into a
// `message contains` leaf.
type pNode interface{ isPNode() }

type pGroup struct {
	op       query.GroupOp
	not      bool
	children []pNode
	// flat is set on AND groups produced by a multi-factor conjunction
	// (a sequence of factors joined by explicit/implicit AND). The
	// finalize pass unwraps a flat root so the outer AND envelope is
	// flat rather than doubly-nested — `service:api level:error`
	// produces `AND[service, level]` rather than `AND[AND[service,
	// level]]`. AND groups built by a range expression keep flat=false
	// so their nested structure survives the outer wrap (matches the
	// §7.3.2 range example output).
	flat bool
}

func (pGroup) isPNode() {}

type pLeaf struct {
	leaf query.Leaf
}

func (pLeaf) isPNode() {}

type pTerm struct {
	value string
}

func (pTerm) isPNode() {}

type parser struct {
	toks     []token
	pos      int
	warnings []string
}

func (p *parser) peek() token { return p.toks[p.pos] }

func (p *parser) peekAt(offset int) token {
	i := p.pos + offset
	if i >= len(p.toks) {
		return p.toks[len(p.toks)-1]
	}
	return p.toks[i]
}

func (p *parser) advance() token {
	t := p.toks[p.pos]
	p.pos++
	return t
}

func (p *parser) warn(msg string) { p.warnings = append(p.warnings, msg) }

func (p *parser) peekKeyword(word string) bool {
	t := p.peek()
	return t.kind == tkIdent && strings.EqualFold(t.text, word)
}

// canStartFactor reports whether the current token can begin a new
// factor (used to detect implicit AND between predicates).
func (p *parser) canStartFactor() bool {
	t := p.peek()
	switch t.kind {
	case tkEOF, tkRParen, tkRBracket, tkRBrace, tkComma:
		return false
	case tkIdent:
		if strings.EqualFold(t.text, "OR") || strings.EqualFold(t.text, "AND") || strings.EqualFold(t.text, "TO") {
			return false
		}
		return true
	case tkLParen, tkString, tkAt:
		return true
	}
	return false
}

func (p *parser) parseDisjunction() (pNode, error) {
	left, err := p.parseConjunction()
	if err != nil {
		return nil, err
	}
	branches := []pNode{left}
	for p.peekKeyword("OR") {
		p.advance()
		right, err := p.parseConjunction()
		if err != nil {
			return nil, err
		}
		branches = append(branches, right)
	}
	if len(branches) == 1 {
		return branches[0], nil
	}
	return pGroup{op: query.OpOr, children: branches}, nil
}

func (p *parser) parseConjunction() (pNode, error) {
	left, err := p.parseFactor()
	if err != nil {
		return nil, err
	}
	branches := []pNode{left}
	for {
		if p.peekKeyword("AND") {
			p.advance()
			right, err := p.parseFactor()
			if err != nil {
				return nil, err
			}
			branches = append(branches, right)
			continue
		}
		if p.peekKeyword("OR") {
			break
		}
		if p.canStartFactor() {
			right, err := p.parseFactor()
			if err != nil {
				return nil, err
			}
			branches = append(branches, right)
			continue
		}
		break
	}
	if len(branches) == 1 {
		return branches[0], nil
	}
	return pGroup{op: query.OpAnd, children: branches, flat: true}, nil
}

func (p *parser) parseFactor() (pNode, error) {
	negated := false
	if p.peekKeyword("NOT") {
		p.advance()
		negated = true
	} else if p.peek().kind == tkIdent && strings.HasPrefix(p.peek().text, "-") {
		stripped := p.peek().text[1:]
		negated = true
		if stripped == "" {
			p.advance()
		} else {
			p.toks[p.pos].text = stripped
		}
	}
	atom, err := p.parseAtom()
	if err != nil {
		return nil, err
	}
	if !negated {
		return atom, nil
	}
	// Per §7.3.2's negation example, NOT is represented as
	// `{op:and, not:true, children:[atom]}` so the tree shape mirrors
	// the source query rather than folding into `neq`.
	return pGroup{op: query.OpAnd, not: true, children: []pNode{atom}}, nil
}

func (p *parser) parseAtom() (pNode, error) {
	t := p.peek()
	switch t.kind {
	case tkLParen:
		p.advance()
		node, err := p.parseDisjunction()
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tkRParen {
			return nil, ParseError{Col: p.peek().col, Msg: "expected ')'"}
		}
		p.advance()
		return node, nil
	case tkAt:
		return p.parseAttributePredicate()
	case tkIdent:
		return p.parseIdentAtom()
	case tkString:
		p.advance()
		return pTerm{value: t.text}, nil
	default:
		return nil, ParseError{Col: t.col, Msg: fmt.Sprintf("unexpected %s", describeToken(t))}
	}
}

var coreFields = map[string]bool{
	"service": true,
	"level":   true,
	"host":    true,
	"source":  true,
	"message": true,
}

func (p *parser) parseIdentAtom() (pNode, error) {
	t := p.peek()
	if p.peekAt(1).kind == tkColon {
		field := t.text
		if !coreFields[field] {
			p.warn(fmt.Sprintf("col %d: bare field %q is not a core field; use @%s for attribute paths", t.col, field, field))
		}
		p.advance() // ident
		p.advance() // colon
		return p.parsePredicateRHS(field)
	}
	p.advance()
	return pTerm{value: t.text}, nil
}

func (p *parser) parseAttributePredicate() (pNode, error) {
	at := p.advance() // @
	if p.peek().kind != tkIdent {
		return nil, ParseError{Col: at.col, Msg: "expected attribute path after '@'"}
	}
	path := p.advance().text
	field := "attributes." + path
	if p.peek().kind != tkColon {
		// `@path` without a colon is interpreted as an exists check on
		// the attribute path. PRD's `field:*` covers the explicit form,
		// but the implicit form is the obvious shorthand.
		return pLeaf{leaf: query.Leaf{Field: field, Op: query.OpExists}}, nil
	}
	p.advance() // :
	return p.parsePredicateRHS(field)
}

func (p *parser) parsePredicateRHS(field string) (pNode, error) {
	t := p.peek()
	switch t.kind {
	case tkGt:
		p.advance()
		v, err := p.parseScalar()
		if err != nil {
			return nil, err
		}
		return pLeaf{leaf: query.Leaf{Field: field, Op: query.OpGt, Value: coerceValue(v)}}, nil
	case tkGte:
		p.advance()
		v, err := p.parseScalar()
		if err != nil {
			return nil, err
		}
		return pLeaf{leaf: query.Leaf{Field: field, Op: query.OpGte, Value: coerceValue(v)}}, nil
	case tkLt:
		p.advance()
		v, err := p.parseScalar()
		if err != nil {
			return nil, err
		}
		return pLeaf{leaf: query.Leaf{Field: field, Op: query.OpLt, Value: coerceValue(v)}}, nil
	case tkLte:
		p.advance()
		v, err := p.parseScalar()
		if err != nil {
			return nil, err
		}
		return pLeaf{leaf: query.Leaf{Field: field, Op: query.OpLte, Value: coerceValue(v)}}, nil
	case tkLBracket:
		return p.parseRange(field, true)
	case tkLBrace:
		return p.parseRange(field, false)
	case tkLParen:
		return p.parseValueList(field)
	case tkIdent, tkString:
		v, _ := p.parseScalar()
		return p.makeValueLeaf(field, v), nil
	default:
		return nil, ParseError{Col: t.col, Msg: "expected value after ':'"}
	}
}

type scalar struct {
	text   string
	quoted bool
	col    int
}

func (p *parser) parseScalar() (scalar, error) {
	t := p.peek()
	if t.kind == tkString {
		p.advance()
		return scalar{text: t.text, quoted: true, col: t.col}, nil
	}
	if t.kind == tkIdent {
		p.advance()
		return scalar{text: t.text, quoted: false, col: t.col}, nil
	}
	return scalar{}, ParseError{Col: t.col, Msg: "expected value"}
}

func (p *parser) makeValueLeaf(field string, v scalar) pNode {
	if v.quoted {
		return pLeaf{leaf: query.Leaf{Field: field, Op: query.OpEq, Value: v.text}}
	}
	if v.text == "*" {
		return pLeaf{leaf: query.Leaf{Field: field, Op: query.OpExists}}
	}
	if strings.ContainsAny(v.text, "*?") {
		// Trailing `*` with no other wildcards → starts_with.
		if strings.HasSuffix(v.text, "*") {
			prefix := v.text[:len(v.text)-1]
			if !strings.ContainsAny(prefix, "*?") {
				return pLeaf{leaf: query.Leaf{Field: field, Op: query.OpStartsWith, Value: prefix}}
			}
		}
		p.warn(fmt.Sprintf("col %d: non-prefix wildcard in %s will be slow", v.col, field))
		stripped := strings.ReplaceAll(strings.ReplaceAll(v.text, "*", ""), "?", "")
		return pLeaf{leaf: query.Leaf{Field: field, Op: query.OpContains, Value: stripped}}
	}
	return pLeaf{leaf: query.Leaf{Field: field, Op: query.OpEq, Value: coerceValue(v)}}
}

func (p *parser) parseRange(field string, inclusive bool) (pNode, error) {
	openTok := p.advance() // [ or {
	closeKind := tkRBracket
	closeLabel := "]"
	if !inclusive {
		closeKind = tkRBrace
		closeLabel = "}"
	}

	var lo, hi *scalar
	if p.peek().kind == tkIdent || p.peek().kind == tkString {
		v, err := p.parseScalar()
		if err != nil {
			return nil, err
		}
		lo = &v
	}
	if !p.peekKeyword("TO") {
		return nil, ParseError{Col: p.peek().col, Msg: "expected 'TO' in range"}
	}
	p.advance() // TO
	if p.peek().kind == tkIdent || p.peek().kind == tkString {
		v, err := p.parseScalar()
		if err != nil {
			return nil, err
		}
		hi = &v
	}
	if p.peek().kind != closeKind {
		return nil, ParseError{Col: p.peek().col, Msg: fmt.Sprintf("expected closing %q", closeLabel)}
	}
	p.advance()

	var children []pNode
	if lo != nil && lo.text != "*" {
		op := query.OpGte
		if !inclusive {
			op = query.OpGt
		}
		children = append(children, pLeaf{leaf: query.Leaf{Field: field, Op: op, Value: coerceValue(*lo)}})
	}
	if hi != nil && hi.text != "*" {
		op := query.OpLte
		if !inclusive {
			op = query.OpLt
		}
		children = append(children, pLeaf{leaf: query.Leaf{Field: field, Op: op, Value: coerceValue(*hi)}})
	}
	switch len(children) {
	case 0:
		// `[* TO *]` is functionally an existence check.
		_ = openTok
		return pLeaf{leaf: query.Leaf{Field: field, Op: query.OpExists}}, nil
	case 1:
		return children[0], nil
	}
	return pGroup{op: query.OpAnd, children: children}, nil
}

func (p *parser) parseValueList(field string) (pNode, error) {
	p.advance() // (
	var values []any
	for {
		if p.peek().kind != tkIdent && p.peek().kind != tkString {
			return nil, ParseError{Col: p.peek().col, Msg: "expected value in list"}
		}
		v, _ := p.parseScalar()
		values = append(values, coerceValue(v))
		switch {
		case p.peek().kind == tkComma:
			p.advance()
			continue
		case p.peekKeyword("OR"):
			p.advance()
			continue
		}
		break
	}
	if p.peek().kind != tkRParen {
		return nil, ParseError{Col: p.peek().col, Msg: "expected ')'"}
	}
	p.advance()
	if len(values) == 1 {
		return pLeaf{leaf: query.Leaf{Field: field, Op: query.OpEq, Value: values[0]}}, nil
	}
	return pLeaf{leaf: query.Leaf{Field: field, Op: query.OpIn, Value: values}}, nil
}

// coerceValue applies the §7.3.2 numeric-vs-string rule: bare tokens
// that parse as int or float become numbers; quoted strings stay
// strings.
func coerceValue(v scalar) any {
	if v.quoted {
		return v.text
	}
	if n, err := strconv.ParseInt(v.text, 10, 64); err == nil {
		return n
	}
	if n, err := strconv.ParseFloat(v.text, 64); err == nil {
		return n
	}
	return v.text
}

// finalize wraps the root in the outer AND envelope shown across the
// §7.3.2 examples, then lifts free-text terms that sit at the top
// level into the `search` string. Terms nested under OR/NOT are
// compiled into `message contains` leaves so they can still influence
// the boolean tree.
func (p *parser) finalize(root pNode) (query.Node, string) {
	if root == nil {
		return nil, ""
	}
	var topChildren []pNode
	if g, ok := root.(pGroup); ok && g.op == query.OpAnd && !g.not && g.flat {
		topChildren = g.children
	} else {
		topChildren = []pNode{root}
	}
	var terms []string
	var keep []pNode
	for _, c := range topChildren {
		if t, ok := c.(pTerm); ok {
			terms = append(terms, t.value)
			continue
		}
		keep = append(keep, c)
	}
	for i, c := range keep {
		keep[i] = p.convertNestedTerms(c)
	}
	search := strings.Join(terms, " ")
	if len(keep) == 0 {
		return nil, search
	}
	out := query.Group{Op: query.OpAnd, Children: make([]query.Node, 0, len(keep))}
	for _, c := range keep {
		out.Children = append(out.Children, compile(c))
	}
	return out, search
}

// convertNestedTerms walks the tree and replaces any pTerm it finds
// with a `message contains` leaf, emitting a warning so the UI can
// surface the implicit conversion.
func (p *parser) convertNestedTerms(n pNode) pNode {
	switch x := n.(type) {
	case pGroup:
		out := pGroup{op: x.op, not: x.not, children: make([]pNode, len(x.children))}
		for i, c := range x.children {
			out.children[i] = p.convertNestedTerms(c)
		}
		return out
	case pTerm:
		p.warn(fmt.Sprintf("free-text term %q inside grouping or negation compiled as message contains", x.value))
		return pLeaf{leaf: query.Leaf{Field: "message", Op: query.OpContains, Value: x.value}}
	}
	return n
}

func compile(n pNode) query.Node {
	switch x := n.(type) {
	case pLeaf:
		return x.leaf
	case pGroup:
		g := query.Group{Op: x.op, Not: x.not, Children: make([]query.Node, len(x.children))}
		for i, c := range x.children {
			g.Children[i] = compile(c)
		}
		return g
	case pTerm:
		// Unreachable in practice: finalize/convertNestedTerms handle
		// every pTerm before compile runs. Compile defensively into a
		// message contains so a stray term never crashes the pipeline.
		return query.Leaf{Field: "message", Op: query.OpContains, Value: x.value}
	}
	return nil
}

func describeToken(t token) string {
	if t.kind == tkIdent || t.kind == tkString {
		return fmt.Sprintf("%q", t.text)
	}
	return fmt.Sprintf("'%s'", t.kind.String())
}
