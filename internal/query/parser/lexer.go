// Package parser implements Logpond's Datadog-style search bar parser
// (PRD §7.3.2 / §7.3.3). It converts a single line of query text into
// the canonical filter tree from §7.3.1 plus a free-text search string.
package parser

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

type tokenKind int

const (
	tkEOF tokenKind = iota
	tkIdent
	tkString
	tkColon
	tkAt
	tkLParen
	tkRParen
	tkLBracket
	tkRBracket
	tkLBrace
	tkRBrace
	tkComma
	tkGt
	tkGte
	tkLt
	tkLte
)

func (k tokenKind) String() string {
	switch k {
	case tkEOF:
		return "EOF"
	case tkIdent:
		return "ident"
	case tkString:
		return "string"
	case tkColon:
		return ":"
	case tkAt:
		return "@"
	case tkLParen:
		return "("
	case tkRParen:
		return ")"
	case tkLBracket:
		return "["
	case tkRBracket:
		return "]"
	case tkLBrace:
		return "{"
	case tkRBrace:
		return "}"
	case tkComma:
		return ","
	case tkGt:
		return ">"
	case tkGte:
		return ">="
	case tkLt:
		return "<"
	case tkLte:
		return "<="
	}
	return "unknown"
}

// token is one lexer output. For idents and strings, text holds the
// value (with quoted-string escapes already resolved). col is the
// 1-based column the token begins at.
type token struct {
	kind tokenKind
	text string
	col  int
}

// lexErr is returned when the lexer cannot consume the next character.
type lexErr struct {
	Col int
	Msg string
}

func (e lexErr) Error() string { return fmt.Sprintf("at col %d: %s", e.Col, e.Msg) }

// tokenize splits input into tokens. Whitespace is skipped between
// tokens; identifiers stop at whitespace or a recognized punctuation
// character.
func tokenize(input string) ([]token, error) {
	var toks []token
	i := 0
	for i < len(input) {
		c := input[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}
		col := i + 1
		switch c {
		case ':':
			toks = append(toks, token{kind: tkColon, col: col})
			i++
			continue
		case '@':
			toks = append(toks, token{kind: tkAt, col: col})
			i++
			continue
		case '(':
			toks = append(toks, token{kind: tkLParen, col: col})
			i++
			continue
		case ')':
			toks = append(toks, token{kind: tkRParen, col: col})
			i++
			continue
		case '[':
			toks = append(toks, token{kind: tkLBracket, col: col})
			i++
			continue
		case ']':
			toks = append(toks, token{kind: tkRBracket, col: col})
			i++
			continue
		case '{':
			toks = append(toks, token{kind: tkLBrace, col: col})
			i++
			continue
		case '}':
			toks = append(toks, token{kind: tkRBrace, col: col})
			i++
			continue
		case ',':
			toks = append(toks, token{kind: tkComma, col: col})
			i++
			continue
		case '>':
			if i+1 < len(input) && input[i+1] == '=' {
				toks = append(toks, token{kind: tkGte, col: col})
				i += 2
			} else {
				toks = append(toks, token{kind: tkGt, col: col})
				i++
			}
			continue
		case '<':
			if i+1 < len(input) && input[i+1] == '=' {
				toks = append(toks, token{kind: tkLte, col: col})
				i += 2
			} else {
				toks = append(toks, token{kind: tkLt, col: col})
				i++
			}
			continue
		case '"':
			start := i
			i++
			var sb strings.Builder
			closed := false
			for i < len(input) {
				if input[i] == '\\' && i+1 < len(input) {
					next := input[i+1]
					switch next {
					case '"':
						sb.WriteByte('"')
					case '\\':
						sb.WriteByte('\\')
					default:
						sb.WriteByte('\\')
						sb.WriteByte(next)
					}
					i += 2
					continue
				}
				if input[i] == '"' {
					closed = true
					i++
					break
				}
				sb.WriteByte(input[i])
				i++
			}
			if !closed {
				return nil, lexErr{Col: start + 1, Msg: "unclosed quoted string"}
			}
			toks = append(toks, token{kind: tkString, text: sb.String(), col: start + 1})
			continue
		}

		// Bare identifier: greedily consume identifier characters.
		start := i
		for i < len(input) && isIdentByte(input[i]) {
			i++
		}
		if i == start {
			r, _ := utf8.DecodeRuneInString(input[i:])
			return nil, lexErr{Col: col, Msg: fmt.Sprintf("unexpected character %q", r)}
		}
		toks = append(toks, token{kind: tkIdent, text: input[start:i], col: col})
	}
	toks = append(toks, token{kind: tkEOF, col: len(input) + 1})
	return toks, nil
}

func isIdentByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z':
		return true
	case c >= 'A' && c <= 'Z':
		return true
	case c >= '0' && c <= '9':
		return true
	}
	switch c {
	case '_', '.', '-', '*', '?', '/', '+':
		return true
	}
	return false
}
