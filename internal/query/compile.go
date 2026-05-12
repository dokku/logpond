package query

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// CoreColumns lists the fields directly stored as columns (PRD §7.2).
var CoreColumns = map[string]bool{
	"timestamp": true,
	"service":   true,
	"level":     true,
	"message":   true,
	"host":      true,
	"source":    true,
	"raw":       true,
}

// AttributePrefix marks a field path that addresses the JSON attributes
// column rather than a core column.
const AttributePrefix = "attributes."

// Compiled is a SQL fragment plus its bound arguments.
type Compiled struct {
	SQL  string
	Args []any
}

// Compile turns a filter tree into a DuckDB WHERE-clause fragment.
// Returns an empty SQL string for a tree with no leaves (which the
// caller treats as TRUE).
func Compile(root Node) (Compiled, error) {
	var args []any
	sql, err := compileNode(root, &args)
	if err != nil {
		return Compiled{}, err
	}
	return Compiled{SQL: sql, Args: args}, nil
}

func compileNode(n Node, args *[]any) (string, error) {
	switch x := n.(type) {
	case Group:
		if len(x.Children) == 0 {
			return "", fmt.Errorf("group has no children")
		}
		parts := make([]string, 0, len(x.Children))
		for _, c := range x.Children {
			s, err := compileNode(c, args)
			if err != nil {
				return "", err
			}
			if s == "" {
				continue
			}
			parts = append(parts, "("+s+")")
		}
		if len(parts) == 0 {
			return "", nil
		}
		sep := " AND "
		if x.Op == OpOr {
			sep = " OR "
		}
		s := strings.Join(parts, sep)
		if x.Not {
			s = "NOT (" + s + ")"
		}
		return s, nil
	case Leaf:
		return compileLeaf(x, args)
	default:
		return "", fmt.Errorf("unsupported node type %T", n)
	}
}

func compileLeaf(l Leaf, args *[]any) (string, error) {
	if l.Field == "" {
		return "", fmt.Errorf("leaf missing field")
	}
	isAttr := strings.HasPrefix(l.Field, AttributePrefix)
	if !isAttr && !CoreColumns[l.Field] {
		return "", fmt.Errorf("unknown field %q", l.Field)
	}

	switch l.Op {
	case OpExists:
		return compileExists(l.Field, isAttr), nil
	case OpEq, OpNeq:
		return compileEq(l, isAttr, args)
	case OpIn, OpNotIn:
		return compileIn(l, isAttr, args)
	case OpContains:
		return compileLike(l, isAttr, args, "%", "%")
	case OpStartsWith:
		return compileLike(l, isAttr, args, "", "%")
	case OpGt, OpLt, OpGte, OpLte:
		return compileRange(l, isAttr, args)
	default:
		return "", fmt.Errorf("invalid op %q", l.Op)
	}
}

// columnExpr returns the SQL expression for the field. For core
// columns it is the bare identifier; for attribute paths it is the
// json_extract_string call.
func columnExpr(field string, isAttr bool) string {
	if !isAttr {
		return quoteIdent(field)
	}
	path := field[len(AttributePrefix):]
	return fmt.Sprintf("json_extract_string(attributes, '$.%s')", escapeSingleQuotes(path))
}

// columnRawJSON returns the raw json_extract expression (returns JSON
// value, not text). Used for `exists` checks where we want to
// distinguish a missing key from a null value.
func columnRawJSON(field string) string {
	path := field[len(AttributePrefix):]
	return fmt.Sprintf("json_extract(attributes, '$.%s')", escapeSingleQuotes(path))
}

func compileExists(field string, isAttr bool) string {
	if !isAttr {
		return quoteIdent(field) + " IS NOT NULL"
	}
	return columnRawJSON(field) + " IS NOT NULL"
}

func compileEq(l Leaf, isAttr bool, args *[]any) (string, error) {
	col := columnExpr(l.Field, isAttr)
	val, isNil, err := scalarValue(l.Value)
	if err != nil {
		return "", err
	}
	if isNil {
		if l.Op == OpEq {
			return col + " IS NULL", nil
		}
		return col + " IS NOT NULL", nil
	}
	left, right := col, "?"
	if shouldLower(l) {
		left = "LOWER(" + col + ")"
		right = "LOWER(?)"
		val = lowerString(val)
	}
	op := "="
	if l.Op == OpNeq {
		op = "!="
	}
	*args = append(*args, val)
	return left + " " + op + " " + right, nil
}

func compileIn(l Leaf, isAttr bool, args *[]any) (string, error) {
	arr, ok := l.Value.([]any)
	if !ok {
		return "", fmt.Errorf("op %q requires an array value", l.Op)
	}
	if len(arr) == 0 {
		return "", fmt.Errorf("op %q requires non-empty array", l.Op)
	}
	col := columnExpr(l.Field, isAttr)
	left := col
	useLower := shouldLower(l)
	if useLower {
		left = "LOWER(" + col + ")"
	}
	hasNull := false
	values := make([]any, 0, len(arr))
	for _, v := range arr {
		s, isNil, err := scalarValue(v)
		if err != nil {
			return "", err
		}
		if isNil {
			hasNull = true
			continue
		}
		if useLower {
			s = lowerString(s)
		}
		values = append(values, s)
	}
	var sql string
	if len(values) > 0 {
		placeholders := make([]string, len(values))
		for i := range placeholders {
			placeholders[i] = "?"
			if useLower {
				placeholders[i] = "LOWER(?)"
			}
		}
		if l.Op == OpNotIn {
			sql = left + " NOT IN (" + strings.Join(placeholders, ", ") + ")"
		} else {
			sql = left + " IN (" + strings.Join(placeholders, ", ") + ")"
		}
		*args = append(*args, values...)
	}
	if hasNull {
		nullPred := col + " IS NULL"
		if l.Op == OpNotIn {
			nullPred = col + " IS NOT NULL"
		}
		if sql == "" {
			sql = nullPred
		} else if l.Op == OpNotIn {
			sql = "(" + sql + " AND " + nullPred + ")"
		} else {
			sql = "(" + sql + " OR " + nullPred + ")"
		}
	}
	return sql, nil
}

func compileLike(l Leaf, isAttr bool, args *[]any, leftWild, rightWild string) (string, error) {
	col := columnExpr(l.Field, isAttr)
	val, isNil, err := scalarValue(l.Value)
	if err != nil {
		return "", err
	}
	if isNil {
		return "", fmt.Errorf("op %q does not accept null", l.Op)
	}
	pattern := leftWild + escapeLike(val) + rightWild
	left := col
	patternExpr := "?"
	if shouldLower(l) {
		left = "LOWER(" + col + ")"
		patternExpr = "LOWER(?)"
		pattern = strings.ToLower(pattern)
	}
	*args = append(*args, pattern)
	return left + " LIKE " + patternExpr + ` ESCAPE '\'`, nil
}

func compileRange(l Leaf, isAttr bool, args *[]any) (string, error) {
	col := columnExpr(l.Field, isAttr)
	val, isNil, err := scalarValue(l.Value)
	if err != nil {
		return "", err
	}
	if isNil {
		return "", fmt.Errorf("op %q requires a value", l.Op)
	}
	op := sqlRangeOp(l.Op)
	// Numeric range on attributes: cast both sides to DOUBLE so a
	// stored "1500" string compares as 1500 rather than lexicographically.
	if isAttr {
		if n, ok := toFloat(val); ok {
			*args = append(*args, n)
			return "CAST(" + col + " AS DOUBLE) " + op + " ?", nil
		}
	}
	*args = append(*args, val)
	return col + " " + op + " ?", nil
}

func sqlRangeOp(o Op) string {
	switch o {
	case OpGt:
		return ">"
	case OpLt:
		return "<"
	case OpGte:
		return ">="
	case OpLte:
		return "<="
	}
	return "="
}

// shouldLower decides whether a comparison wraps both sides in
// LOWER(). String-typed ops with case_sensitive=false qualify
// (§7.3.4).
func shouldLower(l Leaf) bool {
	if l.CaseSensitive {
		return false
	}
	switch l.Op {
	case OpEq, OpNeq, OpContains, OpStartsWith, OpIn, OpNotIn:
		return true
	}
	return false
}

// scalarValue normalises an interface{} value pulled from JSON into a
// driver-friendly representation. Numbers become float64/int64; bools
// become bool; nil signals SQL NULL. Returns (value, isNull, err).
func scalarValue(v any) (any, bool, error) {
	switch x := v.(type) {
	case nil:
		return nil, true, nil
	case string:
		return x, false, nil
	case bool:
		return x, false, nil
	case float64:
		if x == float64(int64(x)) {
			return int64(x), false, nil
		}
		return x, false, nil
	case int:
		return int64(x), false, nil
	case int64:
		return x, false, nil
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i, false, nil
		}
		if f, err := x.Float64(); err == nil {
			return f, false, nil
		}
		return x.String(), false, nil
	default:
		return nil, false, fmt.Errorf("unsupported value type %T", v)
	}
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	}
	return 0, false
}

func lowerString(v any) any {
	if s, ok := v.(string); ok {
		return strings.ToLower(s)
	}
	return v
}

// escapeLike escapes the LIKE wildcards (`%`, `_`) and the escape
// character (`\`) so user input is matched literally inside a LIKE
// pattern.
func escapeLike(v any) string {
	s := fmt.Sprintf("%v", v)
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}

func escapeSingleQuotes(s string) string { return strings.ReplaceAll(s, "'", "''") }

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// CompileSearch builds a free-text search predicate against `message`
// and `raw`. Empty input returns an empty Compiled.
func CompileSearch(text string) Compiled {
	text = strings.TrimSpace(text)
	if text == "" {
		return Compiled{}
	}
	pattern := "%" + strings.ToLower(escapeLike(text)) + "%"
	return Compiled{
		SQL:  `(LOWER(message) LIKE ? ESCAPE '\' OR LOWER(raw) LIKE ? ESCAPE '\')`,
		Args: []any{pattern, pattern},
	}
}

// CombineAnd joins two compiled WHERE fragments with AND. Empty
// fragments are skipped so callers can pass optional filters.
func CombineAnd(parts ...Compiled) Compiled {
	keep := parts[:0]
	for _, p := range parts {
		if p.SQL != "" {
			keep = append(keep, p)
		}
	}
	if len(keep) == 0 {
		return Compiled{}
	}
	if len(keep) == 1 {
		return keep[0]
	}
	sqls := make([]string, 0, len(keep))
	args := []any{}
	for _, p := range keep {
		sqls = append(sqls, "("+p.SQL+")")
		args = append(args, p.Args...)
	}
	return Compiled{SQL: strings.Join(sqls, " AND "), Args: args}
}
