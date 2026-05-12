# Search syntax

The search bar accepts a single line of text in a Datadog-style query language. You type predicates (`field:value`), combine them with `AND`/`OR`/`NOT`, and mix in unquoted free-text terms for substring search. The same syntax is what `POST /api/parse-query` parses and what `POST /api/query` accepts as the `q` field.

If you have used Datadog's log explorer, this will feel familiar. If you have not: the rest of this page builds it up operator by operator with worked examples. Every example shows the search-bar string and notes its effect.

## Field equality

Type `field:value` for an exact-equal predicate. The value is unquoted unless it contains whitespace or one of the reserved characters `:()[]{}"\`.

```
service:api
```

That matches events whose `service` column equals `api`.

For values with spaces, quote with double quotes:

```
service:"my web app"
```

The five **core fields** (`service`, `level`, `host`, `source`, `message`) are reachable directly by name. Anything else lives under `attributes` and is reached with the `@` prefix described below.

## Filtering on attributes with `@`

Events carry a JSON `attributes` object alongside the core columns (see [Sources and Extraction](sources-and-extraction.md)). To filter on an attribute, prefix the field name with `@`:

```
@user.id:42
```

The `@` is sugar for `attributes.` - `@user.id` and `attributes.user.id` are the same path. Dotted paths walk into nested objects.

Numeric-looking values are parsed as numbers. To force a string match, quote it:

```
@user.id:"42"
```

`@user.id:42` matches numeric 42; `@user.id:"42"` matches the string `"42"`. Most of the time you do not care which - Logpond does lenient type coercion on attribute filters and will match either. The distinction matters only when your producer is inconsistent and you want to find the cases where it sent the wrong type.

## Multi-value lists (in / not_in)

For "this field is one of these values," wrap the alternatives in parentheses:

```
level:(error OR warn)
```

A comma is an equivalent separator:

```
level:(error, warn)
```

Both compile to a single `in` predicate, which is much cheaper than `level:error OR level:warn`.

## Negation

Two forms work and mean the same thing:

```
service:api -level:debug
```

```
service:api NOT level:debug
```

The leading `-` is shorter; `NOT` reads better when you are building up a complex query. Both produce a negated predicate, not an inequality. (For one-off `!=`, the API supports a `neq` op directly; the search bar does not have a syntax for it.)

## Boolean combinators

`AND` between two predicates is implicit:

```
service:api level:error
```

That is `service:api AND level:error`. You can type the `AND` if you want it for clarity. `OR` is never implicit:

```
service:api OR service:worker
```

Operator precedence, tightest to loosest, is:

1. Parentheses.
2. Predicate (`field:value`).
3. `NOT` / `-`.
4. `AND` (implicit or explicit).
5. `OR`.

So this:

```
service:api AND (level:error OR (level:warn AND @duration_ms:>1000))
```

parses the way the parentheses tell it to: any `api` event that is either `error`, or `warn` with a duration above one second. The keywords `AND`, `OR`, `NOT`, `TO` are case-insensitive, but the UI conventionally uppercases them.

## Ranges

For numeric or time ranges on attributes, the bracket forms work:

```
@duration_ms:[1000 TO 5000]
```

Square brackets are inclusive (`gte`/`lte`); curly braces are exclusive (`gt`/`lt`):

```
@duration_ms:{1000 TO 5000}
```

Open-ended ranges use `*`:

```
@duration_ms:[1000 TO *]
```

If you only need one-sided, the shorthand operators are tidier:

```
@duration_ms:>1000
@duration_ms:>=1000
@duration_ms:<5000
@duration_ms:<=5000
```

## Wildcards

Trailing `*` becomes a prefix match (`starts_with`), which is the fast path:

```
service:api-*
```

Leading or middle wildcards (`*-api`, `ap*i`) and single-character `?` wildcards are accepted but compile to a slower full-scan substring match. The UI shows a "this query will be slow" warning when it sees one. Use trailing-only wildcards if you can.

## Existence check

`field:*` asks "is this attribute present and non-null on this event?":

```
@user.id:*
```

That matches any event with a `user.id` attribute, whatever value it has. Useful for finding events from a code path that adds a tracing field, regardless of its value.

## Free text

Unquoted bare tokens that are not `field:value` predicates are treated as free-text search terms. Multiple terms are AND-combined and matched against the `message` and `raw` columns case-insensitively:

```
connection refused
```

That matches events whose message or raw line contains both `connection` and `refused`.

To search for a literal phrase with whitespace, quote it:

```
"connection refused"
```

You can mix free-text and predicates freely:

```
service:api "connection refused"
```

That is `service:api AND <free-text "connection refused">`.

## Reserved characters and escaping

The characters `:`, `(`, `)`, `[`, `]`, `{`, `}`, `"`, `\`, and unescaped whitespace are reserved. To put them inside a value, wrap it in double quotes:

```
message:"user said: hi"
```

Inside a quoted string, `\"` escapes a double quote and `\\` escapes a backslash.

## Empty query

An empty search bar means "all events in the time range." The API receives no `filter` and no `search`; the time-range picker alone governs the result set. This is the right starting point for "what is going on right now" - select a recent window and let the facet sidebar show you what is moving.

## Case sensitivity

String operators (`eq`, `neq`, `contains`, `starts_with`) are case-insensitive by default. The search bar has no inline switch for this in v1. To opt into case-sensitive matching, either:

- toggle the "case sensitive" checkbox in the UI (planned, but check whether it has shipped); or
- call the API directly with a leaf node that has `"case_sensitive": true`.

## A few more worked examples

Errors from a particular Dokku app in the last hour:

```
service:my-app level:error
```

Slow API requests with a non-debug level:

```
service:api -level:debug @duration_ms:>500
```

Anything from one of two hosts that mentions a specific user:

```
host:(host-1 OR host-2) @user.id:42
```

Free text plus a structured constraint, with grouping:

```
service:api AND (level:error OR @duration_ms:[1000 TO *]) "timeout"
```

## What the parser produces (for API users)

If you are calling Logpond from a script, you have two choices: send the search string as `q` and let Logpond parse it, or build the canonical **filter tree** in JSON yourself and send it as `filter`. Both are accepted by `POST /api/query`.

To see what a search string parses to without executing it, use `POST /api/parse-query`:

```bash
curl -X POST http://localhost:8080/api/parse-query \
  -H 'Content-Type: application/json' \
  -d '{"q":"service:api level:(error OR warn) -host:debug-*"}'
```

The response shows the canonical tree:

```json
{
  "filter": {
    "op": "and",
    "children": [
      { "field": "service", "op": "eq", "value": "api" },
      { "field": "level",   "op": "in", "value": ["error", "warn"] },
      {
        "op": "and",
        "not": true,
        "children": [
          { "field": "host", "op": "starts_with", "value": "debug-" }
        ]
      }
    ]
  },
  "search": null,
  "warnings": []
}
```

Every leaf has a `field`, an `op` (`eq`, `neq`, `in`, `not_in`, `contains`, `starts_with`, `gt`, `lt`, `gte`, `lte`, `exists`), and a `value` (omitted for `exists`). Group nodes have an `op` of `and` or `or` plus a `children` array and an optional `not: true` for negation. The full schema is in [API Reference](api-reference.md#post-apiquery).

Nesting is capped at 32 levels deep. Beyond that you get `filter_too_deep`, which only triggers on programmatically generated queries.
