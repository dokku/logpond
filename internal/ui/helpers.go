package ui

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/dokku/logpond/internal/query"
)

// levelClass maps a log level to the CSS class on its chip. Empty level
// returns "" so the template can render a non-chip span.
func levelClass(level *string) string {
	if level == nil {
		return ""
	}
	switch strings.ToLower(*level) {
	case "debug":
		return "level-chip level-debug"
	case "info":
		return "level-chip level-info"
	case "warn", "warning":
		return "level-chip level-warn"
	case "error", "err", "fatal", "critical":
		return "level-chip level-error"
	}
	return "level-chip"
}

func levelLabel(level *string) string {
	if level == nil {
		return ""
	}
	return strings.ToUpper(*level)
}

// formatCount renders an int64 with US thousand separators.
func formatCount(n int64) string {
	if n < 0 {
		return "-" + formatCount(-n)
	}
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	first := len(s) % 3
	parts := make([]string, 0, len(s)/3+1)
	if first > 0 {
		parts = append(parts, s[:first])
	}
	for i := first; i < len(s); i += 3 {
		parts = append(parts, s[i:i+3])
	}
	return strings.Join(parts, ",")
}

// shorten trims a string to n characters with an ellipsis. Used for the
// collapsed message preview in result rows; the full text is in the
// expand-on-click panel below.
func shorten(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// jsonString marshals a value to compact JSON for embedding in
// data-attributes or Alpine x-data blocks.
func jsonString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// timestampISO renders a query result's timestamp as a UTC RFC3339Nano
// string. Templates feed this into data-ts attributes; the browser-side
// helper in app.js then renders the local-time face.
func timestampISO(ev query.ResultEvent) string {
	return ev.Timestamp.UTC().Format(time.RFC3339Nano)
}
