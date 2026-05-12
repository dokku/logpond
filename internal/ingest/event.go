// Package ingest implements Logpond's ingest path: HTTP body parsing,
// per-source field extraction (PRD §7.1), and the in-memory ring buffer
// that hands events off to the segment manager.
package ingest

import "time"

// Event is the normalized row that Logpond stores. Field semantics match
// the row schema in PRD §7.2.
type Event struct {
	Timestamp  time.Time
	Service    string
	Level      string
	Message    string
	Host       string
	Source     string
	Attributes map[string]any
	Raw        string
}
