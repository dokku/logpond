package ingest

import (
	"strings"
	"testing"
	"time"
)

var defaultSource = SourceSpec{
	Name: "default",
	Extract: map[string][]string{
		"timestamp": {"timestamp", "ts", "time", "@timestamp"},
		"level":     {"level", "severity"},
		"message":   {"message", "msg"},
		"service":   {"service", "app"},
		"host":      {"host", "hostname"},
	},
}

func newTestExtractor(now time.Time) *Extractor {
	e := NewExtractor(defaultSource)
	e.Now = func() time.Time { return now }
	return e
}

func TestExtract_HappyPath_PopulatesCoreColumnsAndStripsAttributes(t *testing.T) {
	e := newTestExtractor(time.Now())
	line := []byte(`{"timestamp":"2026-05-12T14:32:01.123Z","level":"INFO","service":"api","host":"web-1","message":"hello","user_id":42}`)
	ev, err := e.Extract(line)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if ev.Service != "api" {
		t.Errorf("service: got %q", ev.Service)
	}
	if ev.Host != "web-1" {
		t.Errorf("host: got %q", ev.Host)
	}
	if ev.Level != "info" {
		t.Errorf("level: got %q", ev.Level)
	}
	if ev.Message != "hello" {
		t.Errorf("message: got %q", ev.Message)
	}
	if got := ev.Timestamp.UTC().Format(time.RFC3339Nano); got != "2026-05-12T14:32:01.123Z" {
		t.Errorf("timestamp: got %q", got)
	}
	// Extracted columns must be removed; user_id stays.
	for _, key := range []string{"timestamp", "level", "service", "host", "message"} {
		if _, ok := ev.Attributes[key]; ok {
			t.Errorf("attributes still has %q", key)
		}
	}
	if v, ok := ev.Attributes["user_id"]; !ok {
		t.Errorf("attributes missing user_id")
	} else if vs := toString(v); vs != "42" {
		t.Errorf("user_id: got %q", vs)
	}
	if v, ok := ev.Attributes["logpond_level_original"]; !ok || v != "INFO" {
		t.Errorf("missing/incorrect logpond_level_original: got %v", v)
	}
}

func TestExtract_LevelNormalizationTable(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{"trace", "debug"}, {"debug", "debug"}, {"dbg", "debug"}, {"0", "debug"}, {"1", "debug"},
		{"info", "info"}, {"information", "info"}, {"notice", "info"}, {"2", "info"}, {"3", "info"},
		{"warn", "warn"}, {"warning", "warn"}, {"4", "warn"},
		{"error", "error"}, {"err", "error"}, {"critical", "error"}, {"crit", "error"},
		{"fatal", "error"}, {"emerg", "error"}, {"alert", "error"}, {"panic", "error"},
		{"5", "error"}, {"6", "error"}, {"7", "error"},
		{"WARN", "warn"}, {"Error", "error"},
	}
	e := newTestExtractor(time.Now())
	for _, c := range cases {
		t.Run(c.input, func(t *testing.T) {
			line := []byte(`{"timestamp":"2026-05-12T00:00:00Z","level":"` + c.input + `","message":"x"}`)
			ev, err := e.Extract(line)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			if ev.Level != c.expected {
				t.Errorf("level %q: got %q, want %q", c.input, ev.Level, c.expected)
			}
		})
	}
}

func TestExtract_LevelUnrecognizedPreservesOriginal(t *testing.T) {
	e := newTestExtractor(time.Now())
	ev, err := e.Extract([]byte(`{"timestamp":"2026-05-12T00:00:00Z","level":"weird","message":"x"}`))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if ev.Level != "info" {
		t.Errorf("level: got %q", ev.Level)
	}
	if v, _ := ev.Attributes["logpond_level_unrecognized"]; v != "weird" {
		t.Errorf("logpond_level_unrecognized: got %v", v)
	}
}

func TestExtract_LevelSyslogNumberFromInteger(t *testing.T) {
	e := newTestExtractor(time.Now())
	ev, err := e.Extract([]byte(`{"timestamp":"2026-05-12T00:00:00Z","level":3,"message":"x"}`))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if ev.Level != "info" {
		t.Errorf("level: got %q", ev.Level)
	}
}

func TestExtract_TimestampUnixSeconds(t *testing.T) {
	e := newTestExtractor(time.Now())
	ev, err := e.Extract([]byte(`{"timestamp":1715520000,"level":"info","message":"x"}`))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if ev.Timestamp.Unix() != 1715520000 {
		t.Errorf("timestamp: got %d", ev.Timestamp.Unix())
	}
}

func TestExtract_TimestampUnixMilliseconds(t *testing.T) {
	e := newTestExtractor(time.Now())
	ev, err := e.Extract([]byte(`{"timestamp":1715520000123,"level":"info","message":"x"}`))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if got := ev.Timestamp.UnixMilli(); got != 1715520000123 {
		t.Errorf("timestamp: got %d", got)
	}
}

func TestExtract_TimestampFallbackSetsAttribute(t *testing.T) {
	fixed := time.Date(2026, 5, 12, 0, 0, 0, 0, time.UTC)
	e := newTestExtractor(fixed)
	ev, err := e.Extract([]byte(`{"level":"info","message":"no time here"}`))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if !ev.Timestamp.Equal(fixed) {
		t.Errorf("timestamp: got %v want %v", ev.Timestamp, fixed)
	}
	if v, _ := ev.Attributes["logpond_timestamp_fallback"]; v != true {
		t.Errorf("logpond_timestamp_fallback: got %v", v)
	}
}

func TestExtract_TimestampCandidateOrder(t *testing.T) {
	e := newTestExtractor(time.Now())
	ev, err := e.Extract([]byte(`{"ts":"2026-05-12T00:00:00Z","time":"2030-01-01T00:00:00Z","level":"info","message":"x"}`))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if ev.Timestamp.Year() != 2026 {
		t.Errorf("expected first candidate (ts/2026), got %v", ev.Timestamp)
	}
}

func TestExtract_TimestampSkipsNonScalarCandidates(t *testing.T) {
	e := newTestExtractor(time.Now())
	ev, err := e.Extract([]byte(`{"timestamp":["bad"],"ts":"2026-05-12T00:00:00Z","level":"info"}`))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if ev.Timestamp.Year() != 2026 {
		t.Errorf("expected ts fallback, got %v", ev.Timestamp)
	}
	if _, ok := ev.Attributes["timestamp"]; !ok {
		t.Errorf("unparseable timestamp candidate should stay in attributes")
	}
}

func TestExtract_LevelMissingDefaultsToInfo(t *testing.T) {
	e := newTestExtractor(time.Now())
	ev, err := e.Extract([]byte(`{"timestamp":"2026-05-12T00:00:00Z","message":"hi"}`))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if ev.Level != "info" {
		t.Errorf("level: got %q", ev.Level)
	}
	if v, _ := ev.Attributes["logpond_level_fallback"]; v != true {
		t.Errorf("logpond_level_fallback not set; attrs=%v", ev.Attributes)
	}
}

func TestExtract_DottedAttributePath(t *testing.T) {
	source := SourceSpec{
		Name: "nested",
		Extract: map[string][]string{
			"timestamp": {"timestamp"},
			"level":     {"meta.level"},
			"message":   {"msg"},
		},
	}
	e := NewExtractor(source)
	ev, err := e.Extract([]byte(`{"timestamp":"2026-05-12T00:00:00Z","meta":{"level":"warning"},"msg":"hi"}`))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if ev.Level != "warn" {
		t.Errorf("level: got %q", ev.Level)
	}
	meta, ok := ev.Attributes["meta"].(map[string]any)
	if !ok {
		t.Fatalf("meta missing or wrong type")
	}
	if _, ok := meta["level"]; ok {
		t.Errorf("nested level should have been removed; meta=%v", meta)
	}
}

func TestExtract_RejectsNonObject(t *testing.T) {
	e := newTestExtractor(time.Now())
	cases := [][]byte{
		[]byte(`["a","b"]`),
		[]byte(`null`),
		[]byte(`42`),
		[]byte(`"a string"`),
		[]byte(`{`),
	}
	for _, in := range cases {
		if _, err := e.Extract(in); err == nil {
			t.Errorf("expected error for %q", string(in))
		}
	}
}

func TestExtract_MessageCoercesNonStrings(t *testing.T) {
	e := newTestExtractor(time.Now())
	ev, err := e.Extract([]byte(`{"timestamp":"2026-05-12T00:00:00Z","level":"info","message":42}`))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if ev.Message != "42" {
		t.Errorf("message: got %q", ev.Message)
	}
}

func TestExtract_RawPreservesOriginalLine(t *testing.T) {
	e := newTestExtractor(time.Now())
	raw := `{"timestamp":"2026-05-12T00:00:00Z","level":"info","message":"hi","x":1}`
	ev, err := e.Extract([]byte(raw))
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if ev.Raw != raw {
		t.Errorf("raw: got %q want %q", ev.Raw, raw)
	}
}

func toString(v any) string {
	type stringer interface{ String() string }
	if s, ok := v.(string); ok {
		return s
	}
	if s, ok := v.(stringer); ok {
		return s.String()
	}
	return strings.TrimSpace(strings.TrimSpace(strings.ReplaceAll(toJSON(v), `"`, "")))
}

func toJSON(v any) string {
	type marshaller interface{ MarshalJSON() ([]byte, error) }
	if m, ok := v.(marshaller); ok {
		if b, err := m.MarshalJSON(); err == nil {
			return string(b)
		}
	}
	return ""
}
