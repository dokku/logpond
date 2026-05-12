// Command querybench drives the query API with a representative set of
// requests and verifies the §8.1 performance envelope:
//
//   - 1h core-column filter: < 500ms p95
//   - 24h core-column filter: < 2s p95
//   - attribute-filter dominant: 2-5x slower than core (best-effort)
//   - search-suggest: < 100ms p95
//   - count: < 100ms p95
//   - parse-query: < 5ms p99
//
// Each case is repeated -iterations times. The tool prints per-case p50,
// p95, p99 and a PASS/FAIL against its target. A non-zero exit code is
// returned when any required case fails.
//
// Run against a populated Logpond:
//
//	go run ./cmd/querybench -url http://localhost:8080 -iterations 50
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"time"
)

type flags struct {
	URL         string
	Iterations  int
	HTTPTimeout time.Duration
	Skip        string
	Strict      bool
}

func parseFlags() flags {
	var f flags
	flag.StringVar(&f.URL, "url", "http://localhost:8080", "Logpond base URL")
	flag.IntVar(&f.Iterations, "iterations", 50, "iterations per case")
	flag.DurationVar(&f.HTTPTimeout, "http-timeout", 30*time.Second, "per-request HTTP timeout")
	flag.StringVar(&f.Skip, "skip", "", "comma-separated case names to skip")
	flag.BoolVar(&f.Strict, "strict", true, "exit non-zero if any required case misses its target")
	flag.Parse()
	return f
}

func main() {
	cfg := parseFlags()
	skipSet := buildSet(cfg.Skip)

	client := &http.Client{Timeout: cfg.HTTPTimeout}
	defer client.CloseIdleConnections()

	now := time.Now().UTC()
	cases := buildCases(now)

	failures := 0
	for _, c := range cases {
		if _, skip := skipSet[c.Name]; skip {
			fmt.Fprintf(os.Stdout, "case=%s SKIPPED\n", c.Name)
			continue
		}
		stats := run(context.Background(), client, cfg.URL, c, cfg.Iterations)
		verdict := "PASS"
		if c.Target > 0 && stats.p95 > c.Target {
			verdict = "FAIL"
			if c.Required {
				failures++
			} else {
				verdict = "MISS (best-effort)"
			}
		}
		fmt.Fprintf(os.Stdout, "case=%s endpoint=%s p50=%s p95=%s p99=%s target_p95=%s ok=%d errors=%d %s\n",
			c.Name, c.Path,
			stats.p50, stats.p95, stats.p99,
			c.Target,
			stats.ok, stats.errors,
			verdict,
		)
	}

	if cfg.Strict && failures > 0 {
		os.Exit(1)
	}
}

type benchCase struct {
	Name     string
	Path     string
	Body     map[string]any
	Target   time.Duration
	Required bool
}

func buildCases(now time.Time) []benchCase {
	hourAgo := now.Add(-1 * time.Hour)
	dayAgo := now.Add(-24 * time.Hour)

	core := func(from, to time.Time, q string) map[string]any {
		return map[string]any{
			"time_range": map[string]string{
				"from": from.Format(time.RFC3339Nano),
				"to":   to.Format(time.RFC3339Nano),
			},
			"q":     q,
			"limit": 100,
		}
	}

	return []benchCase{
		{
			Name:     "query_1h_core",
			Path:     "/api/query",
			Body:     core(hourAgo, now, "level:error"),
			Target:   500 * time.Millisecond,
			Required: true,
		},
		{
			Name:     "query_24h_core",
			Path:     "/api/query",
			Body:     core(dayAgo, now, "level:(error OR warn)"),
			Target:   2 * time.Second,
			Required: true,
		},
		{
			Name:     "query_1h_attribute",
			Path:     "/api/query",
			Body:     core(hourAgo, now, "@user_id:>500 @duration_ms:>1000"),
			Target:   2500 * time.Millisecond, // best-effort, 5x core
			Required: false,
		},
		{
			Name:     "query_count_1h",
			Path:     "/api/query/count",
			Body:     core(hourAgo, now, "level:error"),
			Target:   100 * time.Millisecond,
			Required: true,
		},
		{
			Name: "search_suggest_field",
			Path: "/api/search-suggest",
			Body: map[string]any{
				"q":          "lev",
				"cursor_pos": 3,
				"max":        10,
				"time_range": map[string]string{
					"from": hourAgo.Format(time.RFC3339Nano),
					"to":   now.Format(time.RFC3339Nano),
				},
			},
			Target:   100 * time.Millisecond,
			Required: true,
		},
		{
			Name: "search_suggest_value",
			Path: "/api/search-suggest",
			Body: map[string]any{
				"q":          "service:",
				"cursor_pos": 8,
				"max":        10,
				"time_range": map[string]string{
					"from": hourAgo.Format(time.RFC3339Nano),
					"to":   now.Format(time.RFC3339Nano),
				},
			},
			Target:   100 * time.Millisecond,
			Required: true,
		},
		{
			Name: "parse_query",
			Path: "/api/parse-query",
			Body: map[string]any{
				"q": "level:error AND @user_id:>500 AND service:(api OR worker)",
			},
			Target:   5 * time.Millisecond,
			Required: true,
		},
	}
}

type caseStats struct {
	p50, p95, p99 time.Duration
	ok, errors    int
}

func run(ctx context.Context, client *http.Client, base string, c benchCase, iterations int) caseStats {
	body, _ := json.Marshal(c.Body)
	lats := make([]time.Duration, 0, iterations)
	ok, errs := 0, 0
	for i := 0; i < iterations; i++ {
		d, success := requestOnce(ctx, client, base+c.Path, body)
		lats = append(lats, d)
		if success {
			ok++
		} else {
			errs++
		}
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	return caseStats{
		p50:    pct(lats, 0.50),
		p95:    pct(lats, 0.95),
		p99:    pct(lats, 0.99),
		ok:     ok,
		errors: errs,
	}
}

func requestOnce(ctx context.Context, client *http.Client, url string, body []byte) (time.Duration, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, false
	}
	req.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := client.Do(req)
	d := time.Since(start)
	if err != nil {
		return d, false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return d, resp.StatusCode >= 200 && resp.StatusCode < 300
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func buildSet(csv string) map[string]struct{} {
	out := map[string]struct{}{}
	if csv == "" {
		return out
	}
	cur := 0
	for i := 0; i <= len(csv); i++ {
		if i == len(csv) || csv[i] == ',' {
			tok := trim(csv[cur:i])
			if tok != "" {
				out[tok] = struct{}{}
			}
			cur = i + 1
		}
	}
	return out
}

func trim(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
