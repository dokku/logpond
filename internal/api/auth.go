// Per-source bearer-token authentication for the ingest endpoint. See
// PRD §7.1.5. Sources with an empty token list bypass the check; sources
// with at least one token require `Authorization: Bearer <token>` to
// match one of the configured tokens via constant-time compare.

package api

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// tokenMap is the per-source list of accepted bearer tokens. A nil or
// empty slice for a source means "auth not required."
type tokenMap map[string][]string

// authChecker holds the live token map behind an atomic.Pointer so
// reloads can swap it without coordinating with in-flight requests.
type authChecker struct {
	current atomic.Pointer[tokenMap]

	logger  *slog.Logger
	counter *prometheus.CounterVec

	mu          sync.Mutex
	lastWarn    map[string]time.Time // source -> last warning time
	warnEvery   time.Duration        // typically 1 minute
	tokenPrefix int                  // fingerprint length, default 6
}

// newAuthChecker builds a checker with the supplied initial token map.
// counter may be nil during unit tests; the rate-limiter still works.
func newAuthChecker(initial tokenMap, logger *slog.Logger, counter *prometheus.CounterVec) *authChecker {
	if logger == nil {
		logger = slog.Default()
	}
	a := &authChecker{
		logger:      logger,
		counter:     counter,
		lastWarn:    make(map[string]time.Time),
		warnEvery:   time.Minute,
		tokenPrefix: 6,
	}
	a.store(initial)
	return a
}

// store replaces the token map. Safe to call concurrently with check.
// A nil map is treated as an empty map (every source is unauthenticated).
func (a *authChecker) store(next tokenMap) {
	if next == nil {
		next = tokenMap{}
	}
	a.current.Store(&next)
}

// tokensFor returns the token list configured for the named source,
// or nil if the source has no auth requirement.
func (a *authChecker) tokensFor(source string) []string {
	m := a.current.Load()
	if m == nil {
		return nil
	}
	return (*m)[source]
}

// check returns true when the request is allowed to proceed. On failure
// it writes the 401 response, sets `WWW-Authenticate: Bearer`, increments
// the auth-failures counter, and emits a rate-limited warning log
// (one per minute per source) carrying the source name and a 6-character
// token fingerprint.
func (a *authChecker) check(w http.ResponseWriter, r *http.Request, source string) bool {
	tokens := a.tokensFor(source)
	if len(tokens) == 0 {
		return true
	}

	offered, ok := extractBearer(r.Header.Get("Authorization"))
	if ok && matchesAny(offered, tokens) {
		return true
	}

	w.Header().Set("WWW-Authenticate", "Bearer")
	writeError(w, http.StatusUnauthorized, "unauthorized",
		"Authentication required for source '"+source+"'.", nil)

	if a.counter != nil {
		a.counter.WithLabelValues(source).Inc()
	}
	a.logFailure(source, offered)
	return false
}

// extractBearer pulls the token out of an `Authorization: Bearer <tok>`
// header. The token may not contain whitespace. Returns "" / false when
// the header is missing or malformed.
func extractBearer(header string) (string, bool) {
	header = strings.TrimSpace(header)
	if header == "" {
		return "", false
	}
	// Match the scheme case-insensitively but require exactly one space
	// between the scheme and the credential to avoid trimming ambiguity.
	const scheme = "bearer "
	if len(header) <= len(scheme) {
		return "", false
	}
	if !strings.EqualFold(header[:len(scheme)], scheme) {
		return "", false
	}
	tok := strings.TrimSpace(header[len(scheme):])
	if tok == "" || strings.ContainsAny(tok, " \t\r\n") {
		return "", false
	}
	return tok, true
}

// matchesAny returns true when offered constant-time-equals any of the
// configured tokens. We compare against every entry to avoid leaking the
// list length through timing.
func matchesAny(offered string, tokens []string) bool {
	off := []byte(offered)
	hit := 0
	for _, t := range tokens {
		if subtle.ConstantTimeCompare(off, []byte(t)) == 1 {
			hit = 1
		}
	}
	return hit == 1
}

// logFailure emits at most one warning per source per warnEvery window.
// fingerprint is the first tokenPrefix characters of the offered token,
// or "(none)" when no bearer was supplied.
func (a *authChecker) logFailure(source, offered string) {
	a.mu.Lock()
	last, ok := a.lastWarn[source]
	now := time.Now()
	if ok && now.Sub(last) < a.warnEvery {
		a.mu.Unlock()
		return
	}
	a.lastWarn[source] = now
	a.mu.Unlock()

	fp := "(none)"
	if offered != "" {
		n := a.tokenPrefix
		if n > len(offered) {
			n = len(offered)
		}
		fp = offered[:n]
	}
	a.logger.Warn("ingest auth rejected", "source", source, "token_fingerprint", fp)
}
