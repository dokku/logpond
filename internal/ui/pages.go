package ui

import (
	"net/http"
	"strconv"
	"time"
)

func (s *Server) handleSearchPage(w http.ResponseWriter, r *http.Request) {
	data := pageData{
		Title:   "Search",
		View:    "search",
		Archive: s.archiveInfo,
		Page:    searchPage{},
	}
	s.renderHTML(w, "search.html", data, http.StatusOK)
}

func (s *Server) handleTailPage(w http.ResponseWriter, r *http.Request) {
	data := pageData{
		Title:   "Live tail",
		View:    "tail",
		Archive: s.archiveInfo,
		Page:    nil,
	}
	s.renderHTML(w, "tail.html", data, http.StatusOK)
}

func (s *Server) handleAdminPage(w http.ResponseWriter, r *http.Request) {
	body := s.buildAdminPage(r)
	data := pageData{
		Title:   "Admin",
		View:    "admin",
		Archive: s.archiveInfo,
		Page:    body,
	}
	s.renderHTML(w, "admin.html", data, http.StatusOK)
}

// buildAdminPage gathers every pane's data in one pass so the initial
// page load is a single SQL/registry sweep. Each fragment endpoint
// re-renders only its slice but shares the same shaping helpers.
func (s *Server) buildAdminPage(r *http.Request) adminPage {
	body := adminPage{
		Retention: retentionView{
			MaxAge:              s.retentionInfo.MaxAge,
			MaxSize:             s.retentionInfo.MaxSize,
			ArchiveBeforeDelete: s.retentionInfo.ArchiveBeforeDelete,
			LastResult:          s.snapshotAdmin().lastRetention,
		},
		Archive:     s.buildArchiveView(),
		Facets:      s.buildFacetTable(),
		Storage:     s.computeStorage(r),
		Segments:    s.buildSegmentsTable(r, "", 0, defaultSegmentsLimit),
		SampleSize:  s.facetSampleSize,
		FacetFields: facetFieldSuggestions(),
	}
	return body
}

// defaultSegmentsLimit caps the admin segment table at a value that
// keeps the rendered table reasonable without forcing pagination on
// small deployments.
const defaultSegmentsLimit = 50

// parseQueryInt reads a query parameter as int, with a fallback when
// missing or invalid.
func parseQueryInt(r *http.Request, name string, fallback int) int {
	v := r.URL.Query().Get(name)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

// snapshotAdmin returns a shallow copy of the cached admin result
// pointers. The mutex protects against concurrent mutation by handlers.
func (s *Server) snapshotAdmin() adminLiveState {
	s.adminMu.Lock()
	defer s.adminMu.Unlock()
	return s.adminState
}

func (s *Server) saveRetentionResult(v *retentionResultView) {
	s.adminMu.Lock()
	defer s.adminMu.Unlock()
	s.adminState.lastRetention = v
}

func (s *Server) saveVerifyResult(v *verifyResultView) {
	s.adminMu.Lock()
	defer s.adminMu.Unlock()
	s.adminState.lastVerify = v
}

func (s *Server) saveTestResult(v *testInvocationView) {
	s.adminMu.Lock()
	defer s.adminMu.Unlock()
	s.adminState.lastTest = v
}

// nowUTC is overridable for tests; keeps "Updated at" labels stable.
var nowUTC = func() time.Time { return time.Now().UTC() }

func (s *Server) renderHTML(w http.ResponseWriter, name string, data any, status int) {
	body, err := s.tmpl.render(name, data)
	if err != nil {
		s.logger.Error("template render failed", "name", name, "err", err)
		http.Error(w, "template error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// facetFieldSuggestions lists candidate field paths the Add-facet modal
// can offer as quick picks. Operators may type any path; the list is
// just guidance.
func facetFieldSuggestions() []string {
	return []string{
		"attributes.user.id",
		"attributes.region",
		"attributes.http.endpoint",
		"attributes.http.status",
		"attributes.container_name",
		"attributes.env",
	}
}
