package ui

import (
	"net/http"
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
	body := adminPage{
		Archive: s.archiveInfo,
		Retention: retentionView{
			MaxAge:              s.retentionInfo.MaxAge,
			MaxSize:             s.retentionInfo.MaxSize,
			ArchiveBeforeDelete: s.retentionInfo.ArchiveBeforeDelete,
		},
	}
	if s.facets != nil {
		body.Facets = s.facets.List()
	}
	data := pageData{
		Title:   "Admin",
		View:    "admin",
		Archive: s.archiveInfo,
		Page:    body,
	}
	s.renderHTML(w, "admin.html", data, http.StatusOK)
}

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
