package ui

import (
	"net/http"
)

const uiCountLimitGuard = 10001

func (s *Server) handleCount(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.renderError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	in := readQueryForm(r)
	req, err := s.buildQueryRequest(in, 1)
	if err != nil {
		// Empty count slot when the input is incomplete: avoid showing
		// "error" while the user is mid-typing.
		s.renderHTML(w, "fragments/count_indicator.html", countView{}, http.StatusOK)
		return
	}
	count, exact, _, durationMs, err := s.executor.Count(r.Context(), req, uiCountLimitGuard)
	if err != nil {
		s.renderHTML(w, "fragments/count_indicator.html", countView{}, http.StatusOK)
		return
	}
	out := countView{Count: count, DurationMs: durationMs}
	if !exact {
		out.Count = uiCountLimitGuard - 1
		out.LowerBound = true
	}
	s.renderHTML(w, "fragments/count_indicator.html", out, http.StatusOK)
}
