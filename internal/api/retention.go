package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

type retentionRunRequest struct {
	DryRun bool `json:"dry_run"`
}

func (s *Server) handleRetentionRun(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if s.retention == nil {
		writeError(w, http.StatusServiceUnavailable, "retention_unavailable",
			"retention evaluator not configured", nil)
		return
	}

	var req retentionRunRequest
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error(), nil)
		return
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON: "+err.Error(), nil)
			return
		}
	}

	result, err := s.retention.Run(r.Context(), req.DryRun)
	if err != nil {
		if errors.Is(err, r.Context().Err()) {
			writeError(w, 499, "client_closed", "client cancelled request", nil)
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, result)
}
