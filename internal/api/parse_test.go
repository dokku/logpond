package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func postParseQuery(t *testing.T, srv *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/parse-query", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	srv.Handler().ServeHTTP(rr, req)
	return rr
}

func TestParseQuery_HappyPath_ReturnsFilterTree(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	rr := postParseQuery(t, srv, `{"q":"service:api level:error"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body)
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	filter, ok := resp["filter"].(map[string]any)
	if !ok {
		t.Fatalf("filter is not an object: %v", resp["filter"])
	}
	if filter["op"] != "and" {
		t.Errorf("op = %v, want and", filter["op"])
	}
	children, _ := filter["children"].([]any)
	if len(children) != 2 {
		t.Errorf("children len = %d, want 2", len(children))
	}
}

func TestParseQuery_EmptyQ_ReturnsNullFilter(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	rr := postParseQuery(t, srv, `{"q":""}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	var resp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["filter"] != nil {
		t.Errorf("filter = %v, want nil", resp["filter"])
	}
	if resp["search"] != nil {
		t.Errorf("search = %v, want nil", resp["search"])
	}
	if _, ok := resp["warnings"].([]any); !ok {
		t.Errorf("warnings missing or wrong type: %v", resp["warnings"])
	}
}

func TestParseQuery_FreeTextOnly_PopulatesSearch(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	rr := postParseQuery(t, srv, `{"q":"\"connection refused\""}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body)
	}
	var resp map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp["filter"] != nil {
		t.Errorf("filter = %v, want nil", resp["filter"])
	}
	if s, _ := resp["search"].(string); s != "connection refused" {
		t.Errorf("search = %q, want connection refused", s)
	}
}

func TestParseQuery_MalformedInput_400WithColumn(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	rr := postParseQuery(t, srv, `{"q":"service:"}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	var env errorEnvelope
	_ = json.Unmarshal(rr.Body.Bytes(), &env)
	if env.Error.Code != "invalid_query_syntax" {
		t.Errorf("code = %q, want invalid_query_syntax", env.Error.Code)
	}
	col, ok := env.Error.Details["column"]
	if !ok {
		t.Fatalf("details.column missing: %+v", env.Error.Details)
	}
	// column comes back as a JSON number — ok if non-zero.
	switch v := col.(type) {
	case float64:
		if v <= 0 {
			t.Errorf("column = %v, want > 0", v)
		}
	default:
		t.Errorf("column type = %T, want number", col)
	}
}

func TestParseQuery_BadJSON_Returns400(t *testing.T) {
	srv, _ := newTestServer(t, 100)
	rr := postParseQuery(t, srv, `not json`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}
