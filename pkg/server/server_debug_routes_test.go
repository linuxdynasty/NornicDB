package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRegisterDebugRoutesRequiresPprofConfig(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	srv := &Server{config: &Config{EnablePprof: false}}

	srv.registerDebugRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /debug/pprof/ status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestRegisterDebugRoutesServesPprofWhenEnabled(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	srv := &Server{config: &Config{EnablePprof: true}}

	srv.registerDebugRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /debug/pprof/ status = %d, want %d", rec.Code, http.StatusOK)
	}
}
