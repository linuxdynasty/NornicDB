package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegisterDebugRoutesRequiresExplicitPprofEnablement(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	srv := &Server{config: &Config{EnablePprof: false}}
	srv.registerDebugRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusNotFound, resp.Code)
}

func TestRegisterDebugRoutesExposesPprofWhenEnabled(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	srv := &Server{config: &Config{EnablePprof: true}}
	srv.registerDebugRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
	require.Contains(t, resp.Body.String(), "Types of profiles available")
}
