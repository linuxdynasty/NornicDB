package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSanitizeUIBasePath(t *testing.T) {
	t.Run("allows normal prefixed path", func(t *testing.T) {
		require.Equal(t, "/nornic-db", sanitizeUIBasePath("/nornic-db/"))
		require.Equal(t, "/nornic-db", sanitizeUIBasePath("nornic-db"))
	})

	t.Run("rejects malicious header payload", func(t *testing.T) {
		require.Equal(t, "", sanitizeUIBasePath(`/" onload="alert(1)`))
		require.Equal(t, "", sanitizeUIBasePath(`/x"><script>alert(1)</script>`))
		require.Equal(t, "", sanitizeUIBasePath(`/../admin`))
		require.Equal(t, "", sanitizeUIBasePath(`/foo//bar`))
		require.Equal(t, "", sanitizeUIBasePath(`/foo\bar`))
	})
}

func TestUIHandler_ServeHTTP_DoesNotReflectMaliciousBasePathHeader(t *testing.T) {
	h := &uiHandler{
		fileServer: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		indexHTML: []byte(`<html><head><link rel="stylesheet" href="/assets/app.css"></head><body><script src="/assets/app.js"></script></body></html>`),
	}

	req := httptest.NewRequest(http.MethodGet, "/app/route", nil)
	req.Header.Set("X-Base-Path", `/" onload="alert(1)`)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.NotContains(t, body, `onload="alert(1)`)
	require.Contains(t, body, `href="/assets/app.css"`)
	require.Contains(t, body, `src="/assets/app.js"`)
}
