// Package server provides an HTTP REST API server for NornicDB.
package server

import (
	"fmt"
	"io/fs"
	"net/http"
	"strings"
)

// UIAssets holds the UI files (set by main package or tests).
var UIAssets fs.FS

// UIEnabled indicates if UI assets are available
var UIEnabled bool

// SetUIAssets configures the UI assets.
func SetUIAssets(assets fs.FS) {
	UIAssets = assets
	UIEnabled = true
}

// uiHandler serves the embedded SPA UI
type uiHandler struct {
	fileServer http.Handler
	indexHTML  []byte
}

func normalizeUIBasePath(raw string) string {
	base := strings.TrimSpace(raw)
	if base == "" || base == "/" {
		return ""
	}
	if !strings.HasPrefix(base, "/") {
		base = "/" + base
	}
	base = strings.TrimSuffix(base, "/")
	if base == "/" {
		return ""
	}
	return base
}

func rewriteIndexHTMLBasePath(indexHTML []byte, basePath string) []byte {
	base := normalizeUIBasePath(basePath)
	if base == "" {
		return indexHTML
	}
	s := string(indexHTML)
	repl := []struct {
		from string
		to   string
	}{
		{`src="/assets/`, `src="` + base + `/assets/`},
		{`href="/assets/`, `href="` + base + `/assets/`},
		{`href="/nornicdb.svg"`, `href="` + base + `/nornicdb.svg"`},
		{`href="/favicon.ico"`, `href="` + base + `/favicon.ico"`},
	}
	for _, r := range repl {
		s = strings.ReplaceAll(s, r.from, r.to)
	}
	return []byte(s)
}

// newUIHandler creates a handler for serving embedded UI assets
func newUIHandler() (*uiHandler, error) {
	if !UIEnabled {
		return nil, nil
	}
	if UIAssets == nil {
		return nil, fmt.Errorf("UI assets not configured")
	}

	// List the embedded files to debug
	entries, err := fs.ReadDir(UIAssets, ".")
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded root: %w", err)
	}

	// Find the correct path (might be just "dist" or "ui/dist")
	var distPath string
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() == "ui" {
			distPath = "ui/dist"
			break
		} else if entry.IsDir() && entry.Name() == "dist" {
			distPath = "dist"
			break
		}
	}

	if distPath == "" {
		return nil, fmt.Errorf("UI dist directory not found in embedded assets")
	}

	// Get the dist subdirectory from embedded files
	distFS, err := fs.Sub(UIAssets, distPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get dist subdirectory: %w", err)
	}

	// Read index.html for SPA fallback
	indexHTML, err := fs.ReadFile(distFS, "index.html")
	if err != nil {
		return nil, fmt.Errorf("failed to read index.html: %w", err)
	}

	return &uiHandler{
		fileServer: http.FileServer(http.FS(distFS)),
		indexHTML:  indexHTML,
	}, nil
}

// ServeHTTP implements http.Handler for the UI
func (h *uiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// Serve static assets directly
	if strings.HasPrefix(path, "/assets/") ||
		strings.HasSuffix(path, ".js") ||
		strings.HasSuffix(path, ".css") ||
		strings.HasSuffix(path, ".svg") ||
		strings.HasSuffix(path, ".png") ||
		strings.HasSuffix(path, ".ico") ||
		strings.HasSuffix(path, ".woff") ||
		strings.HasSuffix(path, ".woff2") {
		h.fileServer.ServeHTTP(w, r)
		return
	}

	// For all other paths, serve index.html (SPA routing)
	basePath := r.Header.Get("X-Base-Path")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(rewriteIndexHTMLBasePath(h.indexHTML, basePath))
}

// isUIRequest checks if request is from a browser wanting HTML
func isUIRequest(r *http.Request) bool {
	accept := r.Header.Get("Accept")
	// Browser requests typically accept text/html
	return strings.Contains(accept, "text/html")
}
