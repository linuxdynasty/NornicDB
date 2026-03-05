// Package server: per-database config override API (admin only).

package server

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/config/dbconfig"
)

// GET /admin/databases/config/keys
func (s *Server) handleDbConfigKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.General.BadRequest", "method not allowed")
		return
	}
	keys := dbconfig.AllowedKeys()
	s.writeJSON(w, http.StatusOK, keys)
}

// handleDbConfigPrefix handles GET/PUT /admin/databases/{dbName}/config.
// Route is registered as /admin/databases/ so we receive e.g. /admin/databases/nornic/config.
func (s *Server) handleDbConfigPrefix(w http.ResponseWriter, r *http.Request) {
	if s.dbConfigStore == nil {
		s.writeNeo4jError(w, http.StatusServiceUnavailable, "Neo.ClientError.General.Unavailable", "per-database config not available (system DB unavailable)")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/admin/databases/")
	if path == "" || path == "config/keys" {
		// config/keys is handled by handleDbConfigKeys
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.General.BadRequest", "not found")
		return
	}
	parts := strings.SplitN(path, "/", 2)
	dbName := parts[0]
	if dbName == "system" {
		s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.General.BadRequest", "system database cannot have config overrides")
		return
	}
	if len(parts) != 2 || parts[1] != "config" {
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.General.BadRequest", "not found")
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.handleGetDbConfig(w, r, dbName)
	case http.MethodPut:
		s.handlePutDbConfig(w, r, dbName)
	default:
		s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.General.BadRequest", "method not allowed")
	}
}

func (s *Server) handleGetDbConfig(w http.ResponseWriter, r *http.Request, dbName string) {
	overrides := s.dbConfigStore.GetOverrides(dbName)
	if overrides == nil {
		overrides = make(map[string]string)
	}
	global := nornicConfig.LoadFromEnv()
	resolved := dbconfig.Resolve(global, overrides)
	effective := make(map[string]string)
	if resolved != nil && resolved.Effective != nil {
		effective = resolved.Effective
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"overrides": overrides,
		"effective": effective,
	})
}

func (s *Server) handlePutDbConfig(w http.ResponseWriter, r *http.Request, dbName string) {
	var body struct {
		Overrides map[string]string `json:"overrides"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.General.BadRequest", "invalid JSON body")
		return
	}
	if body.Overrides == nil {
		body.Overrides = make(map[string]string)
	}
	for key := range body.Overrides {
		if !dbconfig.IsAllowedKey(key) {
			s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.General.BadRequest", "disallowed or unknown key: "+key)
			return
		}
	}
	if err := s.dbConfigStore.SetOverrides(r.Context(), dbName, body.Overrides); err != nil {
		s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.General.UnknownError", err.Error())
		return
	}
	// Reload so in-memory cache is current
	if err := s.dbConfigStore.Load(r.Context()); err != nil {
		log.Printf("⚠️  Failed to reload db config store after PUT: %v", err)
	}
	overrides := s.dbConfigStore.GetOverrides(dbName)
	if overrides == nil {
		overrides = make(map[string]string)
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{"overrides": overrides})
}
