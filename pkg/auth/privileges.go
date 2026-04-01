// Package auth: per-database read/write privileges (Phase 4 RBAC).
//
// Privileges are stored in the system database as _DbPrivilege nodes.
// ResolvedAccess is resolved from the matrix; when no entry exists, fall back to global RolePermissions.
// See docs/plans/per-database-rbac-neo4j-style.md §4.4.

package auth

import (
	"context"
	"strings"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	dbPrivLabel   = "_DbPrivilege"
	dbPrivSystems = "_System"
	dbPrivPrefix  = "db_priv:"
)

// DbPrivilege holds read/write for one (role, database).
type DbPrivilege struct {
	Read  bool
	Write bool
}

// PrivilegesStore persists and resolves per (role, database) read/write.
type PrivilegesStore struct {
	storage storage.Engine
	mu      sync.RWMutex
	// role -> dbName -> { Read, Write }
	matrix map[string]map[string]DbPrivilege
}

// NewPrivilegesStore creates a store that reads/writes _DbPrivilege nodes.
func NewPrivilegesStore(systemStorage storage.Engine) *PrivilegesStore {
	return &PrivilegesStore{storage: systemStorage, matrix: make(map[string]map[string]DbPrivilege)}
}

// Load reads all _DbPrivilege nodes from storage into memory.
func (p *PrivilegesStore) Load(ctx context.Context) error {
	m := make(map[string]map[string]DbPrivilege)
	err := storage.StreamNodesWithFallback(ctx, p.storage, 1000, func(n *storage.Node) error {
		for _, l := range n.Labels {
			if l == dbPrivLabel {
				role, dbName := roleDbFromNodeID(string(n.ID))
				if role == "" || dbName == "" {
					return nil
				}
				priv := privFromProperties(n.Properties)
				if m[role] == nil {
					m[role] = make(map[string]DbPrivilege)
				}
				m[role][dbName] = priv
				break
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.matrix = m
	p.mu.Unlock()
	return nil
}

// Resolve returns ResolvedAccess for (principalRoles, dbName).
// If any role has an entry for this db, aggregate read/write. If none have an entry, fall back to global RolePermissions.
func (p *PrivilegesStore) Resolve(principalRoles []string, dbName string) ResolvedAccess {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var read, write bool
	matchedMatrixEntry := false
	for _, role := range principalRoles {
		role = strings.ToLower(strings.TrimSpace(role))
		role = strings.TrimPrefix(role, "role_")
		if perDb, ok := p.matrix[role]; ok {
			if priv, ok := perDb[dbName]; ok {
				matchedMatrixEntry = true
				read = read || priv.Read
				write = write || priv.Write
			}
		}
	}
	if matchedMatrixEntry {
		return ResolvedAccess{Read: read, Write: write}
	}
	// Fall back to global role permissions
	for _, role := range principalRoles {
		role = strings.ToLower(strings.TrimSpace(role))
		role = strings.TrimPrefix(role, "role_")
		perms, ok := RolePermissions[Role(role)]
		if !ok {
			continue
		}
		for _, perm := range perms {
			if perm == PermRead {
				read = true
			}
			if perm == PermWrite || perm == PermAdmin {
				write = true
			}
		}
	}
	return ResolvedAccess{Read: read, Write: write}
}

// Matrix returns a copy of the full matrix for GET API.
func (p *PrivilegesStore) Matrix() []struct {
	Role     string `json:"role"`
	Database string `json:"database"`
	Read     bool   `json:"read"`
	Write    bool   `json:"write"`
} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var out []struct {
		Role     string `json:"role"`
		Database string `json:"database"`
		Read     bool   `json:"read"`
		Write    bool   `json:"write"`
	}
	for role, perDb := range p.matrix {
		for dbName, priv := range perDb {
			out = append(out, struct {
				Role     string `json:"role"`
				Database string `json:"database"`
				Read     bool   `json:"read"`
				Write    bool   `json:"write"`
			}{Role: role, Database: dbName, Read: priv.Read, Write: priv.Write})
		}
	}
	return out
}

// SavePrivilege persists one (role, database, read, write) and refreshes in-memory.
func (p *PrivilegesStore) SavePrivilege(ctx context.Context, role, dbName string, read, write bool) error {
	role = strings.ToLower(strings.TrimSpace(role))
	nodeID := storage.NodeID(dbPrivPrefix + role + ":" + dbName)
	node := &storage.Node{
		ID:     nodeID,
		Labels: []string{dbPrivLabel, dbPrivSystems},
		Properties: map[string]any{
			"role":     role,
			"database": dbName,
			"read":     read,
			"write":    write,
		},
	}
	existing, err := p.storage.GetNode(nodeID)
	if err == storage.ErrNotFound {
		_, err = p.storage.CreateNode(node)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		node.CreatedAt = existing.CreatedAt
		err = p.storage.UpdateNode(node)
		if err != nil {
			return err
		}
	}
	p.mu.Lock()
	if p.matrix[role] == nil {
		p.matrix[role] = make(map[string]DbPrivilege)
	}
	p.matrix[role][dbName] = DbPrivilege{Read: read, Write: write}
	p.mu.Unlock()
	return nil
}

// PutMatrix replaces the stored matrix with the given list (for PUT /auth/access/privileges).
// Deletes all existing _DbPrivilege nodes then creates nodes for each entry.
func (p *PrivilegesStore) PutMatrix(ctx context.Context, entries []struct {
	Role     string `json:"role"`
	Database string `json:"database"`
	Read     bool   `json:"read"`
	Write    bool   `json:"write"`
}) error {
	var toDelete []storage.NodeID
	_ = storage.StreamNodesWithFallback(ctx, p.storage, 1000, func(n *storage.Node) error {
		for _, l := range n.Labels {
			if l == dbPrivLabel {
				toDelete = append(toDelete, n.ID)
				break
			}
		}
		return nil
	})
	for _, id := range toDelete {
		_ = p.storage.DeleteNode(id)
	}
	p.mu.Lock()
	p.matrix = make(map[string]map[string]DbPrivilege)
	p.mu.Unlock()
	newMatrix := make(map[string]map[string]DbPrivilege)
	for _, e := range entries {
		role := strings.ToLower(strings.TrimSpace(e.Role))
		if newMatrix[role] == nil {
			newMatrix[role] = make(map[string]DbPrivilege)
		}
		newMatrix[role][e.Database] = DbPrivilege{Read: e.Read, Write: e.Write}
		if err := p.SavePrivilege(ctx, e.Role, e.Database, e.Read, e.Write); err != nil {
			return err
		}
	}
	p.mu.Lock()
	p.matrix = newMatrix
	p.mu.Unlock()
	return nil
}

func roleDbFromNodeID(id string) (role, dbName string) {
	if !strings.HasPrefix(id, dbPrivPrefix) {
		return "", ""
	}
	rest := id[len(dbPrivPrefix):]
	idx := strings.Index(rest, ":")
	if idx < 0 {
		return "", ""
	}
	return rest[:idx], rest[idx+1:]
}

func privFromProperties(prop map[string]any) DbPrivilege {
	var read, write bool
	if v, ok := prop["read"].(bool); ok {
		read = v
	}
	if v, ok := prop["write"].(bool); ok {
		write = v
	}
	// Support JSON-unmarshalled bool from string storage
	if v, ok := prop["read"].(float64); ok {
		read = v != 0
	}
	if v, ok := prop["write"].(float64); ok {
		write = v != 0
	}
	return DbPrivilege{Read: read, Write: write}
}
