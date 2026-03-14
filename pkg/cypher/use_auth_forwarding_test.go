package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type useAuthDBInfo struct{}

func (useAuthDBInfo) Name() string         { return "cmp" }
func (useAuthDBInfo) Type() string         { return "composite" }
func (useAuthDBInfo) Status() string       { return "online" }
func (useAuthDBInfo) IsDefault() bool      { return false }
func (useAuthDBInfo) CreatedAt() time.Time { return time.Now() }

type useAuthDBManager struct {
	lastAuthToken string
	engine        storage.Engine
}

func (m *useAuthDBManager) CreateDatabase(name string) error { return nil }
func (m *useAuthDBManager) DropDatabase(name string) error   { return nil }
func (m *useAuthDBManager) ListDatabases() []DatabaseInfoInterface {
	return []DatabaseInfoInterface{useAuthDBInfo{}}
}
func (m *useAuthDBManager) Exists(name string) bool                      { return true }
func (m *useAuthDBManager) CreateAlias(alias, databaseName string) error { return nil }
func (m *useAuthDBManager) DropAlias(alias string) error                 { return nil }
func (m *useAuthDBManager) ListAliases(databaseName string) map[string]string {
	return map[string]string{}
}
func (m *useAuthDBManager) ResolveDatabase(nameOrAlias string) (string, error) {
	return nameOrAlias, nil
}
func (m *useAuthDBManager) SetDatabaseLimits(databaseName string, limits interface{}) error {
	return nil
}
func (m *useAuthDBManager) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return nil, nil
}
func (m *useAuthDBManager) CreateCompositeDatabase(name string, constituents []interface{}) error {
	return nil
}
func (m *useAuthDBManager) DropCompositeDatabase(name string) error { return nil }
func (m *useAuthDBManager) AddConstituent(compositeName string, constituent interface{}) error {
	return nil
}
func (m *useAuthDBManager) RemoveConstituent(compositeName string, alias string) error { return nil }
func (m *useAuthDBManager) GetCompositeConstituents(compositeName string) ([]interface{}, error) {
	return []interface{}{}, nil
}
func (m *useAuthDBManager) ListCompositeDatabases() []DatabaseInfoInterface {
	return []DatabaseInfoInterface{useAuthDBInfo{}}
}
func (m *useAuthDBManager) IsCompositeDatabase(name string) bool { return name == "cmp" }
func (m *useAuthDBManager) GetStorageForUse(name string, authToken string) (interface{}, error) {
	m.lastAuthToken = authToken
	return m.engine, nil
}

func TestUseClause_ForwardsAuthTokenToDatabaseManager(t *testing.T) {
	base := storage.NewMemoryEngine()
	ns := storage.NewNamespacedEngine(base, "nornic")
	exec := NewStorageExecutor(ns)

	mgr := &useAuthDBManager{engine: storage.NewNamespacedEngine(base, "cmp")}
	exec.SetDatabaseManager(mgr)

	ctx := WithAuthToken(context.Background(), "Bearer forwarded-token")
	_, err := exec.Execute(ctx, "USE cmp RETURN 1 AS ok", nil)
	require.NoError(t, err)
	require.Equal(t, "Bearer forwarded-token", mgr.lastAuthToken)
}
