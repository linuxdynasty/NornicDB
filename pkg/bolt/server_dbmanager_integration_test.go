package bolt

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSessionGetExecutorForDatabase_WiresDatabaseManagerCommands(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("tenant_a"))

	s := &Session{server: &Server{dbManager: mgr}}
	exec, err := s.getExecutorForDatabase("nornic")
	require.NoError(t, err)

	res, err := exec.Execute(context.Background(), "SHOW DATABASES", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Rows)
}
