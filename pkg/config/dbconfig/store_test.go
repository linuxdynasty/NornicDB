package dbconfig

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_Load_Get_Set(t *testing.T) {
	ctx := context.Background()
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	store := NewStore(eng)

	err := store.Load(ctx)
	require.NoError(t, err)
	assert.Nil(t, store.GetOverrides("mydb"))

	// Set overrides
	err = store.SetOverrides(ctx, "mydb", map[string]string{
		"NORNICDB_EMBEDDING_MODEL":       "bge-m3",
		"NORNICDB_SEARCH_MIN_SIMILARITY": "0.7",
	})
	require.NoError(t, err)
	o := store.GetOverrides("mydb")
	require.NotNil(t, o)
	assert.Equal(t, "bge-m3", o["NORNICDB_EMBEDDING_MODEL"])
	assert.Equal(t, "0.7", o["NORNICDB_SEARCH_MIN_SIMILARITY"])

	// Reload from storage (simulate restart)
	store2 := NewStore(eng)
	err = store2.Load(ctx)
	require.NoError(t, err)
	o2 := store2.GetOverrides("mydb")
	require.NotNil(t, o2)
	assert.Equal(t, "bge-m3", o2["NORNICDB_EMBEDDING_MODEL"])
}

func TestStore_SetOverrides_EmptyClears(t *testing.T) {
	ctx := context.Background()
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	store := NewStore(eng)

	err := store.SetOverrides(ctx, "mydb", map[string]string{"K": "v"})
	require.NoError(t, err)
	assert.NotNil(t, store.GetOverrides("mydb"))

	err = store.SetOverrides(ctx, "mydb", nil)
	require.NoError(t, err)
	assert.Nil(t, store.GetOverrides("mydb"))

	err = store.SetOverrides(ctx, "mydb", map[string]string{})
	require.NoError(t, err)
	assert.Nil(t, store.GetOverrides("mydb"))
}

func TestStore_EmptyDbNameIgnored(t *testing.T) {
	ctx := context.Background()
	eng := storage.NewMemoryEngine()
	defer eng.Close()
	store := NewStore(eng)
	err := store.SetOverrides(ctx, "", map[string]string{"K": "v"})
	require.NoError(t, err)
	assert.Nil(t, store.GetOverrides(""))
}
