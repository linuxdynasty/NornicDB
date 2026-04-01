package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func toInt64ForTest(t *testing.T, v interface{}) int64 {
	t.Helper()
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	default:
		t.Fatalf("unexpected numeric type %T (%v)", v, v)
		return 0
	}
}

func TestDoubleUnwind_DependentRangeSupported(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)

	res, err := exec.Execute(context.Background(), "UNWIND range(1, 5) AS i UNWIND range(1, i) AS j RETURN i, j", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 15)

	require.Equal(t, int64(1), toInt64ForTest(t, res.Rows[0][0]))
	require.Equal(t, int64(1), toInt64ForTest(t, res.Rows[0][1]))

	last := res.Rows[len(res.Rows)-1]
	require.Equal(t, int64(5), toInt64ForTest(t, last[0]))
	require.Equal(t, int64(5), toInt64ForTest(t, last[1]))
}

func TestDoubleUnwind_DependentRangeSupportsComputedReturn(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)

	res, err := exec.Execute(context.Background(), "UNWIND range(1, 3) AS i UNWIND range(1, i) AS j RETURN i, j, i + j AS s", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 6)

	// Last row: i=3, j=3, s=6
	last := res.Rows[len(res.Rows)-1]
	require.Equal(t, int64(3), toInt64ForTest(t, last[0]))
	require.Equal(t, int64(3), toInt64ForTest(t, last[1]))
	require.Equal(t, int64(6), toInt64ForTest(t, last[2]))
}
