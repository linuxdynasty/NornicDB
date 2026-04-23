package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecuteInTransaction_ReusesExistingTransactionWrapper(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "tenant")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	require.NotNil(t, exec.txContext)

	tx, ok := exec.txContext.tx.(*storage.BadgerTransaction)
	require.True(t, ok, "expected explicit transaction to use BadgerTransaction")

	txWrapper := &transactionStorageWrapper{
		tx:             tx,
		underlying:     exec.storage,
		namespace:      "tenant",
		separator:      ":",
		mutatedNodeIDs: make(map[string]struct{}),
	}
	txExec := exec.cloneWithStorage(txWrapper)
	txCtx := context.WithValue(ctx, ctxKeyTxStorage, txWrapper)

	_, err = txExec.executeInTransaction(
		txCtx,
		"CREATE (:TxReuse {name: 'ctx-wrapper'})",
		strings.ToUpper("CREATE (:TxReuse {name: 'ctx-wrapper'})"),
	)
	require.NoError(t, err)
	require.Len(t, txWrapper.snapshotMutatedNodeIDs(), 1, "existing tx wrapper should record mutations")

	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:TxReuse {name: 'ctx-wrapper'}) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, countFromResult(t, result))
}
