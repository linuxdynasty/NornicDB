package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestExecuteInTransaction_ReusesExistingTransactionWrapper proves that
// recursive execution paths reuse the transaction wrapper already stored in
// context instead of re-wrapping the same Badger transaction and dropping the
// active namespace metadata.
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

func TestExecuteInTransaction_IgnoresStaleTransactionWrapper(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "tenant")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	require.NotNil(t, exec.txContext)

	currentTx, ok := exec.txContext.tx.(*storage.BadgerTransaction)
	require.True(t, ok, "expected explicit transaction to use BadgerTransaction")

	otherExec := NewStorageExecutor(storage.NewNamespacedEngine(baseStore, "tenant"))
	_, err = otherExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	require.NotNil(t, otherExec.txContext)
	t.Cleanup(func() {
		if otherExec.txContext != nil && otherExec.txContext.active {
			_, _ = otherExec.Execute(ctx, "ROLLBACK", nil)
		}
	})

	staleTx, ok := otherExec.txContext.tx.(*storage.BadgerTransaction)
	require.True(t, ok, "expected stale transaction to use BadgerTransaction")
	require.NotEqual(t, staleTx, currentTx, "test requires distinct explicit transactions")

	staleWrapper := &transactionStorageWrapper{
		tx:             staleTx,
		underlying:     otherExec.storage,
		namespace:      "tenant",
		separator:      ":",
		mutatedNodeIDs: make(map[string]struct{}),
	}
	txCtx := context.WithValue(ctx, ctxKeyTxStorage, staleWrapper)

	_, err = exec.executeInTransaction(
		txCtx,
		"CREATE (:TxReuse {name: 'fresh-wrapper'})",
		strings.ToUpper("CREATE (:TxReuse {name: 'fresh-wrapper'})"),
	)
	require.NoError(t, err)
	require.Nil(t, staleWrapper.snapshotMutatedNodeIDs(), "stale wrapper should not be reused")

	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:TxReuse {name: 'fresh-wrapper'}) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, countFromResult(t, result))
}
