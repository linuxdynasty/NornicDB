package storage

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDB_UpdateRetryOnConflict(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	nodeID := NodeID(prefixTestID("db-update-retry"))
	_, err := engine.CreateNode(&Node{
		ID:         nodeID,
		Labels:     []string{"Counter"},
		Properties: map[string]interface{}{"value": 0},
	})
	require.NoError(t, err)

	db := NewDB(engine)
	var attempts atomic.Int32
	conflictStarted := make(chan struct{})
	conflictDone := make(chan struct{})

	go func() {
		<-conflictStarted
		conflictingTx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, conflictingTx.UpdateNode(&Node{
			ID:         nodeID,
			Labels:     []string{"Counter"},
			Properties: map[string]interface{}{"value": 100},
		}))
		require.NoError(t, conflictingTx.Commit())
		close(conflictDone)
	}()

	err = db.Update(func(tx *Transaction) error {
		attempt := attempts.Add(1)
		node, err := tx.GetNode(nodeID)
		require.NoError(t, err)

		updated := copyNode(node)
		updated.Properties["value"] = int(attempt)
		require.NoError(t, tx.UpdateNode(updated))

		if attempt == 1 {
			close(conflictStarted)
			<-conflictDone
		}

		return nil
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, attempts.Load())

	stored, err := engine.GetNode(nodeID)
	require.NoError(t, err)
	require.EqualValues(t, 2, stored.Properties["value"])
}

func TestDBView_RollsBackReadOnlyTransaction(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	nodeID := NodeID(prefixTestID("db-view-node"))
	_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"View"}})
	require.NoError(t, err)

	db := NewDB(engine)
	var readTS MVCCVersion

	err = db.View(func(tx *Transaction) error {
		readTS = tx.readTS
		_, err := tx.GetNode(nodeID)
		return err
	})
	require.NoError(t, err)
	require.False(t, readTS.IsZero())
}

func TestDB_UpdateConcurrentIncrements(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	nodeID := NodeID(prefixTestID("db-update-concurrent-counter"))
	_, err := engine.CreateNode(&Node{
		ID:         nodeID,
		Labels:     []string{"Counter"},
		Properties: map[string]interface{}{"value": 0},
	})
	require.NoError(t, err)

	db := NewDB(engine)
	const workers = 10
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := db.Update(func(tx *Transaction) error {
				node, err := tx.GetNode(nodeID)
				if err != nil {
					return err
				}
				updated := copyNode(node)
				currentValue, err := asInt(updated.Properties["value"])
				if err != nil {
					return err
				}
				updated.Properties["value"] = currentValue + 1
				return tx.UpdateNode(updated)
			})
			if err != nil {
				errCh <- err
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	stored, err := engine.GetNode(nodeID)
	require.NoError(t, err)
	require.EqualValues(t, workers, stored.Properties["value"])
}

func asInt(value interface{}) (int, error) {
	switch typed := value.(type) {
	case int:
		return typed, nil
	case int8:
		return int(typed), nil
	case int16:
		return int(typed), nil
	case int32:
		return int(typed), nil
	case int64:
		return int(typed), nil
	case uint:
		return int(typed), nil
	case uint8:
		return int(typed), nil
	case uint16:
		return int(typed), nil
	case uint32:
		return int(typed), nil
	case uint64:
		return int(typed), nil
	case float32:
		return int(typed), nil
	case float64:
		return int(typed), nil
	default:
		return 0, ErrInvalidData
	}
}
