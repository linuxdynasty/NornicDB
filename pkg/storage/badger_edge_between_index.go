// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

var edgeBetweenIndexReadyKey = []byte{prefixMVCCMeta, 0x02}

const edgeBetweenIndexRebuildBatchSize = 50_000

// ensureEdgeBetweenIndex backfills the direct relationship lookup index once.
//
// Older Badger stores predate the edge-between index. Rebuilding on first open
// preserves correctness for existing databases while keeping future startup
// costs bounded by a small metadata marker.
func (b *BadgerEngine) ensureEdgeBetweenIndex() error {
	var ready bool
	if err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(edgeBetweenIndexReadyKey)
		if err == nil {
			ready = true
			return nil
		}
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	}); err != nil {
		return err
	}
	if ready {
		return nil
	}

	return b.rebuildEdgeBetweenIndex()
}

// rebuildEdgeBetweenIndex scans stored edges and writes direct lookup entries.
func (b *BadgerEngine) rebuildEdgeBetweenIndex() error {
	if err := b.db.DropPrefix([]byte{prefixEdgeBetweenIndex}); err != nil {
		return fmt.Errorf("clear edge-between set index before rebuild: %w", err)
	}
	if err := b.db.DropPrefix([]byte{prefixEdgeBetweenHead}); err != nil {
		return fmt.Errorf("clear edge-between head index before rebuild: %w", err)
	}

	batch := b.db.NewWriteBatch()
	defer batch.Cancel()
	batchCount := 0

	flushBatch := func() error {
		if batchCount == 0 {
			return nil
		}
		if err := batch.Flush(); err != nil {
			return err
		}
		batch.Cancel()
		batch = b.db.NewWriteBatch()
		batchCount = 0
		return nil
	}

	if err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerIterOptsPrefetchValues([]byte{prefixEdge}, 100))
		defer it.Close()

		for it.Rewind(); it.ValidForPrefix([]byte{prefixEdge}); it.Next() {
			item := it.Item()
			if err := item.Value(func(val []byte) error {
				edge, err := decodeEdge(val)
				if err != nil {
					return fmt.Errorf("decode edge for edge-between index: %w", err)
				}
				if err := batch.Set(edgeBetweenIndexKey(edge.StartNode, edge.EndNode, edge.Type, edge.ID), []byte{}); err != nil {
					return err
				}
				if err := batch.Set(edgeBetweenHeadKey(edge.StartNode, edge.EndNode, edge.Type), []byte(edge.ID)); err != nil {
					return err
				}
				batchCount++
				if batchCount >= edgeBetweenIndexRebuildBatchSize {
					return flushBatch()
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	if err := flushBatch(); err != nil {
		return err
	}

	return b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(edgeBetweenIndexReadyKey, []byte{1})
	})
}
