// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

var edgeBetweenIndexReadyKey = []byte{prefixMVCCMeta, 0x02}

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
	var edges []*Edge
	if err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerIterOptsKeyOnly([]byte{prefixEdge}))
		defer it.Close()

		for it.Rewind(); it.ValidForPrefix([]byte{prefixEdge}); it.Next() {
			item := it.Item()
			if err := item.Value(func(val []byte) error {
				edge, err := decodeEdge(val)
				if err != nil {
					return fmt.Errorf("decode edge for edge-between index: %w", err)
				}
				edges = append(edges, edge)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	return b.withUpdate(func(txn *badger.Txn) error {
		for _, edge := range edges {
			if err := txn.Set(edgeBetweenIndexKey(edge.StartNode, edge.EndNode, edge.Type, edge.ID), []byte{}); err != nil {
				return err
			}
		}
		return txn.Set(edgeBetweenIndexReadyKey, []byte{1})
	})
}
