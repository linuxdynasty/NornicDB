// Package storage - BadgerDB transaction wrapper with ACID guarantees.
//
// This file implements atomic transactions for BadgerDB with full constraint
// validation and rollback support.
package storage

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

// BadgerTransaction wraps Badger's native transaction with constraint validation.
//
// Provides ACID guarantees:
//   - Atomicity: All operations commit together or none do
//   - Consistency: Constraints are validated before commit
//   - Isolation: Changes invisible until commit
//   - Durability: Badger's WAL ensures persistence
type BadgerTransaction struct {
	mu sync.Mutex

	// Transaction identity
	ID        string
	StartTime time.Time
	Status    TransactionStatus
	readTS    MVCCVersion
	// CommitVersion is assigned once for a successful commit that mutates storage.
	CommitVersion MVCCVersion

	// Badger's native transaction
	badgerTx *badger.Txn

	// Parent engine for constraint validation
	engine *BadgerEngine

	// Track operations for constraint validation
	pendingNodes map[NodeID]*Node
	pendingEdges map[EdgeID]*Edge
	deletedNodes map[NodeID]struct{}
	deletedEdges map[EdgeID]struct{}
	operations   []Operation

	// Buffered writes - collected during transaction, flushed at commit
	// This batches all writes together for better performance while maintaining ACID guarantees
	pendingWrites  map[string][]byte // key -> value for Set operations
	pendingDeletes map[string]bool   // key -> true for Delete operations
	// When true, skip per-operation constraint checks and validate at commit only.
	deferConstraintValidation bool
	// When true, skip read-before-write existence checks for CREATE operations.
	skipCreateExistenceCheck bool

	// Transaction metadata (for logging/debugging)
	Metadata map[string]interface{}
}

func (b *BadgerEngine) currentMVCCReadVersion() MVCCVersion {
	return MVCCVersion{
		CommitTimestamp: time.Now().UTC(),
		CommitSequence:  b.mvccSeq.Load(),
	}
}

// BeginTransaction starts a new Badger transaction with ACID guarantees.
func (b *BadgerEngine) BeginTransaction() (*BadgerTransaction, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, fmt.Errorf("engine is closed")
	}

	readTS := b.currentMVCCReadVersion()

	return &BadgerTransaction{
		ID:             generateTxID(),
		StartTime:      time.Now(),
		Status:         TxStatusActive,
		readTS:         readTS,
		badgerTx:       b.db.NewTransaction(true), // Read-write transaction
		engine:         b,
		pendingNodes:   make(map[NodeID]*Node),
		pendingEdges:   make(map[EdgeID]*Edge),
		deletedNodes:   make(map[NodeID]struct{}),
		deletedEdges:   make(map[EdgeID]struct{}),
		operations:     make([]Operation, 0),
		pendingWrites:  make(map[string][]byte),
		pendingDeletes: make(map[string]bool),
		Metadata:       make(map[string]interface{}),
	}, nil
}

// IsActive returns true if the transaction is still active.
func (tx *BadgerTransaction) IsActive() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.Status == TxStatusActive
}

// SetDeferredConstraintValidation controls per-operation constraint checks.
// When enabled, constraints are enforced at commit time only.
func (tx *BadgerTransaction) SetDeferredConstraintValidation(deferValidation bool) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	tx.deferConstraintValidation = deferValidation
	return nil
}

// SetSkipCreateExistenceCheck controls read-before-write checks for CREATE.
// When enabled, CREATE skips the storage existence read for UUID IDs.
func (tx *BadgerTransaction) SetSkipCreateExistenceCheck(skip bool) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	tx.skipCreateExistenceCheck = skip
	return nil
}

// bufferSet buffers a write operation to be applied at commit time.
// If the key was previously marked for deletion, it's removed from deletes.
func (tx *BadgerTransaction) bufferSet(key []byte, value []byte) {
	keyStr := string(key)
	// Remove from deletes if it was marked for deletion
	delete(tx.pendingDeletes, keyStr)
	// Buffer the write (copy value to avoid aliasing)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	tx.pendingWrites[keyStr] = valueCopy
}

// bufferDelete buffers a delete operation to be applied at commit time.
// If the key was previously buffered for write, it's removed from writes.
func (tx *BadgerTransaction) bufferDelete(key []byte) {
	keyStr := string(key)
	// Remove from writes if it was buffered
	delete(tx.pendingWrites, keyStr)
	// Mark for deletion
	tx.pendingDeletes[keyStr] = true
}

// flushBufferedWrites applies all buffered writes and deletes to the Badger transaction.
// This is called at commit time to batch all writes together.
func (tx *BadgerTransaction) flushBufferedWrites() error {
	// Apply deletes first (in case a key is both written and deleted, delete wins)
	for keyStr := range tx.pendingDeletes {
		key := []byte(keyStr)
		if err := tx.badgerTx.Delete(key); err != nil {
			return fmt.Errorf("flushing delete for key %s: %w", keyStr, err)
		}
	}

	// Apply writes (only keys that weren't deleted)
	for keyStr, value := range tx.pendingWrites {
		// Skip if this key was also deleted (delete wins)
		if tx.pendingDeletes[keyStr] {
			continue
		}
		key := []byte(keyStr)
		if err := tx.badgerTx.Set(key, value); err != nil {
			return fmt.Errorf("flushing write for key %s: %w", keyStr, err)
		}
	}

	// Clear buffers after successful flush
	tx.pendingWrites = make(map[string][]byte)
	tx.pendingDeletes = make(map[string]bool)

	return nil
}

// CreateNode adds a node to the transaction with constraint validation.
// REQUIRES: node.ID must be prefixed with namespace (e.g., "nornic:node-123").
// This enforces that all nodes are namespaced at the storage layer.
func (tx *BadgerTransaction) CreateNode(node *Node) (NodeID, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return "", ErrTransactionClosed
	}

	// Enforce namespace prefix at storage layer - all node IDs must be prefixed
	if node != nil && node.ID != "" && !strings.Contains(string(node.ID), ":") {
		return "", fmt.Errorf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got unprefixed ID: %s", node.ID)
	}

	// Validate constraints BEFORE writing
	if !tx.deferConstraintValidation {
		if err := tx.validateNodeConstraints(node); err != nil {
			return "", err
		}
	}

	// Check for duplicates in pending
	if _, exists := tx.pendingNodes[node.ID]; exists {
		return "", ErrAlreadyExists
	}

	// Check if exists in storage (read from Badger)
	skipExistenceCheck := tx.skipCreateExistenceCheck && shouldSkipCreateExistenceCheck(node.ID)
	if !skipExistenceCheck {
		if _, deleted := tx.deletedNodes[node.ID]; !deleted {
			_, err := tx.getCommittedNodeLocked(node.ID)
			if err == nil {
				return "", ErrAlreadyExists
			}
			if err != ErrNotFound {
				return "", fmt.Errorf("checking node existence: %w", err)
			}
		}
	}

	// PERFORMANCE OPTIMIZATION: Buffer all writes and flush at commit time
	// This batches all writes together for better performance while maintaining ACID guarantees

	// Serialize node (may store embeddings separately if too large)
	data, embeddingsSeparate, err := encodeNode(node)
	if err != nil {
		return "", fmt.Errorf("serializing node: %w", err)
	}

	key := nodeKey(node.ID)
	// Buffer node write
	tx.bufferSet(key, data)

	// If embeddings are stored separately, buffer them
	if embeddingsSeparate {
		for i, emb := range node.ChunkEmbeddings {
			embKey := embeddingKey(node.ID, i)
			embData, err := encodeEmbedding(emb)
			if err != nil {
				return "", fmt.Errorf("failed to encode embedding chunk %d: %w", i, err)
			}
			tx.bufferSet(embKey, embData)
		}
	}

	// Buffer all label index writes
	for _, label := range node.Labels {
		indexKey := labelIndexKey(label, node.ID)
		tx.bufferSet(indexKey, []byte{})
	}

	// Add to pending embeddings index if needed
	if !isSystemNamespaceID(string(node.ID)) &&
		(len(node.ChunkEmbeddings) == 0 || len(node.ChunkEmbeddings[0]) == 0) &&
		NodeNeedsEmbedding(node) {
		tx.bufferSet(pendingEmbedKey(node.ID), []byte{})
	}

	// Track for read-your-writes and constraint validation
	nodeCopy := copyNode(node)
	tx.pendingNodes[node.ID] = nodeCopy
	delete(tx.deletedNodes, node.ID)

	tx.operations = append(tx.operations, Operation{
		Type:      OpCreateNode,
		Timestamp: time.Now(),
		NodeID:    node.ID,
		Node:      nodeCopy,
	})

	return node.ID, nil
}

// UpdateNode updates a node in the transaction.
func (tx *BadgerTransaction) UpdateNode(node *Node) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	// Validate constraints
	if !tx.deferConstraintValidation {
		if err := tx.validateNodeConstraints(node); err != nil {
			return err
		}
	}

	// Check if node exists
	var oldNode *Node
	if pending, exists := tx.pendingNodes[node.ID]; exists {
		oldNode = copyNode(pending)
	} else {
		var err error
		oldNode, err = tx.getCommittedNodeLocked(node.ID)
		if err == ErrNotFound {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("reading node: %w", err)
		}
	}

	// Buffer updated node write
	nodeBytes, err := serializeNode(node)
	if err != nil {
		return fmt.Errorf("serializing node: %w", err)
	}

	key := nodeKey(node.ID)
	tx.bufferSet(key, nodeBytes)

	// Update label indexes if changed
	oldLabelSet := make(map[string]bool)
	for _, label := range oldNode.Labels {
		oldLabelSet[label] = true
	}

	newLabelSet := make(map[string]bool)
	for _, label := range node.Labels {
		newLabelSet[label] = true
		if !oldLabelSet[label] {
			// New label - buffer index write
			indexKey := labelIndexKey(label, node.ID)
			tx.bufferSet(indexKey, []byte{})
		}
	}

	// Remove old labels
	for _, label := range oldNode.Labels {
		if !newLabelSet[label] {
			indexKey := labelIndexKey(label, node.ID)
			tx.bufferDelete(indexKey)
		}
	}

	// Track for read-your-writes
	nodeCopy := copyNode(node)
	tx.pendingNodes[node.ID] = nodeCopy

	tx.operations = append(tx.operations, Operation{
		Type:      OpUpdateNode,
		Timestamp: time.Now(),
		NodeID:    node.ID,
		Node:      nodeCopy,
		OldNode:   oldNode,
	})

	return nil
}

// deleteNodeBuffered deletes a node and all its edges/embeddings, buffering all writes.
// This is the buffering version of BadgerEngine.deleteNodeInTxn.
func (tx *BadgerTransaction) deleteNodeBuffered(nodeID NodeID, oldNode *Node) (edgesDeleted int64, deletedEdgeIDs []EdgeID, err error) {
	key := nodeKey(nodeID)

	// Buffer deletion of separately stored embeddings
	embPrefix := embeddingPrefix(nodeID)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = embPrefix
	it := tx.badgerTx.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		tx.bufferDelete(it.Item().Key())
	}

	// Get node for label cleanup (if not already provided)
	var deletedNode *Node
	if oldNode != nil {
		deletedNode = oldNode
	} else {
		item, err := tx.badgerTx.Get(key)
		if err == badger.ErrKeyNotFound {
			// Node doesn't exist, but we've already buffered embedding cleanup.
			// Also buffer pending embeddings index deletion.
			tx.bufferDelete(pendingEmbedKey(nodeID))
			return 0, nil, ErrNotFound
		}
		if err != nil {
			return 0, nil, err
		}

		if err := item.Value(func(val []byte) error {
			var decodeErr error
			// Extract nodeID from key (skip prefix byte)
			nodeIDFromKey := NodeID(key[1:])
			deletedNode, decodeErr = decodeNodeWithEmbeddings(tx.badgerTx, val, nodeIDFromKey)
			return decodeErr
		}); err != nil {
			return 0, nil, err
		}
	}

	// Buffer label index deletions
	for _, label := range deletedNode.Labels {
		tx.bufferDelete(labelIndexKey(label, nodeID))
	}

	// Delete outgoing edges (and track count)
	outPrefix := outgoingIndexPrefix(nodeID)
	outCount, outIDs, err := tx.deleteEdgesWithPrefixBuffered(outPrefix)
	if err != nil {
		return 0, nil, err
	}
	edgesDeleted += outCount
	deletedEdgeIDs = append(deletedEdgeIDs, outIDs...)

	// Delete incoming edges (and track count)
	inPrefix := incomingIndexPrefix(nodeID)
	inCount, inIDs, err := tx.deleteEdgesWithPrefixBuffered(inPrefix)
	if err != nil {
		return 0, nil, err
	}
	edgesDeleted += inCount
	deletedEdgeIDs = append(deletedEdgeIDs, inIDs...)

	// Buffer pending embeddings index deletion
	tx.bufferDelete(pendingEmbedKey(nodeID))

	// Buffer node deletion
	tx.bufferDelete(key)

	return edgesDeleted, deletedEdgeIDs, nil
}

// deleteEdgesWithPrefixBuffered deletes all edges with a given prefix, buffering writes.
func (tx *BadgerTransaction) deleteEdgesWithPrefixBuffered(prefix []byte) (int64, []EdgeID, error) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := tx.badgerTx.NewIterator(opts)
	defer it.Close()

	var edgeIDs []EdgeID
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		edgeID := extractEdgeIDFromIndexKey(it.Item().Key())
		edgeIDs = append(edgeIDs, edgeID)
	}

	var deletedCount int64
	var deletedIDs []EdgeID
	for _, edgeID := range edgeIDs {
		// Get edge to delete its indexes
		edgeKey := edgeKey(edgeID)
		item, err := tx.badgerTx.Get(edgeKey)
		if err == badger.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return 0, nil, err
		}

		var edgeBytes []byte
		if err := item.Value(func(val []byte) error {
			edgeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			return 0, nil, err
		}

		edge, err := deserializeEdge(edgeBytes)
		if err != nil {
			return 0, nil, err
		}

		// Buffer edge and index deletions
		tx.bufferDelete(edgeKey)
		tx.bufferDelete(outgoingIndexKey(edge.StartNode, edgeID))
		tx.bufferDelete(incomingIndexKey(edge.EndNode, edgeID))
		tx.bufferDelete(edgeTypeIndexKey(edge.Type, edgeID))

		deletedCount++
		deletedIDs = append(deletedIDs, edgeID)
	}

	return deletedCount, deletedIDs, nil
}

// DeleteNode deletes a node from the transaction.
func (tx *BadgerTransaction) DeleteNode(nodeID NodeID) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	// Capture old node state for constraint bookkeeping (e.g., unique value unregister).
	var oldNode *Node
	if pending, exists := tx.pendingNodes[nodeID]; exists {
		oldNode = copyNode(pending)
	} else {
		var err error
		oldNode, err = tx.getCommittedNodeLocked(nodeID)
		if err == ErrNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
	}

	// Delete with the same semantics as BadgerEngine.DeleteNode (cascade edges + embedding cleanup),
	// but buffer all writes for batch commit.
	edgesDeleted, deletedEdgeIDs, err := tx.deleteNodeBuffered(nodeID, oldNode)
	if err != nil {
		return err
	}

	// Track deletion
	delete(tx.pendingNodes, nodeID)
	tx.deletedNodes[nodeID] = struct{}{}

	tx.operations = append(tx.operations, Operation{
		Type:           OpDeleteNode,
		Timestamp:      time.Now(),
		NodeID:         nodeID,
		OldNode:        oldNode,
		EdgesDeleted:   edgesDeleted,
		DeletedEdgeIDs: deletedEdgeIDs,
	})

	return nil
}

// CreateEdge adds an edge to the transaction.
func (tx *BadgerTransaction) CreateEdge(edge *Edge) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	// Check nodes exist
	if !tx.nodeExists(edge.StartNode) {
		return fmt.Errorf("start node %s does not exist", edge.StartNode)
	}
	if !tx.nodeExists(edge.EndNode) {
		return fmt.Errorf("end node %s does not exist", edge.EndNode)
	}

	// Check for duplicate
	if _, exists := tx.pendingEdges[edge.ID]; exists {
		return ErrAlreadyExists
	}

	// Serialize and buffer write
	edgeBytes, err := serializeEdge(edge)
	if err != nil {
		return fmt.Errorf("serializing edge: %w", err)
	}

	key := edgeKey(edge.ID)
	tx.bufferSet(key, edgeBytes)

	// Buffer edge indexes
	outKey := outgoingIndexKey(edge.StartNode, edge.ID)
	tx.bufferSet(outKey, []byte{})

	inKey := incomingIndexKey(edge.EndNode, edge.ID)
	tx.bufferSet(inKey, []byte{})

	// Buffer edge type index for GetEdgesByType().
	// Without this, edges created inside implicit/explicit transactions are invisible
	// to type-based scans and Cypher fast-paths that rely on the edge-type index.
	tx.bufferSet(edgeTypeIndexKey(edge.Type, edge.ID), []byte{})

	// Track for read-your-writes
	edgeCopy := copyEdge(edge)
	tx.pendingEdges[edge.ID] = edgeCopy

	tx.operations = append(tx.operations, Operation{
		Type:      OpCreateEdge,
		Timestamp: time.Now(),
		EdgeID:    edge.ID,
		Edge:      edgeCopy,
	})

	return nil
}

// UpdateEdge updates an existing edge within the transaction.
//
// This is required so Cypher can do CREATE ... SET r.prop = ... in a single query
// while using implicit/explicit transactions (writes must remain isolated until commit).
func (tx *BadgerTransaction) UpdateEdge(edge *Edge) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if edge == nil {
		return ErrInvalidData
	}
	if edge.ID == "" {
		return ErrInvalidID
	}
	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}
	if _, deleted := tx.deletedEdges[edge.ID]; deleted {
		return ErrNotFound
	}

	// Load existing edge (pending or committed) for index maintenance.
	var oldEdge *Edge
	if pending, exists := tx.pendingEdges[edge.ID]; exists {
		oldEdge = copyEdge(pending)
	} else {
		var err error
		oldEdge, err = tx.getCommittedEdgeLocked(edge.ID)
		if err == ErrNotFound {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("reading edge: %w", err)
		}
	}

	// If endpoints changed, verify they exist and update outgoing/incoming indexes.
	if oldEdge.StartNode != edge.StartNode || oldEdge.EndNode != edge.EndNode {
		if !tx.nodeExists(edge.StartNode) {
			return fmt.Errorf("start node %s does not exist", edge.StartNode)
		}
		if !tx.nodeExists(edge.EndNode) {
			return fmt.Errorf("end node %s does not exist", edge.EndNode)
		}

		tx.bufferDelete(outgoingIndexKey(oldEdge.StartNode, edge.ID))
		tx.bufferDelete(incomingIndexKey(oldEdge.EndNode, edge.ID))
		tx.bufferSet(outgoingIndexKey(edge.StartNode, edge.ID), []byte{})
		tx.bufferSet(incomingIndexKey(edge.EndNode, edge.ID), []byte{})
	}

	// If type changed, update edge type index.
	if oldEdge.Type != edge.Type {
		if oldEdge.Type != "" {
			tx.bufferDelete(edgeTypeIndexKey(oldEdge.Type, edge.ID))
		}
		if edge.Type != "" {
			tx.bufferSet(edgeTypeIndexKey(edge.Type, edge.ID), []byte{})
		}
	}

	// Serialize and buffer updated edge record.
	edgeBytes, err := serializeEdge(edge)
	if err != nil {
		return fmt.Errorf("serializing edge: %w", err)
	}
	tx.bufferSet(edgeKey(edge.ID), edgeBytes)

	// Track for read-your-writes.
	edgeCopy := copyEdge(edge)
	tx.pendingEdges[edge.ID] = edgeCopy

	tx.operations = append(tx.operations, Operation{
		Type:      OpUpdateEdge,
		Timestamp: time.Now(),
		EdgeID:    edge.ID,
		Edge:      edgeCopy,
		OldEdge:   oldEdge,
	})

	return nil
}

// DeleteEdge deletes an edge from the transaction.
func (tx *BadgerTransaction) DeleteEdge(edgeID EdgeID) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	// Get edge to delete its indexes
	var edge *Edge
	if pending, exists := tx.pendingEdges[edgeID]; exists {
		edge = pending
	} else {
		var err error
		edge, err = tx.getCommittedEdgeLocked(edgeID)
		if err == ErrNotFound {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("reading edge: %w", err)
		}
	}

	// Buffer edge deletion
	key := edgeKey(edgeID)
	tx.bufferDelete(key)

	// Buffer index deletions
	outKey := outgoingIndexKey(edge.StartNode, edgeID)
	tx.bufferDelete(outKey)

	inKey := incomingIndexKey(edge.EndNode, edgeID)
	tx.bufferDelete(inKey)

	// Buffer edge type index deletion.
	tx.bufferDelete(edgeTypeIndexKey(edge.Type, edgeID))

	// Track deletion
	delete(tx.pendingEdges, edgeID)
	tx.deletedEdges[edgeID] = struct{}{}

	tx.operations = append(tx.operations, Operation{
		Type:      OpDeleteEdge,
		Timestamp: time.Now(),
		EdgeID:    edgeID,
		OldEdge:   edge,
	})

	return nil
}

// GetNode retrieves a node (read-your-writes).
func (tx *BadgerTransaction) GetNode(nodeID NodeID) (*Node, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Check deleted
	if _, deleted := tx.deletedNodes[nodeID]; deleted {
		return nil, ErrNotFound
	}

	// Check pending
	if node, exists := tx.pendingNodes[nodeID]; exists {
		return copyNode(node), nil
	}

	return tx.getCommittedNodeLocked(nodeID)
}

// Commit applies all changes atomically with full constraint validation.
// Explicit transactions get strict ACID durability with immediate fsync.
func (tx *BadgerTransaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	// Final constraint validation before commit
	if err := tx.validateAllConstraints(); err != nil {
		tx.badgerTx.Discard()
		tx.Status = TxStatusRolledBack
		return fmt.Errorf("constraint violation: %w", err)
	}

	if err := tx.validateSnapshotIsolationConflicts(); err != nil {
		tx.badgerTx.Discard()
		tx.Status = TxStatusRolledBack
		return err
	}

	temporalTargets, err := tx.bufferTemporalIndexWrites()
	if err != nil {
		tx.badgerTx.Discard()
		tx.Status = TxStatusRolledBack
		return fmt.Errorf("buffering temporal index writes: %w", err)
	}

	// Log metadata
	if len(tx.Metadata) > 0 {
		log.Printf("[Transaction %s] Committing with metadata: %v", tx.ID, tx.Metadata)
	}

	if len(tx.operations) > 0 || len(tx.pendingWrites) > 0 || len(tx.pendingDeletes) > 0 {
		version, err := tx.engine.allocateMVCCVersion(tx.badgerTx, time.Now())
		if err != nil {
			tx.badgerTx.Discard()
			tx.Status = TxStatusRolledBack
			return fmt.Errorf("allocating mvcc commit version: %w", err)
		}
		tx.CommitVersion = version
		if err := tx.engine.materializeMVCCCommitInTxn(tx.badgerTx, version, tx.operations); err != nil {
			tx.badgerTx.Discard()
			tx.Status = TxStatusRolledBack
			return fmt.Errorf("materializing mvcc commit state: %w", err)
		}
	}

	// Flush all buffered writes before committing
	// This batches all writes together for better performance while maintaining ACID guarantees
	if err := tx.flushBufferedWrites(); err != nil {
		tx.badgerTx.Discard()
		tx.Status = TxStatusRolledBack
		return fmt.Errorf("flushing buffered writes: %w", err)
	}

	if err := tx.refreshTemporalCurrentPointers(temporalTargets); err != nil {
		tx.badgerTx.Discard()
		tx.Status = TxStatusRolledBack
		return fmt.Errorf("refreshing temporal current pointers: %w", err)
	}

	// Commit Badger transaction (atomic!)
	if err := tx.badgerTx.Commit(); err != nil {
		tx.Status = TxStatusRolledBack
		return normalizeTransactionCommitError(err)
	}

	// Apply cache/count updates and fire callbacks after commit.
	// This keeps cached stats O(1) and ensures external systems (e.g. search indexes)
	// observe transactional writes the same way as non-transactional writes.
	for _, op := range tx.operations {
		switch op.Type {
		case OpCreateNode:
			tx.engine.cacheOnNodeCreated(op.Node)
			tx.engine.notifyNodeCreated(op.Node)
		case OpUpdateNode:
			tx.engine.cacheOnNodeUpdatedWithOldNode(op.Node, op.OldNode)
			tx.engine.notifyNodeUpdated(op.Node)
		case OpDeleteNode:
			if op.OldNode != nil {
				tx.engine.cacheOnNodeDeletedWithLabels(op.NodeID, op.OldNode.Labels, op.EdgesDeleted)
			} else {
				tx.engine.cacheOnNodeDeleted(op.NodeID, op.EdgesDeleted)
			}
			for _, edgeID := range op.DeletedEdgeIDs {
				tx.engine.notifyEdgeDeleted(edgeID)
			}
			tx.engine.notifyNodeDeleted(op.NodeID)
		case OpCreateEdge:
			tx.engine.cacheOnEdgeCreated(op.Edge)
			tx.engine.notifyEdgeCreated(op.Edge)
		case OpUpdateEdge:
			oldType := ""
			if op.OldEdge != nil {
				oldType = op.OldEdge.Type
			}
			tx.engine.cacheOnEdgeUpdated(oldType, op.Edge)
			tx.engine.notifyEdgeUpdated(op.Edge)
		case OpDeleteEdge:
			oldType := ""
			if op.OldEdge != nil {
				oldType = op.OldEdge.Type
			}
			tx.engine.cacheOnEdgeDeleted(op.EdgeID, oldType)
			tx.engine.notifyEdgeDeleted(op.EdgeID)
		case OpUpdateEmbedding:
			// Embeddings are regenerable; no-op for cached counts.
		}
	}

	// Update derived unique-constraint caches (in-memory) based on committed operations.
	// This keeps non-transactional CreateNode() uniqueness checks consistent with transactional writes.
	//
	// NOTE: We don't persist these caches; they are rebuilt from stored nodes on startup.
	for _, op := range tx.operations {
		switch op.Type {
		case OpCreateNode:
			if op.Node == nil {
				continue
			}
			dbName, _, ok := ParseDatabasePrefix(string(op.Node.ID))
			if !ok {
				continue
			}
			schema := tx.engine.GetSchemaForNamespace(dbName)
			for _, label := range op.Node.Labels {
				for propName, propValue := range op.Node.Properties {
					schema.RegisterUniqueValue(label, propName, propValue, op.Node.ID)
				}
			}
		case OpUpdateNode:
			if op.OldNode != nil {
				dbName, _, ok := ParseDatabasePrefix(string(op.OldNode.ID))
				if ok {
					schema := tx.engine.GetSchemaForNamespace(dbName)
					for _, label := range op.OldNode.Labels {
						for propName, propValue := range op.OldNode.Properties {
							schema.UnregisterUniqueValue(label, propName, propValue)
						}
					}
				}
			}
			if op.Node != nil {
				dbName, _, ok := ParseDatabasePrefix(string(op.Node.ID))
				if ok {
					schema := tx.engine.GetSchemaForNamespace(dbName)
					for _, label := range op.Node.Labels {
						for propName, propValue := range op.Node.Properties {
							schema.RegisterUniqueValue(label, propName, propValue, op.Node.ID)
						}
					}
				}
			}
		case OpDeleteNode:
			if op.OldNode == nil {
				continue
			}
			dbName, _, ok := ParseDatabasePrefix(string(op.OldNode.ID))
			if !ok {
				continue
			}
			schema := tx.engine.GetSchemaForNamespace(dbName)
			for _, label := range op.OldNode.Labels {
				for propName, propValue := range op.OldNode.Properties {
					schema.UnregisterUniqueValue(label, propName, propValue)
				}
			}
		}
	}

	// ACID GUARANTEE: Force fsync for explicit transactions
	// This ensures durability - data is on disk before we return success
	// Non-transactional writes use batch sync for better performance
	// Note: In-memory mode (testing) skips fsync as there's no disk
	if !tx.engine.IsInMemory() {
		if err := tx.engine.Sync(); err != nil {
			// Transaction is committed in Badger but fsync failed
			// Log error but don't rollback - data is in Badger's WAL
			log.Printf("[Transaction %s] Warning: fsync failed after commit: %v", tx.ID, err)
		}
	}

	tx.Status = TxStatusCommitted
	return nil
}

func normalizeTransactionCommitError(err error) error {
	if errors.Is(err, ErrConflict) || errors.Is(err, badger.ErrConflict) {
		return fmt.Errorf("%w: concurrent transaction modified data before commit: %w", ErrConflict, err)
	}
	return fmt.Errorf("badger commit failed: %w", err)
}

// Rollback discards all changes.
func (tx *BadgerTransaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	tx.badgerTx.Discard()
	// Clear buffered writes on rollback
	tx.pendingWrites = make(map[string][]byte)
	tx.pendingDeletes = make(map[string]bool)
	tx.Status = TxStatusRolledBack
	return nil
}

// SetMetadata sets transaction metadata (same as Transaction).
func (tx *BadgerTransaction) SetMetadata(metadata map[string]interface{}) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	// Validate size
	totalSize := 0
	for k, v := range metadata {
		totalSize += len(k)
		if v != nil {
			totalSize += len(fmt.Sprint(v))
		}
	}

	if totalSize > 2048 {
		return fmt.Errorf("transaction metadata too large: %d chars (max 2048)", totalSize)
	}

	// Merge
	if tx.Metadata == nil {
		tx.Metadata = make(map[string]interface{})
	}
	for k, v := range metadata {
		tx.Metadata[k] = v
	}

	return nil
}

// GetMetadata returns transaction metadata copy.
func (tx *BadgerTransaction) GetMetadata() map[string]interface{} {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	result := make(map[string]interface{})
	for k, v := range tx.Metadata {
		result[k] = v
	}
	return result
}

// OperationCount returns the number of buffered operations.
func (tx *BadgerTransaction) OperationCount() int {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return len(tx.operations)
}

func (tx *BadgerTransaction) getCommittedNodeLocked(nodeID NodeID) (*Node, error) {
	if tx.readTS.IsZero() {
		key := nodeKey(nodeID)
		item, err := tx.badgerTx.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("reading node: %w", err)
		}
		var nodeBytes []byte
		if err := item.Value(func(val []byte) error {
			nodeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("reading node value: %w", err)
		}
		return deserializeNode(nodeBytes)
	}
	return tx.engine.GetNodeVisibleAt(nodeID, tx.readTS)
}

func (tx *BadgerTransaction) getCommittedEdgeLocked(edgeID EdgeID) (*Edge, error) {
	if tx.readTS.IsZero() {
		key := edgeKey(edgeID)
		item, err := tx.badgerTx.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("reading edge: %w", err)
		}
		var edgeBytes []byte
		if err := item.Value(func(val []byte) error {
			edgeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("reading edge value: %w", err)
		}
		return deserializeEdge(edgeBytes)
	}
	return tx.engine.GetEdgeVisibleAt(edgeID, tx.readTS)
}

func (tx *BadgerTransaction) getNodesByLabelLocked(label string) ([]*Node, error) {
	if tx.readTS.IsZero() {
		return tx.engine.GetNodesByLabel(label)
	}
	return tx.engine.GetNodesByLabelVisibleAt(label, tx.readTS)
}

// nodeExists checks if a node exists (pending or storage).
func (tx *BadgerTransaction) nodeExists(nodeID NodeID) bool {
	if _, deleted := tx.deletedNodes[nodeID]; deleted {
		return false
	}
	if _, exists := tx.pendingNodes[nodeID]; exists {
		return true
	}

	_, err := tx.getCommittedNodeLocked(nodeID)
	return err == nil
}

func (tx *BadgerTransaction) validateSnapshotIsolationConflicts() error {
	for _, op := range tx.operations {
		switch op.Type {
		case OpCreateNode:
			if err := tx.checkNodeCreateConflict(op.NodeID); err != nil {
				return err
			}
		case OpUpdateNode, OpDeleteNode, OpUpdateEmbedding:
			if err := tx.checkNodeWriteConflict(op.NodeID); err != nil {
				return err
			}
			if op.Type == OpDeleteNode {
				if err := tx.checkNodeAdjacencyConflict(op.NodeID); err != nil {
					return err
				}
			}
		case OpCreateEdge:
			if err := tx.checkEdgeCreateConflict(op.EdgeID); err != nil {
				return err
			}
			if err := tx.checkEdgeEndpointConflicts(op.Edge); err != nil {
				return err
			}
		case OpUpdateEdge:
			if err := tx.checkEdgeWriteConflict(op.EdgeID); err != nil {
				return err
			}
			if err := tx.checkEdgeEndpointConflicts(op.Edge); err != nil {
				return err
			}
		case OpDeleteEdge:
			if err := tx.checkEdgeWriteConflict(op.EdgeID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (tx *BadgerTransaction) checkNodeCreateConflict(nodeID NodeID) error {
	head, err := tx.engine.GetNodeCurrentHead(nodeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if head.Version.Compare(tx.readTS) > 0 {
		return fmt.Errorf("%w: node %s changed after transaction start", ErrConflict, nodeID)
	}
	return nil
}

func (tx *BadgerTransaction) checkNodeWriteConflict(nodeID NodeID) error {
	head, err := tx.engine.GetNodeCurrentHead(nodeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if head.Version.Compare(tx.readTS) > 0 {
		return fmt.Errorf("%w: node %s changed after transaction start", ErrConflict, nodeID)
	}
	return nil
}

func (tx *BadgerTransaction) checkEdgeCreateConflict(edgeID EdgeID) error {
	head, err := tx.engine.GetEdgeCurrentHead(edgeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if head.Version.Compare(tx.readTS) > 0 {
		return fmt.Errorf("%w: edge %s changed after transaction start", ErrConflict, edgeID)
	}
	return nil
}

func (tx *BadgerTransaction) checkEdgeWriteConflict(edgeID EdgeID) error {
	head, err := tx.engine.GetEdgeCurrentHead(edgeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if head.Version.Compare(tx.readTS) > 0 {
		return fmt.Errorf("%w: edge %s changed after transaction start", ErrConflict, edgeID)
	}
	return nil
}

func (tx *BadgerTransaction) checkEdgeEndpointConflicts(edge *Edge) error {
	if edge == nil {
		return nil
	}
	for _, nodeID := range []NodeID{edge.StartNode, edge.EndNode} {
		if pending, exists := tx.pendingNodes[nodeID]; exists {
			if pending != nil {
				continue
			}
		}
		head, err := tx.engine.GetNodeCurrentHead(nodeID)
		if err == ErrNotFound {
			return ErrInvalidEdge
		}
		if err != nil {
			return err
		}
		if head.Tombstoned {
			if head.Version.Compare(tx.readTS) > 0 {
				return fmt.Errorf("%w: endpoint node %s was deleted after transaction start", ErrConflict, nodeID)
			}
			return ErrInvalidEdge
		}
	}
	return nil
}

func (tx *BadgerTransaction) checkNodeAdjacencyConflict(nodeID NodeID) error {
	return tx.engine.withView(func(viewTx *badger.Txn) error {
		prefixes := [][]byte{outgoingIndexPrefix(nodeID), incomingIndexPrefix(nodeID)}
		for _, prefix := range prefixes {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = false
			it := viewTx.NewIterator(opts)
			for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
				edgeID := extractEdgeIDFromIndexKey(it.Item().Key())
				head, err := tx.engine.loadEdgeMVCCHeadInTxn(viewTx, edgeID)
				if err == ErrNotFound {
					continue
				}
				if err != nil {
					it.Close()
					return err
				}
				if head.Version.Compare(tx.readTS) > 0 {
					it.Close()
					return fmt.Errorf("%w: node %s has adjacent edge %s changed after transaction start", ErrConflict, nodeID, edgeID)
				}
			}
			it.Close()
		}
		return nil
	})
}

// shouldSkipCreateExistenceCheck avoids a read-before-write for UUID-based IDs.
// UUID collisions are negligible for generated IDs, so we skip the read to save I/O.
func shouldSkipCreateExistenceCheck(nodeID NodeID) bool {
	_, rawID, ok := ParseDatabasePrefix(string(nodeID))
	if !ok {
		return false
	}
	_, err := uuid.Parse(rawID)
	return err == nil
}

// validateNodeConstraints checks all constraints for a node.
func (tx *BadgerTransaction) validateNodeConstraints(node *Node) error {
	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return fmt.Errorf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got: %s", node.ID)
	}

	// Get constraints from the schema for this namespace (per DB).
	schema := tx.engine.GetSchemaForNamespace(dbName)
	constraints := schema.GetConstraintsForLabels(node.Labels)

	for _, constraint := range constraints {
		switch constraint.Type {
		case ConstraintUnique:
			if err := tx.checkUniqueConstraint(node, constraint); err != nil {
				return err
			}
		case ConstraintNodeKey:
			if err := tx.checkNodeKeyConstraint(node, constraint); err != nil {
				return err
			}
		case ConstraintExists:
			if err := tx.checkExistenceConstraint(node, constraint); err != nil {
				return err
			}
		case ConstraintTemporal:
			if err := tx.checkTemporalConstraint(node, constraint); err != nil {
				return err
			}
		}
	}

	typeConstraints := schema.GetPropertyTypeConstraintsForLabels(node.Labels)
	for _, constraint := range typeConstraints {
		value := node.Properties[constraint.Property]
		if err := ValidatePropertyType(value, constraint.ExpectedType); err != nil {
			return &ConstraintViolationError{
				Type:       ConstraintPropertyType,
				Label:      constraint.Label,
				Properties: []string{constraint.Property},
				Message:    fmt.Sprintf("Property %s must be %s (%v)", constraint.Property, constraint.ExpectedType, err),
			}
		}
	}

	return nil
}

// checkUniqueConstraint ensures property value is unique across ALL data.
func (tx *BadgerTransaction) checkUniqueConstraint(node *Node, c Constraint) error {
	prop := c.Properties[0]
	value := node.Properties[prop]

	if value == nil {
		return nil // NULL doesn't violate uniqueness
	}

	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return fmt.Errorf("node ID must be prefixed with namespace, got: %s", node.ID)
	}
	nsPrefix := dbName + ":"

	// Check pending nodes in this transaction (namespace-scoped).
	for id, n := range tx.pendingNodes {
		if id == node.ID {
			continue
		}
		if !strings.HasPrefix(string(id), nsPrefix) {
			continue
		}
		if hasLabel(n.Labels, c.Label) && n.Properties[prop] == value {
			return &ConstraintViolationError{
				Type:       ConstraintUnique,
				Label:      c.Label,
				Properties: []string{prop},
				Message:    fmt.Sprintf("Node with %s=%v already exists in transaction", prop, value),
			}
		}
	}

	// Full-scan check: scan all existing nodes with this label (namespace-scoped).
	if err := tx.scanForUniqueViolation(dbName, c.Label, prop, value, node.ID); err != nil {
		return err
	}

	return nil
}

// scanForUniqueViolation performs a full database scan to check for UNIQUE violations
// within a single namespace (database).
func (tx *BadgerTransaction) scanForUniqueViolation(namespace, label, property string, value interface{}, excludeNodeID NodeID) error {
	nodes, err := tx.getNodesByLabelLocked(label)
	if err != nil {
		return err
	}
	for _, existingNode := range nodes {
		if existingNode == nil || existingNode.ID == excludeNodeID {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(existingNode.ID), namespace+":") {
			continue
		}
		if existingValue, ok := existingNode.Properties[property]; ok && compareValues(existingValue, value) {
			return &ConstraintViolationError{
				Type:       ConstraintUnique,
				Label:      label,
				Properties: []string{property},
				Message:    fmt.Sprintf("Node with %s=%v already exists (nodeID: %s)", property, value, existingNode.ID),
			}
		}
	}

	return nil
}

// checkNodeKeyConstraint ensures composite key uniqueness across ALL data.
func (tx *BadgerTransaction) checkNodeKeyConstraint(node *Node, c Constraint) error {
	values := make([]interface{}, len(c.Properties))
	for i, prop := range c.Properties {
		values[i] = node.Properties[prop]
		if values[i] == nil {
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      c.Label,
				Properties: c.Properties,
				Message:    fmt.Sprintf("NODE KEY property %s cannot be null", prop),
			}
		}
	}

	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return fmt.Errorf("node ID must be prefixed with namespace, got: %s", node.ID)
	}
	nsPrefix := dbName + ":"

	// Check pending nodes in this transaction (namespace-scoped).
	for id, n := range tx.pendingNodes {
		if id == node.ID {
			continue
		}
		if !strings.HasPrefix(string(id), nsPrefix) {
			continue
		}
		if !hasLabel(n.Labels, c.Label) {
			continue
		}

		match := true
		for i, prop := range c.Properties {
			if !compareValues(n.Properties[prop], values[i]) {
				match = false
				break
			}
		}

		if match {
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      c.Label,
				Properties: c.Properties,
				Message:    fmt.Sprintf("Node with key %v=%v already exists in transaction", c.Properties, values),
			}
		}
	}

	// Full-scan check: scan all existing nodes with this label (namespace-scoped).
	if err := tx.scanForNodeKeyViolation(dbName, c.Label, c.Properties, values, node.ID); err != nil {
		return err
	}

	return nil
}

// scanForNodeKeyViolation performs a full database scan to check for NODE KEY violations
// within a single namespace (database).
func (tx *BadgerTransaction) scanForNodeKeyViolation(namespace, label string, properties []string, values []interface{}, excludeNodeID NodeID) error {
	nodes, err := tx.getNodesByLabelLocked(label)
	if err != nil {
		return err
	}
	for _, existingNode := range nodes {
		if existingNode == nil || existingNode.ID == excludeNodeID {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(existingNode.ID), namespace+":") {
			continue
		}

		match := true
		for i, prop := range properties {
			existingValue, ok := existingNode.Properties[prop]
			if !ok || !compareValues(existingValue, values[i]) {
				match = false
				break
			}
		}

		if match {
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      label,
				Properties: properties,
				Message:    fmt.Sprintf("Node with composite key %v=%v already exists (nodeID: %s)", properties, values, existingNode.ID),
			}
		}
	}

	return nil
}

// checkExistenceConstraint ensures required property exists.
func (tx *BadgerTransaction) checkExistenceConstraint(node *Node, c Constraint) error {
	prop := c.Properties[0]
	value := node.Properties[prop]

	if value == nil {
		return &ConstraintViolationError{
			Type:       ConstraintExists,
			Label:      c.Label,
			Properties: []string{prop},
			Message:    fmt.Sprintf("Property %s is required but missing", prop),
		}
	}

	return nil
}

// checkTemporalConstraint enforces TEMPORAL NO OVERLAP constraints within a transaction.
//
// It must validate against:
// - other pending nodes in this transaction (read-your-writes), and
// - existing committed nodes in storage (via the label index scan).
func (tx *BadgerTransaction) checkTemporalConstraint(node *Node, c Constraint) error {
	if len(c.Properties) != 3 {
		return fmt.Errorf("TEMPORAL constraint requires 3 properties (key, valid_from, valid_to)")
	}

	keyProp := c.Properties[0]
	startProp := c.Properties[1]
	endProp := c.Properties[2]

	keyVal := node.Properties[keyProp]
	if keyVal == nil {
		return &ConstraintViolationError{
			Type:       ConstraintTemporal,
			Label:      c.Label,
			Properties: c.Properties,
			Message:    fmt.Sprintf("TEMPORAL key property %s cannot be null", keyProp),
		}
	}

	start, ok := coerceTemporalTime(node.Properties[startProp])
	if !ok {
		return &ConstraintViolationError{
			Type:       ConstraintTemporal,
			Label:      c.Label,
			Properties: c.Properties,
			Message:    fmt.Sprintf("TEMPORAL start property %s must be a datetime", startProp),
		}
	}
	end, hasEnd := coerceTemporalTime(node.Properties[endProp])

	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return fmt.Errorf("node ID must be prefixed with namespace, got: %s", node.ID)
	}
	nsPrefix := dbName + ":"

	// Check overlaps against pending nodes in this transaction (namespace + label + key match).
	for id, other := range tx.pendingNodes {
		if id == node.ID {
			continue
		}
		if !strings.HasPrefix(string(id), nsPrefix) {
			continue
		}
		if !hasLabel(other.Labels, c.Label) {
			continue
		}

		otherKey := other.Properties[keyProp]
		if otherKey == nil || !compareValues(otherKey, keyVal) {
			continue
		}

		otherStart, ok := coerceTemporalTime(other.Properties[startProp])
		if !ok {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      c.Label,
				Properties: []string{keyProp, startProp, endProp},
				Message:    fmt.Sprintf("TEMPORAL constraint requires %s for node %s", startProp, id),
			}
		}
		otherEnd, otherHasEnd := coerceTemporalTime(other.Properties[endProp])

		if intervalsOverlap(
			temporalInterval{start: start, end: end, hasEnd: hasEnd},
			temporalInterval{start: otherStart, end: otherEnd, hasEnd: otherHasEnd},
		) {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      c.Label,
				Properties: []string{keyProp, startProp, endProp},
				Message: fmt.Sprintf("TEMPORAL constraint violation: overlap with node %s for %s=%v",
					id, keyProp, keyVal),
			}
		}
	}

	// Check overlaps against committed storage using the label index scan.
	visibleNodes, err := tx.getNodesByLabelLocked(c.Label)
	if err != nil {
		return err
	}
	for _, other := range visibleNodes {
		if other == nil || other.ID == node.ID {
			continue
		}
		if !strings.HasPrefix(string(other.ID), nsPrefix) {
			continue
		}
		otherKey := other.Properties[keyProp]
		if otherKey == nil || !compareValues(otherKey, keyVal) {
			continue
		}
		otherStart, ok := coerceTemporalTime(other.Properties[startProp])
		if !ok {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      c.Label,
				Properties: []string{keyProp, startProp, endProp},
				Message:    fmt.Sprintf("TEMPORAL constraint requires %s for node %s", startProp, other.ID),
			}
		}
		otherEnd, otherHasEnd := coerceTemporalTime(other.Properties[endProp])
		if intervalsOverlap(
			temporalInterval{start: start, end: end, hasEnd: hasEnd},
			temporalInterval{start: otherStart, end: otherEnd, hasEnd: otherHasEnd},
		) {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      c.Label,
				Properties: []string{keyProp, startProp, endProp},
				Message: fmt.Sprintf("TEMPORAL constraint violation: overlap with node %s for %s=%v",
					other.ID, keyProp, keyVal),
			}
		}
	}
	return nil
}

// validateAllConstraints performs final validation before commit.
func (tx *BadgerTransaction) validateAllConstraints() error {
	for _, node := range tx.pendingNodes {
		if err := tx.validateNodeConstraints(node); err != nil {
			return err
		}
	}
	return nil
}

// Helper: check if node has label
func hasLabel(labels []string, target string) bool {
	for _, label := range labels {
		if label == target {
			return true
		}
	}
	return false
}

// ConstraintViolationError is returned when a constraint is violated.
type ConstraintViolationError struct {
	Type       ConstraintType
	Label      string
	Properties []string
	Message    string
}

func (e *ConstraintViolationError) Error() string {
	return fmt.Sprintf("Constraint violation (%s on %s.%v): %s",
		e.Type, e.Label, e.Properties, e.Message)
}
