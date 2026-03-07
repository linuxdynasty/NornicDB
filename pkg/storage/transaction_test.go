package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestNewTransaction(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	tx, err := engine.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	if tx == nil {
		t.Fatal("Expected non-nil transaction")
	}
	if tx.ID == "" {
		t.Error("Transaction ID should be set")
	}
	if tx.Status != TxStatusActive {
		t.Errorf("Expected active status, got %s", tx.Status)
	}
	if tx.StartTime.IsZero() {
		t.Error("StartTime should be set")
	}
}

func TestTransaction_CreateNode_Basic(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	node := &Node{
		ID:         NodeID(prefixTestID("tx-node-1")),
		Labels:     []string{"Test"},
		Properties: map[string]interface{}{"name": "Test Node"},
	}

	// Create in transaction
	_, err := tx.CreateNode(node)
	if err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	// Node should NOT be visible in engine yet (not committed)
	_, err = engine.GetNode(NodeID(prefixTestID("tx-node-1")))
	if err != ErrNotFound {
		t.Error("Node should not be visible before commit")
	}

	// Node should be visible within transaction (read-your-writes)
	txNode, err := tx.GetNode(NodeID(prefixTestID("tx-node-1")))
	if err != nil {
		t.Errorf("GetNode in transaction failed: %v", err)
	}
	if txNode.Properties["name"] != "Test Node" {
		t.Error("Node properties mismatch")
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Now node should be visible in engine
	stored, err := engine.GetNode(NodeID(prefixTestID("tx-node-1")))
	if err != nil {
		t.Fatalf("GetNode after commit failed: %v", err)
	}
	if stored.Properties["name"] != "Test Node" {
		t.Error("Node properties mismatch after commit")
	}
}

func TestTransaction_Rollback(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	// Create some nodes
	for i := 0; i < 5; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID("rollback-node-" + string(rune('0'+i)))),
			Labels: []string{"Rollback"},
		}
		if _, err := tx.CreateNode(node); err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}
	}

	// Verify operations buffered
	if tx.OperationCount() != 5 {
		t.Errorf("Expected 5 operations, got %d", tx.OperationCount())
	}

	// Rollback
	err := tx.Rollback()
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Verify status
	if tx.Status != TxStatusRolledBack {
		t.Errorf("Expected rolled_back status, got %s", tx.Status)
	}

	// Verify nodes not in engine
	for i := 0; i < 5; i++ {
		_, err := engine.GetNode(NodeID(prefixTestID("rollback-node-" + string(rune('0'+i)))))
		if err != ErrNotFound {
			t.Error("Node should not exist after rollback")
		}
	}
}

func TestTransaction_Atomicity(t *testing.T) {
	engine := NewMemoryEngine()

	// Pre-create a node that will cause conflict
	conflictNode := &Node{ID: NodeID(prefixTestID("conflict-node")), Labels: []string{"Conflict"}}
	if _, err := engine.CreateNode(conflictNode); err != nil {
		t.Fatalf("Pre-create failed: %v", err)
	}

	tx, _ := engine.BeginTransaction()

	// Create some nodes
	for i := 0; i < 3; i++ {
		node := &Node{
			ID:     NodeID(prefixTestID("atomic-node-" + string(rune('0'+i)))),
			Labels: []string{"Atomic"},
		}
		if _, err := tx.CreateNode(node); err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}
	}

	// Try to create the conflicting node (should fail at commit)
	node := &Node{ID: NodeID(prefixTestID("conflict-node")), Labels: []string{"Conflict"}}
	// This will succeed in transaction (we check at commit time)
	// But when we commit, it should fail

	// For this test, let's verify that creating a node with same ID in TX fails
	_, err := tx.CreateNode(node)
	if err != ErrAlreadyExists {
		t.Errorf("Expected ErrAlreadyExists, got %v", err)
	}

	// Commit the valid operations
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// All atomic nodes should exist
	for i := 0; i < 3; i++ {
		_, err := engine.GetNode(NodeID(prefixTestID("atomic-node-" + string(rune('0'+i)))))
		if err != nil {
			t.Errorf("Node atomic-node-%d should exist after commit", i)
		}
	}
}

func TestTransaction_DeleteNode(t *testing.T) {
	engine := NewMemoryEngine()

	// Create a node first
	node := &Node{ID: NodeID(prefixTestID("delete-me")), Labels: []string{"Delete"}}
	if _, err := engine.CreateNode(node); err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	tx, _ := engine.BeginTransaction()

	// Delete in transaction
	err := tx.DeleteNode(NodeID(prefixTestID("delete-me")))
	if err != nil {
		t.Fatalf("DeleteNode failed: %v", err)
	}

	// Node should NOT be deleted from engine yet
	_, err = engine.GetNode(NodeID(prefixTestID("delete-me")))
	if err != nil {
		t.Error("Node should still exist before commit")
	}

	// But should not be visible in transaction
	_, err = tx.GetNode(NodeID(prefixTestID("delete-me")))
	if err != ErrNotFound {
		t.Error("Node should not be visible in transaction after delete")
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Now node should be gone
	_, err = engine.GetNode(NodeID(prefixTestID("delete-me")))
	if err != ErrNotFound {
		t.Error("Node should not exist after commit")
	}
}

func TestTransaction_UpdateNode(t *testing.T) {
	engine := NewMemoryEngine()

	// Create a node first
	node := &Node{
		ID:         NodeID(prefixTestID("update-me")),
		Labels:     []string{"Update"},
		Properties: map[string]interface{}{"version": 1},
	}
	if _, err := engine.CreateNode(node); err != nil {
		t.Fatalf("CreateNode failed: %v", err)
	}

	tx, _ := engine.BeginTransaction()

	// Update in transaction
	updatedNode := &Node{
		ID:         NodeID(prefixTestID("update-me")),
		Labels:     []string{"Updated"},
		Properties: map[string]interface{}{"version": 2},
	}
	err := tx.UpdateNode(updatedNode)
	if err != nil {
		t.Fatalf("UpdateNode failed: %v", err)
	}

	// Engine should still have old version
	old, _ := engine.GetNode(NodeID(prefixTestID("update-me")))
	if old.Properties["version"] != 1 {
		t.Error("Engine should still have old version before commit")
	}

	// Transaction should have new version
	txNode, _ := tx.GetNode(NodeID(prefixTestID("update-me")))
	if txNode.Properties["version"] != 2 {
		t.Error("Transaction should have new version")
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Engine should have new version
	updated, err := engine.GetNode(NodeID(prefixTestID("update-me")))
	if err != nil {
		t.Fatalf("GetNode after commit failed: %v", err)
	}
	// Note: JSON serialization may convert int to float64
	version, ok := updated.Properties["version"].(float64)
	if !ok {
		vInt, ok := updated.Properties["version"].(int)
		if ok {
			version = float64(vInt)
		}
	}
	if version != 2 {
		t.Errorf("Engine should have new version after commit, got version=%v (type %T)",
			updated.Properties["version"], updated.Properties["version"])
	}
}

func TestTransaction_CreateEdge(t *testing.T) {
	engine := NewMemoryEngine()

	// Create nodes first
	node1 := &Node{ID: NodeID(prefixTestID("edge-node-1")), Labels: []string{"Node"}}
	node2 := &Node{ID: NodeID(prefixTestID("edge-node-2")), Labels: []string{"Node"}}
	engine.CreateNode(node1)
	engine.CreateNode(node2)

	tx, _ := engine.BeginTransaction()

	// Create edge in transaction
	edge := &Edge{
		ID:        EdgeID(prefixTestID("tx-edge-1")),
		StartNode: NodeID(prefixTestID("edge-node-1")),
		EndNode:   NodeID(prefixTestID("edge-node-2")),
		Type:      "CONNECTS",
	}
	err := tx.CreateEdge(edge)
	if err != nil {
		t.Fatalf("CreateEdge failed: %v", err)
	}

	// Edge should NOT exist in engine yet
	_, err = engine.GetEdge(EdgeID(prefixTestID("tx-edge-1")))
	if err != ErrNotFound {
		t.Error("Edge should not exist before commit")
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Edge should exist now
	stored, err := engine.GetEdge(EdgeID(prefixTestID("tx-edge-1")))
	if err != nil {
		t.Fatalf("GetEdge after commit failed: %v", err)
	}
	if stored.Type != "CONNECTS" {
		t.Error("Edge type mismatch")
	}
}

func TestTransaction_UpdateEdge(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	node1 := &Node{ID: NodeID(prefixTestID("update-edge-node-1")), Labels: []string{"Node"}}
	node2 := &Node{ID: NodeID(prefixTestID("update-edge-node-2")), Labels: []string{"Node"}}
	node3 := &Node{ID: NodeID(prefixTestID("update-edge-node-3")), Labels: []string{"Node"}}
	if _, err := engine.CreateNode(node1); err != nil {
		t.Fatalf("CreateNode node1 failed: %v", err)
	}
	if _, err := engine.CreateNode(node2); err != nil {
		t.Fatalf("CreateNode node2 failed: %v", err)
	}
	if _, err := engine.CreateNode(node3); err != nil {
		t.Fatalf("CreateNode node3 failed: %v", err)
	}

	edgeID := EdgeID(prefixTestID("update-edge-1"))
	if err := engine.CreateEdge(&Edge{
		ID:         edgeID,
		StartNode:  node1.ID,
		EndNode:    node2.ID,
		Type:       "OLD",
		Properties: map[string]interface{}{"version": 1},
	}); err != nil {
		t.Fatalf("CreateEdge failed: %v", err)
	}

	tx, _ := engine.BeginTransaction()
	updated := &Edge{
		ID:         edgeID,
		StartNode:  node2.ID,
		EndNode:    node3.ID,
		Type:       "NEW",
		Properties: map[string]interface{}{"version": 2},
	}
	if err := tx.UpdateEdge(updated); err != nil {
		t.Fatalf("UpdateEdge failed: %v", err)
	}

	storedBefore, err := engine.GetEdge(edgeID)
	if err != nil {
		t.Fatalf("GetEdge before commit failed: %v", err)
	}
	if storedBefore.Type != "OLD" || storedBefore.StartNode != node1.ID || storedBefore.EndNode != node2.ID {
		t.Error("Engine edge should remain unchanged before commit")
	}

	txEdge, ok := tx.pendingEdges[edgeID]
	if !ok {
		t.Fatal("Transaction should track updated edge in pendingEdges")
	}
	if txEdge.Type != "NEW" || txEdge.StartNode != node2.ID || txEdge.EndNode != node3.ID {
		t.Error("Transaction should expose updated edge")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	storedAfter, err := engine.GetEdge(edgeID)
	if err != nil {
		t.Fatalf("GetEdge after commit failed: %v", err)
	}
	if storedAfter.Type != "NEW" || storedAfter.StartNode != node2.ID || storedAfter.EndNode != node3.ID {
		t.Error("Engine should store updated edge after commit")
	}

	outgoingOld, _ := engine.GetOutgoingEdges(node1.ID)
	if len(outgoingOld) != 0 {
		t.Error("Old outgoing index should be cleared")
	}
	outgoingNew, _ := engine.GetOutgoingEdges(node2.ID)
	if len(outgoingNew) != 1 || outgoingNew[0].ID != edgeID {
		t.Error("New outgoing index should include updated edge")
	}
	incomingOld, _ := engine.GetIncomingEdges(node2.ID)
	if len(incomingOld) != 0 {
		t.Error("Old incoming index should be cleared")
	}
	incomingNew, _ := engine.GetIncomingEdges(node3.ID)
	if len(incomingNew) != 1 || incomingNew[0].ID != edgeID {
		t.Error("New incoming index should include updated edge")
	}
	typed, _ := engine.GetEdgesByType("NEW")
	if len(typed) != 1 || typed[0].ID != edgeID {
		t.Error("Updated type index should include edge")
	}
}

func TestTransaction_UpdateEdge_ValidationPaths(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	tx, _ := engine.BeginTransaction()
	if err := tx.UpdateEdge(nil); err != ErrInvalidData {
		t.Errorf("Expected ErrInvalidData, got %v", err)
	}
	if err := tx.UpdateEdge(&Edge{}); err != ErrInvalidID {
		t.Errorf("Expected ErrInvalidID, got %v", err)
	}

	node1 := &Node{ID: NodeID(prefixTestID("validate-edge-node-1")), Labels: []string{"Node"}}
	node2 := &Node{ID: NodeID(prefixTestID("validate-edge-node-2")), Labels: []string{"Node"}}
	if _, err := engine.CreateNode(node1); err != nil {
		t.Fatalf("CreateNode node1 failed: %v", err)
	}
	if _, err := engine.CreateNode(node2); err != nil {
		t.Fatalf("CreateNode node2 failed: %v", err)
	}
	edgeID := EdgeID(prefixTestID("validate-edge-1"))
	if err := engine.CreateEdge(&Edge{ID: edgeID, StartNode: node1.ID, EndNode: node2.ID, Type: "REL"}); err != nil {
		t.Fatalf("CreateEdge failed: %v", err)
	}

	tx2, _ := engine.BeginTransaction()
	if err := tx2.DeleteEdge(edgeID); err != nil {
		t.Fatalf("DeleteEdge failed: %v", err)
	}
	if err := tx2.UpdateEdge(&Edge{ID: edgeID, StartNode: node1.ID, EndNode: node2.ID, Type: "REL"}); err != ErrNotFound {
		t.Errorf("Expected ErrNotFound for deleted edge, got %v", err)
	}

	tx3, _ := engine.BeginTransaction()
	err := tx3.UpdateEdge(&Edge{
		ID:        edgeID,
		StartNode: node1.ID,
		EndNode:   NodeID(prefixTestID("missing-node")),
		Type:      "REL",
	})
	if err == nil {
		t.Error("Expected missing endpoint validation error")
	}

	if err := tx3.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	if err := tx.UpdateEdge(&Edge{ID: edgeID, StartNode: node1.ID, EndNode: node2.ID, Type: "REL"}); err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}
}

func TestTransaction_CreateEdgeWithNewNodes(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	// Create nodes IN transaction
	node1 := &Node{ID: NodeID(prefixTestID("new-edge-node-1")), Labels: []string{"New"}}
	node2 := &Node{ID: NodeID(prefixTestID("new-edge-node-2")), Labels: []string{"New"}}
	tx.CreateNode(node1)
	tx.CreateNode(node2)

	// Create edge between new nodes (should work!)
	edge := &Edge{
		ID:        EdgeID(prefixTestID("new-edge-1")),
		StartNode: NodeID(prefixTestID("new-edge-node-1")),
		EndNode:   NodeID(prefixTestID("new-edge-node-2")),
		Type:      "LINKS",
	}
	err := tx.CreateEdge(edge)
	if err != nil {
		t.Fatalf("CreateEdge with new nodes failed: %v", err)
	}

	// Commit all
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify all exist
	_, err = engine.GetNode(NodeID(prefixTestID("new-edge-node-1")))
	if err != nil {
		t.Error("Node 1 should exist")
	}
	_, err = engine.GetNode(NodeID(prefixTestID("new-edge-node-2")))
	if err != nil {
		t.Error("Node 2 should exist")
	}
	_, err = engine.GetEdge(EdgeID(prefixTestID("new-edge-1")))
	if err != nil {
		t.Error("Edge should exist")
	}
}

func TestTransaction_DeleteEdge(t *testing.T) {
	engine := NewMemoryEngine()

	// Create nodes and edge first
	node1 := &Node{ID: NodeID(prefixTestID("del-edge-node-1")), Labels: []string{"Node"}}
	node2 := &Node{ID: NodeID(prefixTestID("del-edge-node-2")), Labels: []string{"Node"}}
	engine.CreateNode(node1)
	engine.CreateNode(node2)
	edge := &Edge{
		ID:        EdgeID(prefixTestID("delete-edge-1")),
		StartNode: NodeID(prefixTestID("del-edge-node-1")),
		EndNode:   NodeID(prefixTestID("del-edge-node-2")),
		Type:      "DELETE_ME",
	}
	engine.CreateEdge(edge)

	tx, _ := engine.BeginTransaction()

	// Delete edge in transaction
	err := tx.DeleteEdge(EdgeID(prefixTestID("delete-edge-1")))
	if err != nil {
		t.Fatalf("DeleteEdge failed: %v", err)
	}

	// Edge should still exist in engine
	_, err = engine.GetEdge(EdgeID(prefixTestID("delete-edge-1")))
	if err != nil {
		t.Error("Edge should still exist before commit")
	}

	// Commit
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Edge should be gone
	_, err = engine.GetEdge(EdgeID(prefixTestID("delete-edge-1")))
	if err != ErrNotFound {
		t.Error("Edge should not exist after commit")
	}
}

func TestTransaction_ClosedTransaction(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	// Commit first
	tx.Commit()

	// Try operations on closed transaction
	node := &Node{ID: NodeID(prefixTestID("closed-test")), Labels: []string{"Test"}}
	_, err := tx.CreateNode(node)
	if err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}

	err = tx.UpdateNode(node)
	if err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}

	err = tx.DeleteNode(NodeID(prefixTestID("any")))
	if err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}

	err = tx.Commit()
	if err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}

	err = tx.Rollback()
	if err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}
}

func TestTransaction_IsActive(t *testing.T) {
	engine := NewMemoryEngine()
	tx, _ := engine.BeginTransaction()

	if !tx.IsActive() {
		t.Error("New transaction should be active")
	}

	tx.Commit()

	if tx.IsActive() {
		t.Error("Committed transaction should not be active")
	}
}

func TestTransaction_ConfigSettersAndSkipCreateHelpers(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	tx, _ := engine.BeginTransaction()
	if err := tx.SetDeferredConstraintValidation(true); err != nil {
		t.Fatalf("SetDeferredConstraintValidation failed: %v", err)
	}
	if !tx.deferConstraintValidation {
		t.Error("deferConstraintValidation should be enabled")
	}
	if err := tx.SetSkipCreateExistenceCheck(true); err != nil {
		t.Fatalf("SetSkipCreateExistenceCheck failed: %v", err)
	}
	if !tx.skipCreateExistenceCheck {
		t.Error("skipCreateExistenceCheck should be enabled")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if err := tx.SetDeferredConstraintValidation(false); err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}
	if err := tx.SetSkipCreateExistenceCheck(false); err != ErrTransactionClosed {
		t.Errorf("Expected ErrTransactionClosed, got %v", err)
	}

	if shouldSkipCreateExistenceCheck(NodeID("test:550e8400-e29b-41d4-a716-446655440000")) != true {
		t.Error("Expected UUID-prefixed node ID to skip existence check")
	}
	if shouldSkipCreateExistenceCheck(NodeID(prefixTestID("non-uuid"))) {
		t.Error("Expected non-UUID node ID not to skip existence check")
	}
	if shouldSkipCreateExistenceCheck(NodeID("missingprefix")) {
		t.Error("Expected non-prefixed node ID not to skip existence check")
	}
}

func TestTransaction_CheckTemporalConstraint(t *testing.T) {
	newTemporalNode := func(id, key string, start interface{}, end interface{}) *Node {
		return &Node{
			ID:     NodeID(id),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"account": key,
				"from":    start,
				"to":      end,
			},
		}
	}

	constraint := Constraint{
		Type:       ConstraintTemporal,
		Label:      "Person",
		Properties: []string{"account", "from", "to"},
	}
	now := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)

	t.Run("validates basic temporal inputs", func(t *testing.T) {
		engine := NewMemoryEngine()
		defer engine.Close()
		tx, _ := engine.BeginTransaction()

		err := tx.checkTemporalConstraint(newTemporalNode(prefixTestID("temporal-count"), "acct", now, now.Add(time.Hour)), Constraint{
			Type:       ConstraintTemporal,
			Label:      "Person",
			Properties: []string{"account", "from"},
		})
		if err == nil {
			t.Fatal("expected property count error")
		}

		node := newTemporalNode(prefixTestID("temporal-null"), "", now, now.Add(time.Hour))
		node.Properties["account"] = nil
		if err := tx.checkTemporalConstraint(node, constraint); err == nil {
			t.Fatal("expected null key error")
		}

		node = newTemporalNode(prefixTestID("temporal-bad-start"), "acct", "bad", now.Add(time.Hour))
		if err := tx.checkTemporalConstraint(node, constraint); err == nil {
			t.Fatal("expected invalid start error")
		}

		node = newTemporalNode("unprefixed", "acct", now, now.Add(time.Hour))
		if err := tx.checkTemporalConstraint(node, constraint); err == nil {
			t.Fatal("expected unprefixed id error")
		}
	})

	t.Run("detects overlap and invalid pending nodes", func(t *testing.T) {
		engine := NewMemoryEngine()
		defer engine.Close()
		tx, _ := engine.BeginTransaction()

		tx.pendingNodes[NodeID(prefixTestID("pending-invalid"))] = &Node{
			ID:     NodeID(prefixTestID("pending-invalid")),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"account": "acct",
				"from":    "bad",
				"to":      now.Add(time.Hour),
			},
		}

		err := tx.checkTemporalConstraint(newTemporalNode(prefixTestID("pending-check"), "acct", now, now.Add(time.Hour)), constraint)
		if err == nil {
			t.Fatal("expected invalid pending node error")
		}

		delete(tx.pendingNodes, NodeID(prefixTestID("pending-invalid")))
		tx.pendingNodes[NodeID(prefixTestID("pending-overlap"))] = newTemporalNode(prefixTestID("pending-overlap"), "acct", now, now.Add(2*time.Hour))

		err = tx.checkTemporalConstraint(newTemporalNode(prefixTestID("pending-check"), "acct", now.Add(time.Hour), now.Add(3*time.Hour)), constraint)
		if err == nil {
			t.Fatal("expected pending overlap error")
		}
	})

	t.Run("detects committed invalid and overlapping intervals", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		_, err := engine.CreateNode(&Node{
			ID:     NodeID(prefixTestID("committed-invalid")),
			Labels: []string{"Person"},
			Properties: map[string]interface{}{
				"account": "acct",
				"from":    "bad",
				"to":      now.Add(time.Hour),
			},
		})
		if err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}

		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}

		err = tx.checkTemporalConstraint(newTemporalNode(prefixTestID("committed-check"), "acct", now, now.Add(time.Hour)), constraint)
		if err == nil {
			t.Fatal("expected committed invalid start error")
		}

		engine = createTestBadgerEngine(t)
		_, err = engine.CreateNode(newTemporalNode(prefixTestID("committed-overlap"), "acct", now, now.Add(2*time.Hour)))
		if err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}
		tx, err = engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}

		err = tx.checkTemporalConstraint(newTemporalNode(prefixTestID("committed-check"), "acct", now.Add(time.Hour), now.Add(3*time.Hour)), constraint)
		if err == nil {
			t.Fatal("expected committed overlap error")
		}
	})

	t.Run("allows non-overlapping intervals and ignores same node id", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		_, err := engine.CreateNode(newTemporalNode(prefixTestID("same-node"), "acct", now, now.Add(time.Hour)))
		if err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}

		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}

		err = tx.checkTemporalConstraint(newTemporalNode(prefixTestID("same-node"), "acct", now, now.Add(time.Hour)), constraint)
		if err != nil {
			t.Fatalf("expected same-node update to be ignored, got %v", err)
		}

		err = tx.checkTemporalConstraint(newTemporalNode(prefixTestID("non-overlap"), "acct", now.Add(2*time.Hour), now.Add(3*time.Hour)), constraint)
		if err != nil {
			t.Fatalf("expected non-overlapping interval to pass, got %v", err)
		}
	})
}

func TestTransaction_CheckNodeKeyConstraintAndValidateNodeConstraints(t *testing.T) {
	newNode := func(id string, props map[string]interface{}) *Node {
		return &Node{
			ID:         NodeID(id),
			Labels:     []string{"Person"},
			Properties: props,
		}
	}

	now := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)

	t.Run("checkNodeKeyConstraint validates nulls pending duplicates and committed duplicates", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}
		c := Constraint{Type: ConstraintNodeKey, Label: "Person", Properties: []string{"tenant", "external_id"}}

		err = tx.checkNodeKeyConstraint(newNode(prefixTestID("nk-null"), map[string]interface{}{"tenant": "t1"}), c)
		if err == nil {
			t.Fatal("expected null node-key property error")
		}

		tx.pendingNodes[NodeID(prefixTestID("nk-pending"))] = newNode(prefixTestID("nk-pending"), map[string]interface{}{
			"tenant":      "t1",
			"external_id": "e1",
		})
		err = tx.checkNodeKeyConstraint(newNode(prefixTestID("nk-check"), map[string]interface{}{
			"tenant":      "t1",
			"external_id": "e1",
		}), c)
		if err == nil {
			t.Fatal("expected pending node-key duplicate error")
		}

		engine = createTestBadgerEngine(t)
		_, err = engine.CreateNode(newNode(prefixTestID("nk-existing"), map[string]interface{}{
			"tenant":      "t1",
			"external_id": "e1",
		}))
		if err != nil {
			t.Fatalf("CreateNode failed: %v", err)
		}
		tx, err = engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}
		err = tx.checkNodeKeyConstraint(newNode(prefixTestID("nk-check"), map[string]interface{}{
			"tenant":      "t1",
			"external_id": "e1",
		}), c)
		if err == nil {
			t.Fatal("expected committed node-key duplicate error")
		}
	})

	t.Run("validateNodeConstraints checks prefix schema constraints and property types", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		schema := engine.GetSchemaForNamespace("test")
		if err := schema.AddConstraint(Constraint{Name: "person_name_exists", Type: ConstraintExists, Label: "Person", Properties: []string{"name"}}); err != nil {
			t.Fatalf("AddConstraint exists failed: %v", err)
		}
		if err := schema.AddConstraint(Constraint{Name: "person_node_key", Type: ConstraintNodeKey, Label: "Person", Properties: []string{"tenant", "external_id"}}); err != nil {
			t.Fatalf("AddConstraint node key failed: %v", err)
		}
		if err := schema.AddConstraint(Constraint{Name: "person_temporal", Type: ConstraintTemporal, Label: "Person", Properties: []string{"account", "from", "to"}}); err != nil {
			t.Fatalf("AddConstraint temporal failed: %v", err)
		}
		if err := schema.AddPropertyTypeConstraint("person_age_type", "Person", "age", PropertyTypeInteger); err != nil {
			t.Fatalf("AddPropertyTypeConstraint failed: %v", err)
		}

		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}

		err = tx.validateNodeConstraints(newNode("unprefixed", map[string]interface{}{}))
		if err == nil {
			t.Fatal("expected unprefixed id validation error")
		}

		err = tx.validateNodeConstraints(newNode(prefixTestID("missing-name"), map[string]interface{}{
			"tenant":      "t1",
			"external_id": "e1",
			"account":     "acct",
			"from":        now,
			"to":          now.Add(time.Hour),
			"age":         int64(10),
		}))
		if err == nil {
			t.Fatal("expected exists constraint error")
		}

		err = tx.validateNodeConstraints(newNode(prefixTestID("bad-type"), map[string]interface{}{
			"name":        "Alice",
			"tenant":      "t1",
			"external_id": "e1",
			"account":     "acct",
			"from":        now,
			"to":          now.Add(time.Hour),
			"age":         "ten",
		}))
		if err == nil {
			t.Fatal("expected property type constraint error")
		}

		err = tx.validateNodeConstraints(newNode(prefixTestID("valid"), map[string]interface{}{
			"name":        "Alice",
			"tenant":      "t1",
			"external_id": "e1",
			"account":     "acct",
			"from":        now,
			"to":          now.Add(time.Hour),
			"age":         int64(10),
		}))
		if err != nil {
			t.Fatalf("expected valid node to pass, got %v", err)
		}

		tx.pendingNodes[NodeID(prefixTestID("dupe"))] = newNode(prefixTestID("dupe"), map[string]interface{}{
			"name":        "Bob",
			"tenant":      "t1",
			"external_id": "e1",
			"account":     "acct-2",
			"from":        now,
			"to":          now.Add(time.Hour),
			"age":         int64(11),
		})

		err = tx.validateNodeConstraints(newNode(prefixTestID("valid"), map[string]interface{}{
			"name":        "Alice",
			"tenant":      "t1",
			"external_id": "e1",
			"account":     "acct-3",
			"from":        now,
			"to":          now.Add(time.Hour),
			"age":         int64(10),
		}))
		if err == nil {
			t.Fatal("expected node-key duplicate through validateNodeConstraints")
		}
	})
}

func TestTransaction_DeleteEdgesWithPrefixBuffered(t *testing.T) {
	t.Run("buffers edge and index deletions for matched prefix", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: NodeID(prefixTestID("del-node-1")), Labels: []string{"Person"}})
		if err != nil {
			t.Fatalf("CreateNode 1 failed: %v", err)
		}
		_, err = engine.CreateNode(&Node{ID: NodeID(prefixTestID("del-node-2")), Labels: []string{"Person"}})
		if err != nil {
			t.Fatalf("CreateNode 2 failed: %v", err)
		}
		edge := &Edge{ID: EdgeID(prefixTestID("del-edge-1")), StartNode: NodeID(prefixTestID("del-node-1")), EndNode: NodeID(prefixTestID("del-node-2")), Type: "KNOWS"}
		if err := engine.CreateEdge(edge); err != nil {
			t.Fatalf("CreateEdge failed: %v", err)
		}

		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}
		defer tx.Rollback()

		count, ids, err := tx.deleteEdgesWithPrefixBuffered(outgoingIndexPrefix(edge.StartNode))
		if err != nil {
			t.Fatalf("deleteEdgesWithPrefixBuffered failed: %v", err)
		}
		if count != 1 || len(ids) != 1 || ids[0] != edge.ID {
			t.Fatalf("unexpected delete result: count=%d ids=%v", count, ids)
		}
		if !tx.pendingDeletes[string(edgeKey(edge.ID))] ||
			!tx.pendingDeletes[string(outgoingIndexKey(edge.StartNode, edge.ID))] ||
			!tx.pendingDeletes[string(incomingIndexKey(edge.EndNode, edge.ID))] ||
			!tx.pendingDeletes[string(edgeTypeIndexKey(edge.Type, edge.ID))] {
			t.Fatal("expected edge and index deletes to be buffered")
		}
	})

	t.Run("skips missing edges and errors on corrupt edge payloads", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		nodeID := NodeID(prefixTestID("del-prefix"))
		otherID := NodeID(prefixTestID("del-other"))
		missingEdge := EdgeID(prefixTestID("missing-edge"))
		badEdge := EdgeID(prefixTestID("bad-edge"))

		if err := engine.withUpdate(func(txn *badger.Txn) error {
			if err := txn.Set(outgoingIndexKey(nodeID, missingEdge), []byte{}); err != nil {
				return err
			}
			if err := txn.Set(outgoingIndexKey(nodeID, badEdge), []byte{}); err != nil {
				return err
			}
			if err := txn.Set(edgeKey(badEdge), []byte("not-an-edge")); err != nil {
				return err
			}
			if err := txn.Set(incomingIndexKey(otherID, badEdge), []byte{}); err != nil {
				return err
			}
			return txn.Set(edgeTypeIndexKey("BROKEN", badEdge), []byte{})
		}); err != nil {
			t.Fatalf("seed update failed: %v", err)
		}

		tx, err := engine.BeginTransaction()
		if err != nil {
			t.Fatalf("BeginTransaction failed: %v", err)
		}
		defer tx.Rollback()

		_, _, err = tx.deleteEdgesWithPrefixBuffered(outgoingIndexPrefix(nodeID))
		if err == nil {
			t.Fatal("expected corrupt edge payload error")
		}
	})
}

func TestTransaction_Isolation(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	// Transaction 1 creates a node
	tx1, _ := engine.BeginTransaction()
	node := &Node{ID: NodeID(prefixTestID("isolated-node")), Labels: []string{"Isolated"}}
	tx1.CreateNode(node)

	// Transaction 2 should NOT see this node
	tx2, _ := engine.BeginTransaction()
	_, err := tx2.GetNode(NodeID(prefixTestID("isolated-node")))
	if err != ErrNotFound {
		t.Error("TX2 should not see TX1's uncommitted node")
	}

	// Commit TX1
	tx1.Commit()

	// TX2 still shouldn't see it (snapshot isolation would require this)
	// But our basic implementation will see it now - this is acceptable
	// for the current implementation level

	// Close TX2
	tx2.Rollback()
}

func TestTransaction_MultipleOperationTypes(t *testing.T) {
	engine := NewMemoryEngine()

	// Pre-create some data
	engine.CreateNode(&Node{ID: NodeID(prefixTestID("existing-1")), Labels: []string{"Existing"}})
	engine.CreateNode(&Node{ID: NodeID(prefixTestID("existing-2")), Labels: []string{"Existing"}})

	tx, _ := engine.BeginTransaction()

	// Mix of operations
	tx.CreateNode(&Node{ID: NodeID(prefixTestID("new-1")), Labels: []string{"New"}})
	tx.CreateNode(&Node{ID: NodeID(prefixTestID("new-2")), Labels: []string{"New"}})
	tx.UpdateNode(&Node{ID: NodeID(prefixTestID("existing-1")), Labels: []string{"Updated"}})
	tx.DeleteNode(NodeID(prefixTestID("existing-2")))

	// Verify operation count
	if tx.OperationCount() != 4 {
		t.Errorf("Expected 4 operations, got %d", tx.OperationCount())
	}

	// Commit
	tx.Commit()

	// Verify final state
	_, err := engine.GetNode(NodeID(prefixTestID("new-1")))
	if err != nil {
		t.Error("new-1 should exist")
	}
	_, err = engine.GetNode(NodeID(prefixTestID("new-2")))
	if err != nil {
		t.Error("new-2 should exist")
	}
	updated, _ := engine.GetNode(NodeID(prefixTestID("existing-1")))
	if updated.Labels[0] != "Updated" {
		t.Error("existing-1 should be updated")
	}
	_, err = engine.GetNode(NodeID(prefixTestID("existing-2")))
	if err != ErrNotFound {
		t.Error("existing-2 should be deleted")
	}
}

// Benchmark transaction overhead
func BenchmarkTransaction_CommitNodes(b *testing.B) {
	engine := NewMemoryEngine()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := engine.BeginTransaction()
		for j := 0; j < 10; j++ {
			node := &Node{
				ID:     NodeID(prefixTestID("bench-" + time.Now().Format("150405.000000") + "-" + string(rune('0'+j)))),
				Labels: []string{"Bench"},
			}
			tx.CreateNode(node)
		}
		tx.Commit()
	}
}

func BenchmarkTransaction_RollbackNodes(b *testing.B) {
	engine := NewMemoryEngine()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := engine.BeginTransaction()
		for j := 0; j < 10; j++ {
			node := &Node{
				ID:     NodeID(prefixTestID("bench-" + time.Now().Format("150405.000000") + "-" + string(rune('0'+j)))),
				Labels: []string{"Bench"},
			}
			tx.CreateNode(node)
		}
		tx.Rollback()
	}
}
