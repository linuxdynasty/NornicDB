package multidb

import (
	"fmt"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// sizeTrackingEngine wraps a database-scoped engine and maintains cached storage-size
// bytes in DatabaseManager metadata for O(1) reads.
type sizeTrackingEngine struct {
	storage.Engine
	manager *DatabaseManager
	dbName  string
}

var _ storage.Engine = (*sizeTrackingEngine)(nil)

func newSizeTrackingEngine(engine storage.Engine, manager *DatabaseManager, dbName string) storage.Engine {
	return &sizeTrackingEngine{
		Engine:  engine,
		manager: manager,
		dbName:  dbName,
	}
}

func (t *sizeTrackingEngine) GetInnerEngine() storage.Engine {
	return t.Engine
}

func (t *sizeTrackingEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if err := t.manager.ensureStorageSizeInitialized(t.dbName, t.Engine); err != nil {
		return "", err
	}
	id, err := t.Engine.CreateNode(node)
	if err != nil {
		return id, err
	}
	created, getErr := t.Engine.GetNode(id)
	if getErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return id, nil
	}
	size, sizeErr := calculateNodeSize(created)
	if sizeErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return id, nil
	}
	t.manager.applyStorageSizeDelta(t.dbName, size, 0)
	return id, nil
}

func (t *sizeTrackingEngine) UpdateNode(node *storage.Node) error {
	if err := t.manager.ensureStorageSizeInitialized(t.dbName, t.Engine); err != nil {
		return err
	}
	existing, getErr := t.Engine.GetNode(node.ID)
	if getErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return t.Engine.UpdateNode(node)
	}
	oldSize, oldErr := calculateNodeSize(existing)
	newSize, newErr := calculateNodeSize(node)
	if oldErr != nil || newErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return t.Engine.UpdateNode(node)
	}
	if err := t.Engine.UpdateNode(node); err != nil {
		return err
	}
	t.manager.applyStorageSizeDelta(t.dbName, newSize-oldSize, 0)
	return nil
}

func (t *sizeTrackingEngine) DeleteNode(id storage.NodeID) error {
	if err := t.manager.ensureStorageSizeInitialized(t.dbName, t.Engine); err != nil {
		return err
	}
	existing, getErr := t.Engine.GetNode(id)
	if getErr != nil {
		// Nothing to track if the node doesn't exist.
		return t.Engine.DeleteNode(id)
	}
	nodeSize, sizeErr := calculateNodeSize(existing)
	if sizeErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return t.Engine.DeleteNode(id)
	}
	edgeDelta, edgeErr := t.connectedEdgeBytes(id)
	if edgeErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
	}
	if err := t.Engine.DeleteNode(id); err != nil {
		return err
	}
	t.manager.applyStorageSizeDelta(t.dbName, -nodeSize, -edgeDelta)
	return nil
}

func (t *sizeTrackingEngine) CreateEdge(edge *storage.Edge) error {
	if err := t.manager.ensureStorageSizeInitialized(t.dbName, t.Engine); err != nil {
		return err
	}
	if err := t.Engine.CreateEdge(edge); err != nil {
		return err
	}
	size, sizeErr := calculateEdgeSize(edge)
	if sizeErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return nil
	}
	t.manager.applyStorageSizeDelta(t.dbName, 0, size)
	return nil
}

func (t *sizeTrackingEngine) UpdateEdge(edge *storage.Edge) error {
	if err := t.manager.ensureStorageSizeInitialized(t.dbName, t.Engine); err != nil {
		return err
	}
	existing, getErr := t.Engine.GetEdge(edge.ID)
	if getErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return t.Engine.UpdateEdge(edge)
	}
	oldSize, oldErr := calculateEdgeSize(existing)
	newSize, newErr := calculateEdgeSize(edge)
	if oldErr != nil || newErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return t.Engine.UpdateEdge(edge)
	}
	if err := t.Engine.UpdateEdge(edge); err != nil {
		return err
	}
	t.manager.applyStorageSizeDelta(t.dbName, 0, newSize-oldSize)
	return nil
}

func (t *sizeTrackingEngine) DeleteEdge(id storage.EdgeID) error {
	if err := t.manager.ensureStorageSizeInitialized(t.dbName, t.Engine); err != nil {
		return err
	}
	existing, getErr := t.Engine.GetEdge(id)
	if getErr != nil {
		return t.Engine.DeleteEdge(id)
	}
	size, sizeErr := calculateEdgeSize(existing)
	if sizeErr != nil {
		t.manager.markStorageSizeDirty(t.dbName)
		return t.Engine.DeleteEdge(id)
	}
	if err := t.Engine.DeleteEdge(id); err != nil {
		return err
	}
	t.manager.applyStorageSizeDelta(t.dbName, 0, -size)
	return nil
}

func (t *sizeTrackingEngine) BulkCreateNodes(nodes []*storage.Node) error {
	for _, n := range nodes {
		if _, err := t.CreateNode(n); err != nil {
			return err
		}
	}
	return nil
}

func (t *sizeTrackingEngine) BulkCreateEdges(edges []*storage.Edge) error {
	for _, e := range edges {
		if err := t.CreateEdge(e); err != nil {
			return err
		}
	}
	return nil
}

func (t *sizeTrackingEngine) BulkDeleteNodes(ids []storage.NodeID) error {
	for _, id := range ids {
		if err := t.DeleteNode(id); err != nil {
			return err
		}
	}
	return nil
}

func (t *sizeTrackingEngine) BulkDeleteEdges(ids []storage.EdgeID) error {
	for _, id := range ids {
		if err := t.DeleteEdge(id); err != nil {
			return err
		}
	}
	return nil
}

func (t *sizeTrackingEngine) connectedEdgeBytes(id storage.NodeID) (int64, error) {
	outgoing, err := t.Engine.GetOutgoingEdges(id)
	if err != nil {
		return 0, fmt.Errorf("get outgoing edges: %w", err)
	}
	incoming, err := t.Engine.GetIncomingEdges(id)
	if err != nil {
		return 0, fmt.Errorf("get incoming edges: %w", err)
	}
	seen := make(map[storage.EdgeID]struct{}, len(outgoing)+len(incoming))
	var total int64
	for _, e := range outgoing {
		if _, ok := seen[e.ID]; ok {
			continue
		}
		seen[e.ID] = struct{}{}
		size, sizeErr := calculateEdgeSize(e)
		if sizeErr != nil {
			return 0, sizeErr
		}
		total += size
	}
	for _, e := range incoming {
		if _, ok := seen[e.ID]; ok {
			continue
		}
		seen[e.ID] = struct{}{}
		size, sizeErr := calculateEdgeSize(e)
		if sizeErr != nil {
			return 0, sizeErr
		}
		total += size
	}
	return total, nil
}
