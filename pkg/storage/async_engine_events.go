package storage

import "time"

// ListNamespaces returns known namespaces from the wrapped engine, if supported.
func (ae *AsyncEngine) ListNamespaces() []string {
	if lister, ok := ae.engine.(NamespaceLister); ok {
		return lister.ListNamespaces()
	}
	return nil
}

// OnNodeCreated sets a callback to be invoked when nodes are created.
func (ae *AsyncEngine) OnNodeCreated(callback NodeEventCallback) {
	ae.callbackMu.Lock()
	defer ae.callbackMu.Unlock()
	ae.onNodeCreated = callback
}

// OnNodeUpdated sets a callback to be invoked when nodes are updated.
func (ae *AsyncEngine) OnNodeUpdated(callback NodeEventCallback) {
	ae.callbackMu.Lock()
	defer ae.callbackMu.Unlock()
	ae.onNodeUpdated = callback
}

// OnNodeDeleted sets a callback to be invoked when nodes are deleted.
func (ae *AsyncEngine) OnNodeDeleted(callback NodeDeleteCallback) {
	ae.callbackMu.Lock()
	defer ae.callbackMu.Unlock()
	ae.onNodeDeleted = callback
}

// OnEdgeCreated sets a callback to be invoked when edges are created.
func (ae *AsyncEngine) OnEdgeCreated(callback EdgeEventCallback) {
	ae.callbackMu.Lock()
	defer ae.callbackMu.Unlock()
	ae.onEdgeCreated = callback
}

// OnEdgeUpdated sets a callback to be invoked when edges are updated.
func (ae *AsyncEngine) OnEdgeUpdated(callback EdgeEventCallback) {
	ae.callbackMu.Lock()
	defer ae.callbackMu.Unlock()
	ae.onEdgeUpdated = callback
}

// OnEdgeDeleted sets a callback to be invoked when edges are deleted.
func (ae *AsyncEngine) OnEdgeDeleted(callback EdgeDeleteCallback) {
	ae.callbackMu.Lock()
	defer ae.callbackMu.Unlock()
	ae.onEdgeDeleted = callback
}

func (ae *AsyncEngine) notifyNodeDeleted(nodeID NodeID) {
	ae.callbackMu.RLock()
	callback := ae.onNodeDeleted
	ae.callbackMu.RUnlock()
	if callback != nil {
		callback(nodeID)
	}
}

// DefaultAsyncEngineConfig returns sensible defaults.
func DefaultAsyncEngineConfig() *AsyncEngineConfig {
	return &AsyncEngineConfig{
		FlushInterval:    50 * time.Millisecond,
		MaxNodeCacheSize: 50000,  // 50K nodes (~35MB)
		MaxEdgeCacheSize: 100000, // 100K edges (~50MB)
		AdaptiveFlush:    true,
		MinFlushInterval: 10 * time.Millisecond,
		MaxFlushInterval: 200 * time.Millisecond,
		TargetFlushSize:  1000,
	}
}

// GetUnderlying returns the underlying storage engine.
// This is used for transaction support when the underlying engine
// supports ACID transactions (e.g., BadgerEngine).
func (ae *AsyncEngine) GetUnderlying() Engine {
	return ae.engine
}

// Stats returns async engine statistics.
func (ae *AsyncEngine) Stats() (pendingWrites, totalFlushes int64) {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	return ae.pendingWrites, ae.totalFlushes
}

// HasPendingWrites returns true if there are unflushed writes.
// This is a cheap check that can be used to avoid unnecessary flush calls.
func (ae *AsyncEngine) HasPendingWrites() bool {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	return len(ae.nodeCache) > 0 || len(ae.edgeCache) > 0 ||
		len(ae.deleteNodes) > 0 || len(ae.deleteEdges) > 0
}

func (ae *AsyncEngine) pendingWriteCount() int {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	return len(ae.nodeCache) + len(ae.edgeCache) + len(ae.deleteNodes) + len(ae.deleteEdges)
}

func (ae *AsyncEngine) adaptiveFlushInterval(pending int) time.Duration {
	if pending <= 0 || ae.targetFlushSize <= 0 {
		return ae.maxFlushInterval
	}
	if ae.maxFlushInterval <= ae.minFlushInterval {
		return ae.minFlushInterval
	}
	ratio := float64(pending) / float64(ae.targetFlushSize)
	if ratio > 1 {
		ratio = 1
	}
	span := ae.maxFlushInterval - ae.minFlushInterval
	return ae.minFlushInterval + time.Duration(ratio*float64(span))
}
