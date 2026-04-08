package cypher

// HotPathTrace records which key query hot paths were used for the most recent Execute call.
type HotPathTrace struct {
	OuterIndexTopK            bool
	OuterScanFallbackUsed     bool
	FabricBatchedApplyRows    bool
	SimpleMatchLimitFastPath  bool
	TraversalEndSeedTopK      bool
	UnwindSimpleMergeBatch    bool
	UnwindFixedChainLinkBatch bool
	CallTailTraversalFastPath bool
	MergeSchemaLookupUsed     bool
	MergeScanFallbackUsed     bool
}

func (e *StorageExecutor) resetHotPathTrace() {
	if e.hotPathTraceState == nil {
		e.hotPathTraceState = &hotPathTraceState{}
	}
	e.hotPathTraceState.mu.Lock()
	e.hotPathTraceState.trace = HotPathTrace{}
	e.hotPathTraceState.mu.Unlock()
}

func (e *StorageExecutor) markOuterIndexTopKUsed() {
	if e.hotPathTraceState == nil {
		e.hotPathTraceState = &hotPathTraceState{}
	}
	e.hotPathTraceState.mu.Lock()
	e.hotPathTraceState.trace.OuterIndexTopK = true
	e.hotPathTraceState.mu.Unlock()
}

func (e *StorageExecutor) markOuterScanFallbackUsed() {
	if e.hotPathTraceState == nil {
		e.hotPathTraceState = &hotPathTraceState{}
	}
	e.hotPathTraceState.mu.Lock()
	e.hotPathTraceState.trace.OuterScanFallbackUsed = true
	e.hotPathTraceState.mu.Unlock()
}

func (e *StorageExecutor) setFabricBatchedApplyRowsUsed(v bool) {
	if !v {
		return
	}
	if e.hotPathTraceState == nil {
		e.hotPathTraceState = &hotPathTraceState{}
	}
	e.hotPathTraceState.mu.Lock()
	e.hotPathTraceState.trace.FabricBatchedApplyRows = true
	e.hotPathTraceState.mu.Unlock()
}

func (e *StorageExecutor) markSimpleMatchLimitFastPathUsed() {
	if e.hotPathTraceState == nil {
		e.hotPathTraceState = &hotPathTraceState{}
	}
	e.hotPathTraceState.mu.Lock()
	e.hotPathTraceState.trace.SimpleMatchLimitFastPath = true
	e.hotPathTraceState.mu.Unlock()
}

func (e *StorageExecutor) markTraversalEndSeedTopKUsed() {
	if e.hotPathTraceState == nil {
		e.hotPathTraceState = &hotPathTraceState{}
	}
	e.hotPathTraceState.mu.Lock()
	e.hotPathTraceState.trace.TraversalEndSeedTopK = true
	e.hotPathTraceState.mu.Unlock()
}

func (e *StorageExecutor) markUnwindSimpleMergeBatchUsed() {
	if e.hotPathTraceState == nil {
		e.hotPathTraceState = &hotPathTraceState{}
	}
	e.hotPathTraceState.mu.Lock()
	e.hotPathTraceState.trace.UnwindSimpleMergeBatch = true
	e.hotPathTraceState.mu.Unlock()
}

func (e *StorageExecutor) markUnwindFixedChainLinkBatchUsed() {
	if e.hotPathTraceState == nil {
		e.hotPathTraceState = &hotPathTraceState{}
	}
	e.hotPathTraceState.mu.Lock()
	e.hotPathTraceState.trace.UnwindFixedChainLinkBatch = true
	e.hotPathTraceState.mu.Unlock()
}

func (e *StorageExecutor) markCallTailTraversalFastPathUsed() {
	if e.hotPathTraceState == nil {
		e.hotPathTraceState = &hotPathTraceState{}
	}
	e.hotPathTraceState.mu.Lock()
	e.hotPathTraceState.trace.CallTailTraversalFastPath = true
	e.hotPathTraceState.mu.Unlock()
}

func (e *StorageExecutor) markMergeSchemaLookupUsed() {
	if e.hotPathTraceState == nil {
		e.hotPathTraceState = &hotPathTraceState{}
	}
	e.hotPathTraceState.mu.Lock()
	e.hotPathTraceState.trace.MergeSchemaLookupUsed = true
	e.hotPathTraceState.mu.Unlock()
}

func (e *StorageExecutor) markMergeScanFallbackUsed() {
	if e.hotPathTraceState == nil {
		e.hotPathTraceState = &hotPathTraceState{}
	}
	e.hotPathTraceState.mu.Lock()
	e.hotPathTraceState.trace.MergeScanFallbackUsed = true
	e.hotPathTraceState.mu.Unlock()
}

// LastHotPathTrace returns a snapshot of the latest per-query hot path trace.
func (e *StorageExecutor) LastHotPathTrace() HotPathTrace {
	if e.hotPathTraceState == nil {
		e.hotPathTraceState = &hotPathTraceState{}
	}
	e.hotPathTraceState.mu.RLock()
	defer e.hotPathTraceState.mu.RUnlock()
	return e.hotPathTraceState.trace
}
