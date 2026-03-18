package cypher

// HotPathTrace records which key query hot paths were used for the most recent Execute call.
type HotPathTrace struct {
	OuterIndexTopK         bool
	OuterScanFallbackUsed  bool
	FabricBatchedApplyRows bool
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

// LastHotPathTrace returns a snapshot of the latest per-query hot path trace.
func (e *StorageExecutor) LastHotPathTrace() HotPathTrace {
	if e.hotPathTraceState == nil {
		e.hotPathTraceState = &hotPathTraceState{}
	}
	e.hotPathTraceState.mu.RLock()
	defer e.hotPathTraceState.mu.RUnlock()
	return e.hotPathTraceState.trace
}
