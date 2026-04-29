package cypher

import (
	"log"
	"os"
	"strings"
	"time"
)

const unwindMergeChainProfileEnv = "NORNICDB_PCG_CHAIN_PROFILE"

// unwindMergeChainBatchProfile records one compact phase split for PCG-style
// UNWIND/MATCH/MERGE relationship batches when explicitly enabled.
type unwindMergeChainBatchProfile struct {
	enabled bool
	start   time.Time

	inputRows     int
	processedRows int
	skippedRows   int

	nodeLookups      int
	nodeLookupHits   int
	nodeLookupMisses int
	relLookups       int
	relLookupHits    int
	relLookupMisses  int

	nodeCreates         int
	nodeUpdates         int
	relationshipUpdates int
	bulkCreateCalls     int
	bulkCreateRows      int

	nodeLookupDuration time.Duration
	relLookupDuration  time.Duration
	nodeCreateDuration time.Duration
	nodeUpdateDuration time.Duration
	edgeUpdateDuration time.Duration
	bulkCreateDuration time.Duration
}

func newUnwindMergeChainBatchProfile(inputRows int) *unwindMergeChainBatchProfile {
	enabled := strings.TrimSpace(os.Getenv(unwindMergeChainProfileEnv)) != ""
	profile := &unwindMergeChainBatchProfile{
		enabled:   enabled,
		inputRows: inputRows,
	}
	if enabled {
		profile.start = time.Now()
	}
	return profile
}

func (p *unwindMergeChainBatchProfile) timerStart() time.Time {
	if !p.enabled {
		return time.Time{}
	}
	return time.Now()
}

func (p *unwindMergeChainBatchProfile) elapsed(start time.Time) time.Duration {
	if !p.enabled {
		return 0
	}
	return time.Since(start)
}

func (p *unwindMergeChainBatchProfile) recordNodeLookup(duration time.Duration, hit bool) {
	if !p.enabled {
		return
	}
	p.nodeLookups++
	p.nodeLookupDuration += duration
	if hit {
		p.nodeLookupHits++
		return
	}
	p.nodeLookupMisses++
}

func (p *unwindMergeChainBatchProfile) recordRelationshipLookup(duration time.Duration, hit bool) {
	if !p.enabled {
		return
	}
	p.relLookups++
	p.relLookupDuration += duration
	if hit {
		p.relLookupHits++
		return
	}
	p.relLookupMisses++
}

func (p *unwindMergeChainBatchProfile) addNodeCreate(duration time.Duration) {
	if !p.enabled {
		return
	}
	p.nodeCreates++
	p.nodeCreateDuration += duration
}

func (p *unwindMergeChainBatchProfile) addNodeUpdate(duration time.Duration) {
	if !p.enabled {
		return
	}
	p.nodeUpdates++
	p.nodeUpdateDuration += duration
}

func (p *unwindMergeChainBatchProfile) addEdgeUpdate(duration time.Duration) {
	if !p.enabled {
		return
	}
	p.relationshipUpdates++
	p.edgeUpdateDuration += duration
}

func (p *unwindMergeChainBatchProfile) addBulkCreate(rows int, duration time.Duration) {
	if !p.enabled {
		return
	}
	p.bulkCreateCalls++
	p.bulkCreateRows += rows
	p.bulkCreateDuration += duration
}

func (p *unwindMergeChainBatchProfile) finish(processedRows int, skippedRows int) {
	if !p.enabled {
		return
	}
	p.processedRows = processedRows
	p.skippedRows = skippedRows
	log.Printf(
		"pcg_unwind_merge_chain_profile input_rows=%d processed_rows=%d skipped_rows=%d total_ms=%.3f node_lookups=%d node_lookup_hits=%d node_lookup_misses=%d node_lookup_ms=%.3f rel_lookups=%d rel_lookup_hits=%d rel_lookup_misses=%d rel_lookup_ms=%.3f node_creates=%d node_create_ms=%.3f node_updates=%d node_update_ms=%.3f relationship_updates=%d edge_update_ms=%.3f bulk_create_calls=%d bulk_create_rows=%d bulk_create_ms=%.3f",
		p.inputRows,
		p.processedRows,
		p.skippedRows,
		float64(time.Since(p.start).Microseconds())/1000.0,
		p.nodeLookups,
		p.nodeLookupHits,
		p.nodeLookupMisses,
		float64(p.nodeLookupDuration.Microseconds())/1000.0,
		p.relLookups,
		p.relLookupHits,
		p.relLookupMisses,
		float64(p.relLookupDuration.Microseconds())/1000.0,
		p.nodeCreates,
		float64(p.nodeCreateDuration.Microseconds())/1000.0,
		p.nodeUpdates,
		float64(p.nodeUpdateDuration.Microseconds())/1000.0,
		p.relationshipUpdates,
		float64(p.edgeUpdateDuration.Microseconds())/1000.0,
		p.bulkCreateCalls,
		p.bulkCreateRows,
		float64(p.bulkCreateDuration.Microseconds())/1000.0,
	)
}
