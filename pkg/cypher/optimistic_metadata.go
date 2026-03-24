package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

type optimisticMutationMeta struct {
	CreatedNodeIDs         []string `json:"createdNodeIds,omitempty"`
	CreatedRelationshipIDs []string `json:"createdRelationshipIds,omitempty"`
}

func addOptimisticNodeID(result *ExecuteResult, id storage.NodeID) {
	if result == nil || id == "" {
		return
	}
	meta := ensureOptimisticMeta(result)
	idStr := string(id)
	for _, existing := range meta.CreatedNodeIDs {
		if existing == idStr {
			return
		}
	}
	meta.CreatedNodeIDs = append(meta.CreatedNodeIDs, idStr)
}

func addOptimisticRelationshipID(result *ExecuteResult, id storage.EdgeID) {
	if result == nil || id == "" {
		return
	}
	meta := ensureOptimisticMeta(result)
	idStr := string(id)
	for _, existing := range meta.CreatedRelationshipIDs {
		if existing == idStr {
			return
		}
	}
	meta.CreatedRelationshipIDs = append(meta.CreatedRelationshipIDs, idStr)
}

func ensureOptimisticMeta(result *ExecuteResult) *optimisticMutationMeta {
	if result.Metadata == nil {
		result.Metadata = make(map[string]interface{})
	}
	if raw, ok := result.Metadata["optimistic"]; ok {
		if typed, ok := raw.(*optimisticMutationMeta); ok && typed != nil {
			return typed
		}
		if typed, ok := raw.(optimisticMutationMeta); ok {
			copyVal := typed
			result.Metadata["optimistic"] = &copyVal
			return &copyVal
		}
	}
	meta := &optimisticMutationMeta{}
	result.Metadata["optimistic"] = meta
	return meta
}
