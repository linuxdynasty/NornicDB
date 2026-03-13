package cypher

import (
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// parseLeadingUseClause extracts a leading `USE <database>` clause from a query fragment.
// It returns the selected database, remaining query, and whether a USE clause was found.
func parseLeadingUseClause(cypher string) (database, remaining string, hasUse bool, err error) {
	trimmed := strings.TrimSpace(cypher)
	if !startsWithKeywordFold(trimmed, "USE") {
		return "", cypher, false, nil
	}

	rest := strings.TrimSpace(trimmed[len("USE"):])
	if rest == "" {
		return "", "", true, fmt.Errorf("USE clause requires a database name")
	}

	if strings.HasPrefix(rest, "`") {
		// Backtick-quoted identifier. Support escaped backticks using ``.
		var b strings.Builder
		escaped := false
		for i := 1; i < len(rest); i++ {
			ch := rest[i]
			if ch == '`' {
				if i+1 < len(rest) && rest[i+1] == '`' {
					b.WriteByte('`')
					i++
					continue
				}
				database = b.String()
				remaining = strings.TrimSpace(rest[i+1:])
				return database, remaining, true, nil
			}
			if escaped {
				escaped = false
			}
			b.WriteByte(ch)
		}
		return "", "", true, fmt.Errorf("invalid USE clause: unterminated backtick identifier")
	}

	parts := strings.Fields(rest)
	if len(parts) == 0 {
		return "", "", true, fmt.Errorf("USE clause requires a database name")
	}

	database = parts[0]
	if len(parts) > 1 {
		remaining = strings.TrimSpace(strings.TrimPrefix(rest, database))
	}

	return database, remaining, true, nil
}

func (e *StorageExecutor) cloneForStorage(store storage.Engine) *StorageExecutor {
	cloned := NewStorageExecutor(store)
	cloned.deferFlush = e.deferFlush
	cloned.embedder = e.embedder
	cloned.searchService = e.searchService
	cloned.inferenceManager = e.inferenceManager
	cloned.onNodeMutated = e.onNodeMutated
	cloned.defaultEmbeddingDimensions = e.defaultEmbeddingDimensions
	cloned.dbManager = e.dbManager
	cloned.vectorRegistry = e.vectorRegistry
	cloned.vectorIndexSpaces = e.vectorIndexSpaces
	cloned.txContext = e.txContext

	e.shellParamsMu.RLock()
	if len(e.shellParams) > 0 {
		cloned.shellParams = make(map[string]interface{}, len(e.shellParams))
		for k, v := range e.shellParams {
			cloned.shellParams[k] = v
		}
	}
	e.shellParamsMu.RUnlock()

	return cloned
}

func (e *StorageExecutor) scopedExecutorForUse(db string) (*StorageExecutor, string, error) {
	targetDB := strings.TrimSpace(db)
	if targetDB == "" {
		return nil, "", fmt.Errorf("USE clause requires a database name")
	}

	if e.dbManager != nil {
		resolved, err := e.dbManager.ResolveDatabase(targetDB)
		if err != nil {
			return nil, "", fmt.Errorf("USE %s failed: %w", targetDB, err)
		}
		targetDB = resolved
	}

	ns, ok := e.storage.(*storage.NamespacedEngine)
	if !ok {
		return nil, "", fmt.Errorf("USE %s is not supported by this storage backend", targetDB)
	}

	if strings.EqualFold(ns.Namespace(), targetDB) {
		return e, targetDB, nil
	}

	scopedStore := storage.NewNamespacedEngine(ns.GetInnerEngine(), targetDB)
	return e.cloneForStorage(scopedStore), targetDB, nil
}
