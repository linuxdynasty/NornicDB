package cypher

import (
	"fmt"
	"strings"
)

// parseSetMergeMapLiteralStrict parses an inline map literal used by SET +=.
// Unlike permissive property parsing helpers, this enforces Cypher semantics:
// malformed maps must return an error instead of becoming an empty map/no-op.
func (e *StorageExecutor) parseSetMergeMapLiteralStrict(s string) (map[string]interface{}, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil, fmt.Errorf("map literal must be enclosed in { ... }")
	}

	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return map[string]interface{}{}, nil
	}

	props := make(map[string]interface{})
	pairs := splitTopLevelComma(inner)
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			return nil, fmt.Errorf("empty map entry")
		}

		colonIdx := strings.Index(pair, ":")
		if colonIdx <= 0 || colonIdx == len(pair)-1 {
			return nil, fmt.Errorf("invalid map entry %q", pair)
		}

		key := normalizePropertyKey(strings.TrimSpace(pair[:colonIdx]))
		if key == "" {
			return nil, fmt.Errorf("empty map key")
		}
		value := strings.TrimSpace(pair[colonIdx+1:])
		if value == "" {
			return nil, fmt.Errorf("empty map value for key %q", key)
		}

		props[key] = e.parseValue(value)
	}

	return props, nil
}
