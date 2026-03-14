package cypher

import (
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// tryCollectNodesFromPropertyIndex attempts to satisfy MATCH node candidates from a schema property index.
// It only applies to simple equality predicates:
//   - <var>.<prop> = <value>
//   - <value> = <var>.<prop>
//
// It returns (nodes, true, nil) when index planning was used (including empty matches),
// and (nil, false, nil) when the predicate is not eligible for index lookup.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndex(nodePattern nodePatternInfo, whereClause string) ([]*storage.Node, bool, error) {
	property, value, ok := e.parseSimpleIndexedEquality(nodePattern.variable, whereClause)
	if !ok {
		return nil, false, nil
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, false, nil
	}

	labels := e.indexCandidateLabels(schema, nodePattern.labels, property)
	if len(labels) == 0 {
		return nil, false, nil
	}

	idSet := make(map[storage.NodeID]struct{})
	for _, label := range labels {
		for _, id := range schema.PropertyIndexLookup(label, property, value) {
			idSet[id] = struct{}{}
		}
	}

	if len(idSet) == 0 {
		return []*storage.Node{}, true, nil
	}

	nodes := make([]*storage.Node, 0, len(idSet))
	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	for _, id := range ids {
		node, err := e.storage.GetNode(storage.NodeID(id))
		if err != nil || node == nil {
			continue
		}
		if len(nodePattern.labels) > 0 && !nodeHasAnyLabel(node, nodePattern.labels) {
			continue
		}
		nodes = append(nodes, node)
	}

	return nodes, true, nil
}

func (e *StorageExecutor) indexCandidateLabels(schema *storage.SchemaManager, queryLabels []string, property string) []string {
	if len(queryLabels) > 0 {
		out := make([]string, 0, len(queryLabels))
		for _, label := range queryLabels {
			if _, exists := schema.GetPropertyIndex(label, property); exists {
				out = append(out, label)
			}
		}
		return out
	}

	labels := make(map[string]struct{})
	for _, raw := range schema.GetIndexes() {
		idx, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		typ, _ := idx["type"].(string)
		if !strings.EqualFold(strings.TrimSpace(typ), "PROPERTY") {
			continue
		}
		label, _ := idx["label"].(string)
		if strings.TrimSpace(label) == "" {
			continue
		}
		prop := ""
		if p, ok := idx["property"].(string); ok {
			prop = p
		}
		if prop == "" {
			if props, ok := idx["properties"].([]string); ok && len(props) == 1 {
				prop = props[0]
			}
			if prop == "" {
				if vals, ok := idx["properties"].([]interface{}); ok && len(vals) == 1 {
					if ps, ok := vals[0].(string); ok {
						prop = ps
					}
				}
			}
		}
		if !strings.EqualFold(strings.TrimSpace(prop), property) {
			continue
		}
		labels[label] = struct{}{}
	}

	out := make([]string, 0, len(labels))
	for label := range labels {
		out = append(out, label)
	}
	sort.Strings(out)
	return out
}

func (e *StorageExecutor) parseSimpleIndexedEquality(variable, whereClause string) (property string, value interface{}, ok bool) {
	clause := strings.TrimSpace(whereClause)
	for strings.HasPrefix(clause, "(") && strings.HasSuffix(clause, ")") && len(clause) >= 2 {
		inner := strings.TrimSpace(clause[1 : len(clause)-1])
		if inner == clause {
			break
		}
		clause = inner
	}
	if clause == "" {
		return "", nil, false
	}

	// Only simple predicates are index-eligible.
	for _, kw := range []string{"AND", "OR", "NOT", " IN ", " IS ", "<>", "!=", ">=", "<=", ">", "<"} {
		if topLevelKeywordIndex(clause, kw) >= 0 {
			return "", nil, false
		}
	}

	eqIdx := topLevelEqualsIndex(clause)
	if eqIdx <= 0 || eqIdx >= len(clause)-1 {
		return "", nil, false
	}

	left := strings.TrimSpace(clause[:eqIdx])
	right := strings.TrimSpace(clause[eqIdx+1:])
	if left == "" || right == "" {
		return "", nil, false
	}

	prop, isLeftVarProp := parseVariableProperty(left, variable)
	if isLeftVarProp {
		return prop, e.parseValue(right), true
	}
	prop, isRightVarProp := parseVariableProperty(right, variable)
	if isRightVarProp {
		return prop, e.parseValue(left), true
	}
	return "", nil, false
}

func parseVariableProperty(expr, variable string) (string, bool) {
	dot := strings.IndexByte(expr, '.')
	if dot <= 0 || dot >= len(expr)-1 {
		return "", false
	}
	lhs := strings.TrimSpace(expr[:dot])
	if !strings.EqualFold(lhs, strings.TrimSpace(variable)) {
		return "", false
	}
	prop := normalizePropertyKey(strings.TrimSpace(expr[dot+1:]))
	if prop == "" {
		return "", false
	}
	return prop, true
}

func topLevelEqualsIndex(clause string) int {
	inSingle, inDouble, inBacktick := false, false, false
	paren, bracket, brace := 0, 0, 0
	for i := 0; i < len(clause); i++ {
		ch := clause[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
			continue
		case '"':
			inDouble = true
			continue
		case '`':
			inBacktick = true
			continue
		case '(':
			paren++
			continue
		case ')':
			if paren > 0 {
				paren--
			}
			continue
		case '[':
			bracket++
			continue
		case ']':
			if bracket > 0 {
				bracket--
			}
			continue
		case '{':
			brace++
			continue
		case '}':
			if brace > 0 {
				brace--
			}
			continue
		case '=':
			if paren != 0 || bracket != 0 || brace != 0 {
				continue
			}
			if i > 0 {
				prev := clause[i-1]
				if prev == '!' || prev == '<' || prev == '>' {
					continue
				}
			}
			if i+1 < len(clause) {
				next := clause[i+1]
				if next == '=' {
					continue
				}
			}
			return i
		}
	}
	return -1
}

func nodeHasAnyLabel(node *storage.Node, labels []string) bool {
	for _, want := range labels {
		for _, have := range node.Labels {
			if strings.EqualFold(strings.TrimSpace(want), strings.TrimSpace(have)) {
				return true
			}
		}
	}
	return false
}
