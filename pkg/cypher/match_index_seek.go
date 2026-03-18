package cypher

import (
	"fmt"
	"sort"
	"strconv"
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

// tryCollectNodesFromPropertyIndexIn attempts to satisfy simple IN-list predicates:
//
//	<var>.<prop> IN $param
//
// where $param is a list value from params. This path is used heavily by Fabric
// batched correlated APPLY lookups.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexIn(
	nodePattern nodePatternInfo,
	whereClause string,
	params map[string]interface{},
) ([]*storage.Node, bool, error) {
	property, listValues, ok := e.parseSimpleIndexedInParam(nodePattern.variable, whereClause, params)
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

	idSet := make(map[storage.NodeID]struct{}, 256)
	for _, label := range labels {
		for _, value := range listValues {
			for _, id := range schema.PropertyIndexLookup(label, property, value) {
				idSet[id] = struct{}{}
			}
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

// tryCollectNodesFromPropertyIndexNotNullOrderLimit attempts to satisfy:
//
//	MATCH (n:Label) WHERE n.prop IS NOT NULL RETURN ... ORDER BY n.prop [ASC|DESC] LIMIT K
//
// using the property index directly (top-K by indexed key) without label scan.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexNotNullOrderLimit(
	nodePattern nodePatternInfo,
	whereClause string,
	orderExpr string,
	limit int,
) ([]*storage.Node, bool, error) {
	if limit <= 0 {
		return nil, false, nil
	}

	whereProp, ok := e.parseSimpleIndexedIsNotNull(nodePattern.variable, whereClause)
	if !ok {
		return nil, false, nil
	}

	orderSpecs := e.parseNodeOrderSpecs(orderExpr, nodePattern.variable)
	if len(orderSpecs) != 1 {
		return nil, false, nil
	}
	spec := orderSpecs[0]
	if !strings.EqualFold(strings.TrimSpace(spec.propName), strings.TrimSpace(whereProp)) {
		return nil, false, nil
	}

	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, false, nil
	}

	labels := e.indexCandidateLabels(schema, nodePattern.labels, whereProp)
	if len(labels) != 1 {
		// Keep semantics simple/deterministic for now; multi-label merge ordering can be added later.
		return nil, false, nil
	}
	label := labels[0]
	ids := schema.PropertyIndexTopK(label, whereProp, limit, spec.descending)
	if len(ids) == 0 {
		return []*storage.Node{}, true, nil
	}

	nodes := make([]*storage.Node, 0, len(ids))
	for _, id := range ids {
		node, err := e.storage.GetNode(id)
		if err != nil || node == nil {
			continue
		}
		if len(nodePattern.labels) > 0 && !nodeHasAnyLabel(node, nodePattern.labels) {
			continue
		}
		nodes = append(nodes, node)
		if len(nodes) >= limit {
			break
		}
	}
	return nodes, true, nil
}

// tryCollectNodesFromPropertyIndexNotNull attempts to satisfy:
//
//	MATCH (n:Label) WHERE n.prop IS NOT NULL
//
// from schema index entries, avoiding label scans.
func (e *StorageExecutor) tryCollectNodesFromPropertyIndexNotNull(
	nodePattern nodePatternInfo,
	whereClause string,
) ([]*storage.Node, bool, error) {
	property, ok := e.parseSimpleIndexedIsNotNull(nodePattern.variable, whereClause)
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
	ordered := make([]storage.NodeID, 0)
	for _, label := range labels {
		ids := schema.PropertyIndexAllNonNil(label, property, false)
		for _, id := range ids {
			if _, exists := idSet[id]; exists {
				continue
			}
			idSet[id] = struct{}{}
			ordered = append(ordered, id)
		}
	}
	if len(ordered) == 0 {
		return []*storage.Node{}, true, nil
	}

	nodes := make([]*storage.Node, 0, len(ordered))
	for _, id := range ordered {
		node, err := e.storage.GetNode(id)
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

func (e *StorageExecutor) parseSimpleIndexedInParam(variable, whereClause string, params map[string]interface{}) (property string, values []interface{}, ok bool) {
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
	// Keep this optimization deterministic and safe: only simple standalone IN predicates.
	if containsFold(clause, " AND ") || containsFold(clause, " OR ") {
		return "", nil, false
	}
	inIdx := keywordIndexFrom(clause, "IN", 0, defaultKeywordScanOpts())
	if inIdx <= 0 || inIdx >= len(clause)-2 {
		return "", nil, false
	}
	left := strings.TrimSpace(clause[:inIdx])
	right := strings.TrimSpace(clause[inIdx+2:])
	if !strings.HasPrefix(right, "$") {
		return "", nil, false
	}
	paramName := strings.TrimSpace(strings.TrimPrefix(right, "$"))
	if paramName == "" || params == nil {
		return "", nil, false
	}
	parsedProp, ok := parseVariableProperty(left, variable)
	if !ok {
		return "", nil, false
	}
	raw, exists := params[paramName]
	if !exists || raw == nil {
		return "", nil, false
	}
	list := coerceInterfaceList(raw)
	if len(list) == 0 {
		return "", []interface{}{}, true
	}
	out := make([]interface{}, 0, len(list))
	seen := make(map[string]struct{}, len(list))
	for _, v := range list {
		if v == nil {
			continue
		}
		k := fmt.Sprintf("%T:%v", v, v)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, v)
	}
	return parsedProp, out, true
}

func coerceInterfaceList(v interface{}) []interface{} {
	switch x := v.(type) {
	case []interface{}:
		return x
	case []string:
		out := make([]interface{}, len(x))
		for i := range x {
			out[i] = x[i]
		}
		return out
	case []int:
		out := make([]interface{}, len(x))
		for i := range x {
			out[i] = x[i]
		}
		return out
	case []int64:
		out := make([]interface{}, len(x))
		for i := range x {
			out[i] = x[i]
		}
		return out
	case []float64:
		out := make([]interface{}, len(x))
		for i := range x {
			out[i] = x[i]
		}
		return out
	default:
		return nil
	}
}

func (e *StorageExecutor) parseSimpleIndexedIsNotNull(variable, whereClause string) (property string, ok bool) {
	clause := strings.TrimSpace(whereClause)
	clause = unwrapOuterParens(clause)
	if clause == "" {
		return "", false
	}
	parts := splitTopLevelAndConjuncts(clause)
	targetProp := ""
	for _, raw := range parts {
		part := strings.TrimSpace(raw)
		if part == "" {
			continue
		}
		part = unwrapOuterParens(part)
		if p, ok := parseSimpleSingleIndexedIsNotNull(variable, part); ok {
			if targetProp != "" && !strings.EqualFold(targetProp, p) {
				return "", false
			}
			targetProp = p
			continue
		}
		// Allow only constant boolean conjuncts in addition to var.prop IS NOT NULL.
		// This keeps top-K semantics correct while supporting cache-buster style predicates.
		if _, isConst := e.tryEvaluateConstantBooleanConjunct(part); isConst {
			continue
		}
		return "", false
	}
	if targetProp == "" {
		return "", false
	}
	return targetProp, true
}

func parseSimpleSingleIndexedIsNotNull(variable, clause string) (property string, ok bool) {
	upper := strings.ToUpper(strings.TrimSpace(clause))
	sfx := " IS NOT NULL"
	if !strings.HasSuffix(upper, sfx) {
		return "", false
	}
	left := strings.TrimSpace(clause[:len(clause)-len(sfx)])
	if left == "" {
		return "", false
	}
	prop, ok := parseVariableProperty(left, variable)
	if !ok {
		return "", false
	}
	return prop, true
}

func unwrapOuterParens(clause string) string {
	out := strings.TrimSpace(clause)
	for strings.HasPrefix(out, "(") && strings.HasSuffix(out, ")") && len(out) >= 2 {
		inner := strings.TrimSpace(out[1 : len(out)-1])
		if inner == out {
			break
		}
		out = inner
	}
	return out
}

func splitTopLevelAndConjuncts(clause string) []string {
	inSingle, inDouble, inBacktick := false, false, false
	paren, bracket, brace := 0, 0, 0
	parts := make([]string, 0, 2)
	start := 0
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
		}
		if paren == 0 && bracket == 0 && brace == 0 && i+3 <= len(clause) && strings.EqualFold(clause[i:i+3], "AND") {
			prevOK := i == 0 || isWhitespace(clause[i-1]) || clause[i-1] == '('
			nextIdx := i + 3
			nextOK := nextIdx >= len(clause) || isWhitespace(clause[nextIdx]) || clause[nextIdx] == ')'
			if prevOK && nextOK {
				parts = append(parts, strings.TrimSpace(clause[start:i]))
				start = nextIdx
				i = nextIdx - 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(clause[start:]))
	return parts
}

func (e *StorageExecutor) tryEvaluateConstantBooleanConjunct(clause string) (value bool, ok bool) {
	expr := strings.TrimSpace(clause)
	if expr == "" {
		return false, false
	}
	if strings.EqualFold(expr, "TRUE") {
		return true, true
	}
	if strings.EqualFold(expr, "FALSE") {
		return false, true
	}
	// If expression mentions variables/properties, treat as non-constant.
	if strings.Contains(expr, ".") || strings.Contains(expr, "$") {
		return false, false
	}
	for _, op := range []string{"<>", "!=", ">=", "<=", "=", ">", "<"} {
		if idx := topLevelSymbolIndex(expr, op); idx >= 0 {
			left := strings.TrimSpace(expr[:idx])
			right := strings.TrimSpace(expr[idx+len(op):])
			lv, lok := parseLiteralValue(left)
			rv, rok := parseLiteralValue(right)
			if !lok || !rok {
				return false, false
			}
			return compareLiteralValues(lv, rv, op)
		}
	}
	return false, false
}

func topLevelSymbolIndex(expr, sym string) int {
	inSingle, inDouble, inBacktick := false, false, false
	paren, bracket, brace := 0, 0, 0
	for i := 0; i <= len(expr)-len(sym); i++ {
		ch := expr[i]
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
		}
		if paren == 0 && bracket == 0 && brace == 0 && expr[i:i+len(sym)] == sym {
			return i
		}
	}
	return -1
}

func parseLiteralValue(raw string) (interface{}, bool) {
	s := strings.TrimSpace(raw)
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1], true
		}
	}
	if strings.EqualFold(s, "true") {
		return true, true
	}
	if strings.EqualFold(s, "false") {
		return false, true
	}
	if strings.EqualFold(s, "null") {
		return nil, true
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, true
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, true
	}
	return nil, false
}

func compareLiteralValues(left, right interface{}, op string) (bool, bool) {
	switch op {
	case "=", "!=", "<>":
		eq := fmt.Sprintf("%v", left) == fmt.Sprintf("%v", right)
		if op == "=" {
			return eq, true
		}
		return !eq, true
	}
	lf, lok := toFloat64(left)
	rf, rok := toFloat64(right)
	if !lok || !rok {
		return false, false
	}
	switch op {
	case ">":
		return lf > rf, true
	case "<":
		return lf < rf, true
	case ">=":
		return lf >= rf, true
	case "<=":
		return lf <= rf, true
	default:
		return false, false
	}
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
