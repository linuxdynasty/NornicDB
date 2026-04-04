package storage

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/convert"
)

const (
	ConstraintContractKindPrimitiveNode         = "primitive-node"
	ConstraintContractKindPrimitiveRelationship = "primitive-relationship"
	ConstraintContractKindBooleanNode           = "boolean-node"
	ConstraintContractKindBooleanRelationship   = "boolean-relationship"
)

type ConstraintContract struct {
	Name              string                    `json:"name"`
	TargetEntityType  string                    `json:"target_entity_type"`
	TargetLabelOrType string                    `json:"target_label_or_type"`
	Definition        string                    `json:"definition"`
	Entries           []ConstraintContractEntry `json:"entries,omitempty"`
}

type ConstraintContractEntry struct {
	Kind          string   `json:"kind"`
	PrimitiveType string   `json:"primitive_type,omitempty"`
	Properties    []string `json:"properties,omitempty"`
	Property      string   `json:"property,omitempty"`
	ExpectedType  string   `json:"expected_type,omitempty"`
	Expression    string   `json:"expression,omitempty"`
}

func cloneConstraintContractEntry(entry ConstraintContractEntry) ConstraintContractEntry {
	cloned := entry
	if len(entry.Properties) > 0 {
		cloned.Properties = append([]string(nil), entry.Properties...)
	}
	return cloned
}

func cloneConstraintContract(contract ConstraintContract) ConstraintContract {
	cloned := contract
	if len(contract.Entries) > 0 {
		cloned.Entries = make([]ConstraintContractEntry, 0, len(contract.Entries))
		for _, entry := range contract.Entries {
			cloned.Entries = append(cloned.Entries, cloneConstraintContractEntry(entry))
		}
	}
	return cloned
}

func constraintContractEqual(a, b ConstraintContract) bool {
	if a.Name != b.Name ||
		a.TargetEntityType != b.TargetEntityType ||
		a.TargetLabelOrType != b.TargetLabelOrType ||
		a.Definition != b.Definition ||
		len(a.Entries) != len(b.Entries) {
		return false
	}
	for i := range a.Entries {
		left := a.Entries[i]
		right := b.Entries[i]
		if left.Kind != right.Kind ||
			left.PrimitiveType != right.PrimitiveType ||
			left.Property != right.Property ||
			left.ExpectedType != right.ExpectedType ||
			left.Expression != right.Expression ||
			len(left.Properties) != len(right.Properties) {
			return false
		}
		for j := range left.Properties {
			if left.Properties[j] != right.Properties[j] {
				return false
			}
		}
	}
	return true
}

func (sm *SchemaManager) GetAllConstraintContracts() []ConstraintContract {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	contracts := make([]ConstraintContract, 0, len(sm.constraintContracts))
	for _, contract := range sm.constraintContracts {
		contracts = append(contracts, cloneConstraintContract(contract))
	}
	sort.Slice(contracts, func(i, j int) bool {
		return contracts[i].Name < contracts[j].Name
	})
	return contracts
}

func (sm *SchemaManager) GetConstraintContractsForTarget(entityType ConstraintEntityType, labelOrType string) []ConstraintContract {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	contracts := make([]ConstraintContract, 0)
	for _, contract := range sm.constraintContracts {
		if contract.TargetEntityType != string(entityType) || contract.TargetLabelOrType != labelOrType {
			continue
		}
		contracts = append(contracts, cloneConstraintContract(contract))
	}
	sort.Slice(contracts, func(i, j int) bool {
		return contracts[i].Name < contracts[j].Name
	})
	return contracts
}

func (sm *SchemaManager) AddConstraintContractBundle(contract ConstraintContract, compiledConstraints []Constraint, compiledTypes []PropertyTypeConstraint, ifNotExists bool) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	snapshot := sm.exportDefinitionLocked()

	if existing, exists := sm.constraintContracts[contract.Name]; exists {
		if ifNotExists && constraintContractEqual(existing, contract) {
			return nil
		}
		return fmt.Errorf("constraint contract %q already exists", contract.Name)
	}
	if _, exists := sm.constraints[contract.Name]; exists {
		return fmt.Errorf("constraint contract %q conflicts with an existing constraint name", contract.Name)
	}
	if _, exists := sm.propertyTypeConstraints[contract.Name]; exists {
		return fmt.Errorf("constraint contract %q conflicts with an existing constraint name", contract.Name)
	}

	for _, compiled := range compiledConstraints {
		if err := sm.addConstraintLocked(compiled, false); err != nil {
			sm.replaceFromDefinitionLocked(snapshot)
			return err
		}
	}
	for _, compiled := range compiledTypes {
		if err := sm.addPropertyTypeConstraintValueLocked(compiled, false); err != nil {
			sm.replaceFromDefinitionLocked(snapshot)
			return err
		}
	}

	sm.constraintContracts[contract.Name] = cloneConstraintContract(contract)

	if sm.persist != nil {
		def := sm.exportDefinitionLocked()
		if err := sm.persist(def); err != nil {
			sm.replaceFromDefinitionLocked(snapshot)
			return err
		}
	}

	return nil
}

func ValidateConstraintContractOnCreationForEngine(engine Engine, contract ConstraintContract) error {
	entityType := ConstraintEntityType(contract.TargetEntityType)
	switch entityType {
	case ConstraintEntityNode:
		nodes, err := engine.GetNodesByLabel(contract.TargetLabelOrType)
		if err != nil {
			return fmt.Errorf("scanning nodes: %w", err)
		}
		for _, node := range nodes {
			if err := validateConstraintContractForNodeEngine(engine, contract, node); err != nil {
				return err
			}
		}
	case ConstraintEntityRelationship:
		edges, err := engine.GetEdgesByType(contract.TargetLabelOrType)
		if err != nil {
			return fmt.Errorf("scanning relationships: %w", err)
		}
		for _, edge := range edges {
			if err := validateConstraintContractForEdgeEngine(engine, contract, edge); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported constraint contract target entity type: %s", contract.TargetEntityType)
	}
	return nil
}

func validateConstraintContractForNodeEngine(engine Engine, contract ConstraintContract, node *Node) error {
	for _, entry := range contract.Entries {
		if entry.Kind != ConstraintContractKindBooleanNode {
			continue
		}
		ok, err := evaluateNodeConstraintContractExpressionEngine(engine, node, entry.Expression)
		if err != nil {
			return fmt.Errorf("constraint contract %s invalid: predicate %q: %w", contract.Name, entry.Expression, err)
		}
		if !ok {
			return fmt.Errorf("constraint contract %s violated: predicate %q evaluated to false", contract.Name, entry.Expression)
		}
	}
	return nil
}

func validateConstraintContractForEdgeEngine(engine Engine, contract ConstraintContract, edge *Edge) error {
	for _, entry := range contract.Entries {
		if entry.Kind != ConstraintContractKindBooleanRelationship {
			continue
		}
		ok, err := evaluateRelationshipConstraintContractExpressionEngine(engine, edge, entry.Expression)
		if err != nil {
			return fmt.Errorf("constraint contract %s invalid: predicate %q: %w", contract.Name, entry.Expression, err)
		}
		if !ok {
			return fmt.Errorf("constraint contract %s violated: predicate %q evaluated to false", contract.Name, entry.Expression)
		}
	}
	return nil
}

func evaluateNodeConstraintContractExpressionEngine(engine Engine, node *Node, expr string) (bool, error) {
	if matched, values, property, err := parsePropertyInExpression(expr); err != nil {
		return false, err
	} else if matched {
		return evaluatePropertyInExpression(node.Properties[property], values), nil
	}

	if matched, pattern, comparator, threshold, err := parseCountPatternExpression(expr); err != nil {
		return false, err
	} else if matched {
		count, err := countMatchingPatternEdgesEngine(engine, node, pattern)
		if err != nil {
			return false, err
		}
		return compareInt(count, comparator, threshold), nil
	}

	if matched, pattern, err := parseNotExistsPatternExpression(expr); err != nil {
		return false, err
	} else if matched {
		count, err := countMatchingPatternEdgesEngine(engine, node, pattern)
		if err != nil {
			return false, err
		}
		return count == 0, nil
	}

	return false, fmt.Errorf("unsupported node predicate")
}

func evaluateRelationshipConstraintContractExpressionEngine(engine Engine, edge *Edge, expr string) (bool, error) {
	if matched, property, values, err := parseRelationshipPropertyInExpression(expr); err != nil {
		return false, err
	} else if matched {
		return evaluatePropertyInExpression(edge.Properties[property], values), nil
	}

	if matched := relationshipDistinctPattern.MatchString(strings.TrimSpace(expr)); matched {
		return edge.StartNode != edge.EndNode, nil
	}

	if matched, leftProp, rightProp := parseEndpointPropertyEqualityExpression(expr); matched {
		startNode, err := engine.GetNode(edge.StartNode)
		if err != nil {
			return false, err
		}
		endNode, err := engine.GetNode(edge.EndNode)
		if err != nil {
			return false, err
		}
		if startNode == nil || endNode == nil {
			return false, fmt.Errorf("missing relationship endpoint")
		}
		return compareValues(startNode.Properties[leftProp], endNode.Properties[rightProp]), nil
	}

	if matched, property, comparator, value, err := parseRelationshipPropertyComparisonExpression(expr); err != nil {
		return false, err
	} else if matched {
		return compareConstraintExpressionValue(edge.Properties[property], comparator, value), nil
	}

	return false, fmt.Errorf("unsupported relationship predicate")
}

type contractPattern struct {
	Direction    string
	RelationType string
	TargetLabel  string
}

var (
	countPatternExpr                = regexp.MustCompile(`(?is)^COUNT\s*\{\s*(?:MATCH\s+)?(.+?)\s*\}\s*(<=|>=|<>|!=|=|<|>)\s*(-?\d+)\s*$`)
	notExistsPatternExpr            = regexp.MustCompile(`(?is)^NOT\s+EXISTS\s*\{\s*(?:MATCH\s+)?(.+?)\s*\}\s*$`)
	propertyInExpr                  = regexp.MustCompile(`^\s*[A-Za-z_][A-Za-z0-9_]*\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\s+IN\s+\[(.*)\]\s*$`)
	relationshipDistinctPattern     = regexp.MustCompile(`(?i)^\s*startNode\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*\)\s*<>\s*endNode\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*\)\s*$`)
	endpointPropertyEqualityExpr    = regexp.MustCompile(`(?i)^\s*startNode\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*\)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*endNode\s*\(\s*[A-Za-z_][A-Za-z0-9_]*\s*\)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*$`)
	relationshipPropertyCompareExpr = regexp.MustCompile(`^\s*[A-Za-z_][A-Za-z0-9_]*\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*(<=|>=|<>|!=|=|<|>)\s*(.+?)\s*$`)
	patternOutgoingExpr             = regexp.MustCompile(`(?is)^\(\s*[A-Za-z_][A-Za-z0-9_]*(?:\s*:\s*[A-Za-z_][A-Za-z0-9_]*)?\s*\)\s*-\s*\[:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*->\s*\(\s*(?::\s*([A-Za-z_][A-Za-z0-9_]*))?\s*\)$`)
	patternIncomingExpr             = regexp.MustCompile(`(?is)^\(\s*[A-Za-z_][A-Za-z0-9_]*(?:\s*:\s*[A-Za-z_][A-Za-z0-9_]*)?\s*\)\s*<-\s*\[:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\]\s*-\s*\(\s*(?::\s*([A-Za-z_][A-Za-z0-9_]*))?\s*\)$`)
)

func parsePropertyInExpression(expr string) (bool, []interface{}, string, error) {
	matches := propertyInExpr.FindStringSubmatch(strings.TrimSpace(expr))
	if matches == nil {
		return false, nil, "", nil
	}
	values, err := parseContractLiteralList(matches[2])
	if err != nil {
		return false, nil, "", err
	}
	return true, values, matches[1], nil
}

func parseRelationshipPropertyInExpression(expr string) (bool, string, []interface{}, error) {
	matched, values, property, err := parsePropertyInExpression(expr)
	if !matched || err != nil {
		return matched, property, values, err
	}
	return true, property, values, nil
}

func parseCountPatternExpression(expr string) (bool, contractPattern, string, int, error) {
	matches := countPatternExpr.FindStringSubmatch(strings.TrimSpace(expr))
	if matches == nil {
		return false, contractPattern{}, "", 0, nil
	}
	pattern, err := parseConstraintPattern(matches[1])
	if err != nil {
		return false, contractPattern{}, "", 0, err
	}
	threshold, err := strconv.Atoi(matches[3])
	if err != nil {
		return false, contractPattern{}, "", 0, err
	}
	return true, pattern, matches[2], threshold, nil
}

func parseNotExistsPatternExpression(expr string) (bool, contractPattern, error) {
	matches := notExistsPatternExpr.FindStringSubmatch(strings.TrimSpace(expr))
	if matches == nil {
		return false, contractPattern{}, nil
	}
	pattern, err := parseConstraintPattern(matches[1])
	if err != nil {
		return false, contractPattern{}, err
	}
	return true, pattern, nil
}

func parseConstraintPattern(raw string) (contractPattern, error) {
	raw = strings.TrimSpace(raw)
	if matches := patternOutgoingExpr.FindStringSubmatch(raw); matches != nil {
		return contractPattern{Direction: "OUTGOING", RelationType: matches[1], TargetLabel: matches[2]}, nil
	}
	if matches := patternIncomingExpr.FindStringSubmatch(raw); matches != nil {
		return contractPattern{Direction: "INCOMING", RelationType: matches[1], TargetLabel: matches[2]}, nil
	}
	return contractPattern{}, fmt.Errorf("unsupported pattern %q", raw)
}

func parseEndpointPropertyEqualityExpression(expr string) (bool, string, string) {
	matches := endpointPropertyEqualityExpr.FindStringSubmatch(strings.TrimSpace(expr))
	if matches == nil {
		return false, "", ""
	}
	return true, matches[1], matches[2]
}

func parseRelationshipPropertyComparisonExpression(expr string) (bool, string, string, interface{}, error) {
	matches := relationshipPropertyCompareExpr.FindStringSubmatch(strings.TrimSpace(expr))
	if matches == nil {
		return false, "", "", nil, nil
	}
	if _, _, property, err := parsePropertyInExpression(expr); err == nil && property != "" {
		return false, "", "", nil, nil
	}
	value, err := parseContractLiteral(matches[3])
	if err != nil {
		return false, "", "", nil, err
	}
	return true, matches[1], matches[2], value, nil
}

func parseContractLiteralList(raw string) ([]interface{}, error) {
	parts := splitTopLevelCSV(raw)
	values := make([]interface{}, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		value, err := parseContractLiteral(part)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func parseContractLiteral(raw string) (interface{}, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) >= 2 {
		if (raw[0] == '\'' && raw[len(raw)-1] == '\'') || (raw[0] == '"' && raw[len(raw)-1] == '"') {
			return raw[1 : len(raw)-1], nil
		}
	}
	if i, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return i, nil
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f, nil
	}
	if strings.EqualFold(raw, "true") {
		return true, nil
	}
	if strings.EqualFold(raw, "false") {
		return false, nil
	}
	if strings.EqualFold(raw, "null") {
		return nil, nil
	}
	return nil, fmt.Errorf("unsupported literal %q", raw)
}

func splitTopLevelCSV(raw string) []string {
	parts := make([]string, 0)
	var builder strings.Builder
	depth := 0
	quote := rune(0)
	for _, ch := range raw {
		switch {
		case quote != 0:
			builder.WriteRune(ch)
			if ch == quote {
				quote = 0
			}
		case ch == '\'' || ch == '"':
			quote = ch
			builder.WriteRune(ch)
		case ch == '[' || ch == '(' || ch == '{':
			depth++
			builder.WriteRune(ch)
		case ch == ']' || ch == ')' || ch == '}':
			if depth > 0 {
				depth--
			}
			builder.WriteRune(ch)
		case ch == ',' && depth == 0:
			parts = append(parts, strings.TrimSpace(builder.String()))
			builder.Reset()
		default:
			builder.WriteRune(ch)
		}
	}
	if builder.Len() > 0 {
		parts = append(parts, strings.TrimSpace(builder.String()))
	}
	return parts
}

func evaluatePropertyInExpression(actual interface{}, values []interface{}) bool {
	if actual == nil {
		return false
	}
	for _, value := range values {
		if compareValues(actual, value) {
			return true
		}
	}
	return false
}

func compareConstraintExpressionValue(actual interface{}, comparator string, expected interface{}) bool {
	switch comparator {
	case "=":
		return compareValues(actual, expected)
	case "<>", "!=":
		return !compareValues(actual, expected)
	case ">":
		return compareGreaterConstraint(actual, expected)
	case ">=":
		return compareGreaterConstraint(actual, expected) || compareValues(actual, expected)
	case "<":
		return compareLessConstraint(actual, expected)
	case "<=":
		return compareLessConstraint(actual, expected) || compareValues(actual, expected)
	default:
		return false
	}
}

func compareGreaterConstraint(actual interface{}, expected interface{}) bool {
	actualFloat, actualOK := convert.ToFloat64(actual)
	expectedFloat, expectedOK := convert.ToFloat64(expected)
	if actualOK && expectedOK {
		return actualFloat > expectedFloat
	}
	return fmt.Sprintf("%v", actual) > fmt.Sprintf("%v", expected)
}

func compareLessConstraint(actual interface{}, expected interface{}) bool {
	actualFloat, actualOK := convert.ToFloat64(actual)
	expectedFloat, expectedOK := convert.ToFloat64(expected)
	if actualOK && expectedOK {
		return actualFloat < expectedFloat
	}
	return fmt.Sprintf("%v", actual) < fmt.Sprintf("%v", expected)
}

func compareInt(actual int, comparator string, expected int) bool {
	switch comparator {
	case "=":
		return actual == expected
	case "<>", "!=":
		return actual != expected
	case ">":
		return actual > expected
	case ">=":
		return actual >= expected
	case "<":
		return actual < expected
	case "<=":
		return actual <= expected
	default:
		return false
	}
}

func countMatchingPatternEdgesEngine(engine Engine, node *Node, pattern contractPattern) (int, error) {
	var edges []*Edge
	var err error
	if pattern.Direction == "INCOMING" {
		edges, err = engine.GetIncomingEdges(node.ID)
	} else {
		edges, err = engine.GetOutgoingEdges(node.ID)
	}
	if err != nil {
		return 0, err
	}
	count := 0
	for _, edge := range edges {
		if edge.Type != pattern.RelationType {
			continue
		}
		if pattern.TargetLabel != "" {
			otherID := edge.EndNode
			if pattern.Direction == "INCOMING" {
				otherID = edge.StartNode
			}
			otherNode, err := engine.GetNode(otherID)
			if err != nil {
				return 0, err
			}
			if otherNode == nil || !hasLabel(otherNode.Labels, pattern.TargetLabel) {
				continue
			}
		}
		count++
	}
	return count, nil
}

func (tx *BadgerTransaction) validateConstraintContracts() error {
	affectedNodes := make(map[NodeID]struct{})
	affectedEdges := make(map[EdgeID]struct{})

	for nodeID := range tx.pendingNodes {
		affectedNodes[nodeID] = struct{}{}
	}
	for edgeID, edge := range tx.pendingEdges {
		affectedEdges[edgeID] = struct{}{}
		affectedNodes[edge.StartNode] = struct{}{}
		affectedNodes[edge.EndNode] = struct{}{}
	}
	for _, op := range tx.operations {
		if op.OldEdge != nil {
			affectedNodes[op.OldEdge.StartNode] = struct{}{}
			affectedNodes[op.OldEdge.EndNode] = struct{}{}
		}
	}

	for nodeID := range affectedNodes {
		node, err := tx.currentNodeLocked(nodeID)
		if err != nil {
			return err
		}
		if node == nil {
			continue
		}
		if err := tx.validateConstraintContractsForNodeLocked(node); err != nil {
			return err
		}
		adjacent, err := tx.currentAdjacentEdgesLocked(node.ID)
		if err != nil {
			return err
		}
		for _, edge := range adjacent {
			if err := tx.validateConstraintContractsForEdgeLocked(edge); err != nil {
				return err
			}
		}
	}
	for edgeID := range affectedEdges {
		edge, err := tx.currentEdgeLocked(edgeID)
		if err != nil {
			return err
		}
		if edge == nil {
			continue
		}
		if err := tx.validateConstraintContractsForEdgeLocked(edge); err != nil {
			return err
		}
	}

	return nil
}

func (tx *BadgerTransaction) validateConstraintContractsForNodeLocked(node *Node) error {
	dbName, ok := constraintContractNamespaceForNode(node)
	if !ok {
		return nil
	}
	schema := tx.engine.GetSchemaForNamespace(dbName)
	if schema == nil {
		return nil
	}
	contractsByName := make(map[string]ConstraintContract)
	for _, label := range node.Labels {
		for _, contract := range schema.GetConstraintContractsForTarget(ConstraintEntityNode, label) {
			contractsByName[contract.Name] = contract
		}
	}
	for _, contract := range contractsByName {
		for _, entry := range contract.Entries {
			if entry.Kind != ConstraintContractKindBooleanNode {
				continue
			}
			ok, err := tx.evaluateNodeConstraintContractExpressionLocked(node, entry.Expression)
			if err != nil {
				return fmt.Errorf("constraint contract %s invalid: predicate %q: %w", contract.Name, entry.Expression, err)
			}
			if !ok {
				return fmt.Errorf("constraint contract %s violated: predicate %q evaluated to false", contract.Name, entry.Expression)
			}
		}
	}
	return nil
}

func (tx *BadgerTransaction) validateConstraintContractsForEdgeLocked(edge *Edge) error {
	dbName, ok := constraintContractNamespaceForEdge(edge)
	if !ok {
		return nil
	}
	schema := tx.engine.GetSchemaForNamespace(dbName)
	if schema == nil {
		return nil
	}
	for _, contract := range schema.GetConstraintContractsForTarget(ConstraintEntityRelationship, edge.Type) {
		for _, entry := range contract.Entries {
			if entry.Kind != ConstraintContractKindBooleanRelationship {
				continue
			}
			ok, err := tx.evaluateRelationshipConstraintContractExpressionLocked(edge, entry.Expression)
			if err != nil {
				return fmt.Errorf("constraint contract %s invalid: predicate %q: %w", contract.Name, entry.Expression, err)
			}
			if !ok {
				return fmt.Errorf("constraint contract %s violated: predicate %q evaluated to false", contract.Name, entry.Expression)
			}
		}
	}
	return nil
}

func (tx *BadgerTransaction) evaluateNodeConstraintContractExpressionLocked(node *Node, expr string) (bool, error) {
	if matched, values, property, err := parsePropertyInExpression(expr); err != nil {
		return false, err
	} else if matched {
		return evaluatePropertyInExpression(node.Properties[property], values), nil
	}
	if matched, pattern, comparator, threshold, err := parseCountPatternExpression(expr); err != nil {
		return false, err
	} else if matched {
		count, err := tx.countMatchingPatternEdgesLocked(node, pattern)
		if err != nil {
			return false, err
		}
		return compareInt(count, comparator, threshold), nil
	}
	if matched, pattern, err := parseNotExistsPatternExpression(expr); err != nil {
		return false, err
	} else if matched {
		count, err := tx.countMatchingPatternEdgesLocked(node, pattern)
		if err != nil {
			return false, err
		}
		return count == 0, nil
	}
	return false, fmt.Errorf("unsupported node predicate")
}

func (tx *BadgerTransaction) evaluateRelationshipConstraintContractExpressionLocked(edge *Edge, expr string) (bool, error) {
	if matched, property, values, err := parseRelationshipPropertyInExpression(expr); err != nil {
		return false, err
	} else if matched {
		return evaluatePropertyInExpression(edge.Properties[property], values), nil
	}
	if relationshipDistinctPattern.MatchString(strings.TrimSpace(expr)) {
		return edge.StartNode != edge.EndNode, nil
	}
	if matched, leftProp, rightProp := parseEndpointPropertyEqualityExpression(expr); matched {
		startNode, err := tx.currentNodeLocked(edge.StartNode)
		if err != nil {
			return false, err
		}
		endNode, err := tx.currentNodeLocked(edge.EndNode)
		if err != nil {
			return false, err
		}
		if startNode == nil || endNode == nil {
			return false, fmt.Errorf("missing relationship endpoint")
		}
		return compareValues(startNode.Properties[leftProp], endNode.Properties[rightProp]), nil
	}
	if matched, property, comparator, value, err := parseRelationshipPropertyComparisonExpression(expr); err != nil {
		return false, err
	} else if matched {
		return compareConstraintExpressionValue(edge.Properties[property], comparator, value), nil
	}
	return false, fmt.Errorf("unsupported relationship predicate")
}

func (tx *BadgerTransaction) countMatchingPatternEdgesLocked(node *Node, pattern contractPattern) (int, error) {
	var edges []*Edge
	var err error
	if pattern.Direction == "INCOMING" {
		edges, err = tx.currentIncomingEdgesLocked(node.ID)
	} else {
		edges, err = tx.currentOutgoingEdgesLocked(node.ID)
	}
	if err != nil {
		return 0, err
	}
	count := 0
	for _, edge := range edges {
		if edge.Type != pattern.RelationType {
			continue
		}
		if pattern.TargetLabel != "" {
			otherID := edge.EndNode
			if pattern.Direction == "INCOMING" {
				otherID = edge.StartNode
			}
			otherNode, err := tx.currentNodeLocked(otherID)
			if err != nil {
				return 0, err
			}
			if otherNode == nil || !hasLabel(otherNode.Labels, pattern.TargetLabel) {
				continue
			}
		}
		count++
	}
	return count, nil
}

func (tx *BadgerTransaction) currentNodeLocked(nodeID NodeID) (*Node, error) {
	if _, deleted := tx.deletedNodes[nodeID]; deleted {
		return nil, nil
	}
	if pending, exists := tx.pendingNodes[nodeID]; exists {
		return copyNode(pending), nil
	}
	node, err := tx.getCommittedNodeLocked(nodeID)
	if err == ErrNotFound {
		return nil, nil
	}
	return node, err
}

func (tx *BadgerTransaction) currentEdgeLocked(edgeID EdgeID) (*Edge, error) {
	if _, deleted := tx.deletedEdges[edgeID]; deleted {
		return nil, nil
	}
	if pending, exists := tx.pendingEdges[edgeID]; exists {
		return copyEdge(pending), nil
	}
	edge, err := tx.getCommittedEdgeLocked(edgeID)
	if err == ErrNotFound {
		return nil, nil
	}
	return edge, err
}

func (tx *BadgerTransaction) currentOutgoingEdgesLocked(nodeID NodeID) ([]*Edge, error) {
	committed, err := tx.engine.GetOutgoingEdges(nodeID)
	if err != nil {
		return nil, err
	}
	edges := make(map[EdgeID]*Edge)
	for _, edge := range committed {
		if _, deleted := tx.deletedEdges[edge.ID]; deleted {
			continue
		}
		if pending, exists := tx.pendingEdges[edge.ID]; exists {
			if pending.StartNode == nodeID {
				edges[edge.ID] = copyEdge(pending)
			}
			continue
		}
		if edge.StartNode == nodeID {
			edges[edge.ID] = edge
		}
	}
	for edgeID, edge := range tx.pendingEdges {
		if edge.StartNode == nodeID {
			edges[edgeID] = copyEdge(edge)
		}
	}
	result := make([]*Edge, 0, len(edges))
	for _, edge := range edges {
		result = append(result, edge)
	}
	return result, nil
}

func constraintContractNamespaceForNode(node *Node) (string, bool) {
	if node == nil {
		return "", false
	}
	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	return dbName, ok
}

func constraintContractNamespaceForEdge(edge *Edge) (string, bool) {
	if edge == nil {
		return "", false
	}
	if dbName, _, ok := ParseDatabasePrefix(string(edge.ID)); ok {
		return dbName, true
	}
	if dbName, _, ok := ParseDatabasePrefix(string(edge.StartNode)); ok {
		return dbName, true
	}
	if dbName, _, ok := ParseDatabasePrefix(string(edge.EndNode)); ok {
		return dbName, true
	}
	return "", false
}

func (tx *BadgerTransaction) currentIncomingEdgesLocked(nodeID NodeID) ([]*Edge, error) {
	committed, err := tx.engine.GetIncomingEdges(nodeID)
	if err != nil {
		return nil, err
	}
	edges := make(map[EdgeID]*Edge)
	for _, edge := range committed {
		if _, deleted := tx.deletedEdges[edge.ID]; deleted {
			continue
		}
		if pending, exists := tx.pendingEdges[edge.ID]; exists {
			if pending.EndNode == nodeID {
				edges[edge.ID] = copyEdge(pending)
			}
			continue
		}
		if edge.EndNode == nodeID {
			edges[edge.ID] = edge
		}
	}
	for edgeID, edge := range tx.pendingEdges {
		if edge.EndNode == nodeID {
			edges[edgeID] = copyEdge(edge)
		}
	}
	result := make([]*Edge, 0, len(edges))
	for _, edge := range edges {
		result = append(result, edge)
	}
	return result, nil
}

func (tx *BadgerTransaction) currentAdjacentEdgesLocked(nodeID NodeID) ([]*Edge, error) {
	outgoing, err := tx.currentOutgoingEdgesLocked(nodeID)
	if err != nil {
		return nil, err
	}
	incoming, err := tx.currentIncomingEdgesLocked(nodeID)
	if err != nil {
		return nil, err
	}
	edges := make(map[EdgeID]*Edge)
	for _, edge := range outgoing {
		edges[edge.ID] = edge
	}
	for _, edge := range incoming {
		edges[edge.ID] = edge
	}
	result := make([]*Edge, 0, len(edges))
	for _, edge := range edges {
		result = append(result, edge)
	}
	return result, nil
}
