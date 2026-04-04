package cypher

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var (
	constraintContractNamedNodePattern           = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s*\{(.*)\}\s*$`)
	constraintContractUnnamedNodePattern         = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s*\{(.*)\}\s*$`)
	constraintContractNamedRelationshipPattern   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s*\{(.*)\}\s*$`)
	constraintContractUnnamedRelationshipPattern = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s*\{(.*)\}\s*$`)

	blockPropertyUniquePattern  = regexp.MustCompile(`(?is)^\s*` + ddlVariableToken + `\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+UNIQUE\s*$`)
	blockPropertyNotNullPattern = regexp.MustCompile(`(?is)^\s*` + ddlVariableToken + `\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+NOT\s+NULL\s*$`)
	blockPropertyTypePattern    = regexp.MustCompile(`(?is)^\s*` + ddlVariableToken + `\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+(?:::|TYPED)\s*([A-Z]+(?:\s+[A-Z]+)?)\s*$`)
	blockNodeKeyPattern         = regexp.MustCompile(`(?is)^\s*\(([^)]+)\)\s+IS\s+NODE\s+KEY\s*$`)
	blockRelationshipKeyPattern = regexp.MustCompile(`(?is)^\s*\(([^)]+)\)\s+IS\s+RELATIONSHIP\s+KEY\s*$`)
	blockTemporalPattern        = regexp.MustCompile(`(?is)^\s*\(([^)]+)\)\s+IS\s+TEMPORAL(?:\s+NO\s+OVERLAP)?\s*$`)
)

func (e *StorageExecutor) executeCreateConstraintContract(ctx context.Context, cypher string, ifNotExists bool) (*ExecuteResult, bool, error) {
	trimmed := strings.TrimSpace(cypher)

	if matches := constraintContractNamedNodePattern.FindStringSubmatch(trimmed); matches != nil {
		return e.createConstraintContractForTarget(ctx, cypher, normalizeIdentifierToken(matches[1]), storage.ConstraintEntityNode, normalizeIdentifierToken(matches[2]), normalizeIdentifierToken(matches[3]), matches[4], ifNotExists)
	}
	if matches := constraintContractUnnamedNodePattern.FindStringSubmatch(trimmed); matches != nil {
		name := fmt.Sprintf("constraint_%s_contract", strings.ToLower(normalizeIdentifierToken(matches[2])))
		return e.createConstraintContractForTarget(ctx, cypher, name, storage.ConstraintEntityNode, normalizeIdentifierToken(matches[1]), normalizeIdentifierToken(matches[2]), matches[3], ifNotExists)
	}
	if matches := constraintContractNamedRelationshipPattern.FindStringSubmatch(trimmed); matches != nil {
		return e.createConstraintContractForTarget(ctx, cypher, normalizeIdentifierToken(matches[1]), storage.ConstraintEntityRelationship, normalizeIdentifierToken(matches[2]), normalizeIdentifierToken(matches[3]), matches[4], ifNotExists)
	}
	if matches := constraintContractUnnamedRelationshipPattern.FindStringSubmatch(trimmed); matches != nil {
		name := fmt.Sprintf("constraint_%s_contract", strings.ToLower(normalizeIdentifierToken(matches[2])))
		return e.createConstraintContractForTarget(ctx, cypher, name, storage.ConstraintEntityRelationship, normalizeIdentifierToken(matches[1]), normalizeIdentifierToken(matches[2]), matches[3], ifNotExists)
	}

	return nil, false, nil
}

func (e *StorageExecutor) createConstraintContractForTarget(ctx context.Context, definition, name string, entityType storage.ConstraintEntityType, variable, labelOrType, body string, ifNotExists bool) (*ExecuteResult, bool, error) {
	entriesRaw, err := splitConstraintContractEntries(body)
	if err != nil {
		return nil, true, err
	}
	if len(entriesRaw) == 0 {
		return nil, true, fmt.Errorf("constraint contract requires at least one block entry")
	}

	contract := storage.ConstraintContract{
		Name:              name,
		TargetEntityType:  string(entityType),
		TargetLabelOrType: labelOrType,
		Definition:        strings.TrimSpace(definition),
		Entries:           make([]storage.ConstraintContractEntry, 0, len(entriesRaw)),
	}
	compiledConstraints := make([]storage.Constraint, 0)
	compiledTypes := make([]storage.PropertyTypeConstraint, 0)

	for idx, rawEntry := range entriesRaw {
		entry, compiledConstraint, compiledType, err := e.parseConstraintContractEntry(rawEntry, variable, entityType, labelOrType, name, idx)
		if err != nil {
			return nil, true, err
		}
		contract.Entries = append(contract.Entries, entry)
		if compiledConstraint != nil {
			compiledConstraints = append(compiledConstraints, *compiledConstraint)
		}
		if compiledType != nil {
			compiledTypes = append(compiledTypes, *compiledType)
		}
	}

	for _, constraint := range compiledConstraints {
		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, true, err
		}
	}
	for _, constraint := range compiledTypes {
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, true, err
		}
	}
	if err := storage.ValidateConstraintContractOnCreationForEngine(e.storage, contract); err != nil {
		return nil, true, err
	}
	if err := e.storage.GetSchema().AddConstraintContractBundle(contract, compiledConstraints, compiledTypes, ifNotExists); err != nil {
		return nil, true, err
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, true, nil
}

func splitConstraintContractEntries(body string) ([]string, error) {
	entries := make([]string, 0)
	var current strings.Builder
	braceDepth := 0
	bracketDepth := 0
	parenDepth := 0
	quote := rune(0)

	flush := func() {
		entry := strings.TrimSpace(current.String())
		if entry != "" {
			entries = append(entries, entry)
		}
		current.Reset()
	}

	for _, ch := range body {
		switch {
		case quote != 0:
			current.WriteRune(ch)
			if ch == quote {
				quote = 0
			}
		case ch == '\'' || ch == '"':
			quote = ch
			current.WriteRune(ch)
		case ch == '{':
			braceDepth++
			current.WriteRune(ch)
		case ch == '}':
			if braceDepth == 0 {
				return nil, fmt.Errorf("malformed REQUIRE block")
			}
			braceDepth--
			current.WriteRune(ch)
		case ch == '[':
			bracketDepth++
			current.WriteRune(ch)
		case ch == ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
			current.WriteRune(ch)
		case ch == '(':
			parenDepth++
			current.WriteRune(ch)
		case ch == ')':
			if parenDepth > 0 {
				parenDepth--
			}
			current.WriteRune(ch)
		case ch == '\n' || ch == ';':
			if braceDepth == 0 && bracketDepth == 0 && parenDepth == 0 {
				flush()
				continue
			}
			current.WriteRune(ch)
		default:
			current.WriteRune(ch)
		}
	}
	if quote != 0 || braceDepth != 0 || bracketDepth != 0 {
		return nil, fmt.Errorf("malformed REQUIRE block")
	}
	flush()
	return entries, nil
}

func isNestedConstraintContractEntry(entryText string) bool {
	trimmed := strings.TrimSpace(entryText)
	if !matchKeywordAt(trimmed, 0, "FOR") {
		return false
	}
	return hasKeywordFollowedByBrace(trimmed, "REQUIRE")
}

func nestedConstraintContractEntryError(entryText string) error {
	return fmt.Errorf(
		"nested FOR ... REQUIRE entries are not supported inside REQUIRE blocks; create a separate targeted block constraint such as %s",
		strings.TrimSpace(entryText),
	)
}

func (e *StorageExecutor) parseConstraintContractEntry(rawEntry, variable string, entityType storage.ConstraintEntityType, labelOrType, contractName string, index int) (storage.ConstraintContractEntry, *storage.Constraint, *storage.PropertyTypeConstraint, error) {
	entryText := strings.TrimSpace(rawEntry)
	if isNestedConstraintContractEntry(entryText) {
		return storage.ConstraintContractEntry{}, nil, nil, nestedConstraintContractEntryError(entryText)
	}

	entryName := fmt.Sprintf("%s__entry_%02d", contractName, index+1)
	if compiledConstraint, contractEntry, ok, err := e.parseConstraintContractPrimitive(entryText, variable, entityType, labelOrType, entryName); ok || err != nil {
		return contractEntry, compiledConstraint, nil, err
	}
	if compiledType, contractEntry, ok, err := e.parseConstraintContractPropertyType(entryText, variable, entityType, labelOrType, entryName); ok || err != nil {
		return contractEntry, nil, compiledType, err
	}

	kind := storage.ConstraintContractKindBooleanNode
	if entityType == storage.ConstraintEntityRelationship {
		kind = storage.ConstraintContractKindBooleanRelationship
	}
	return storage.ConstraintContractEntry{Kind: kind, Expression: entryText}, nil, nil, nil
}

func (e *StorageExecutor) parseConstraintContractPrimitive(entryText, variable string, entityType storage.ConstraintEntityType, labelOrType, entryName string) (*storage.Constraint, storage.ConstraintContractEntry, bool, error) {
	if matches := blockPropertyUniquePattern.FindStringSubmatch(entryText); matches != nil {
		property := normalizeIdentifierToken(matches[1])
		constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintUnique, EntityType: entityType, Label: labelOrType, Properties: []string{property}}
		kind := storage.ConstraintContractKindPrimitiveNode
		if entityType == storage.ConstraintEntityRelationship {
			kind = storage.ConstraintContractKindPrimitiveRelationship
		}
		return constraint, storage.ConstraintContractEntry{Kind: kind, PrimitiveType: string(storage.ConstraintUnique), Property: property, Properties: []string{property}, Expression: entryText}, true, nil
	}
	if matches := blockPropertyNotNullPattern.FindStringSubmatch(entryText); matches != nil {
		property := normalizeIdentifierToken(matches[1])
		constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintExists, EntityType: entityType, Label: labelOrType, Properties: []string{property}}
		kind := storage.ConstraintContractKindPrimitiveNode
		if entityType == storage.ConstraintEntityRelationship {
			kind = storage.ConstraintContractKindPrimitiveRelationship
		}
		return constraint, storage.ConstraintContractEntry{Kind: kind, PrimitiveType: string(storage.ConstraintExists), Property: property, Properties: []string{property}, Expression: entryText}, true, nil
	}
	if entityType == storage.ConstraintEntityNode {
		if matches := blockNodeKeyPattern.FindStringSubmatch(entryText); matches != nil {
			properties := e.parseConstraintProperties(matches[1])
			if len(properties) == 0 {
				return nil, storage.ConstraintContractEntry{}, true, fmt.Errorf("NODE KEY constraint requires properties")
			}
			constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintNodeKey, EntityType: entityType, Label: labelOrType, Properties: properties}
			return constraint, storage.ConstraintContractEntry{Kind: storage.ConstraintContractKindPrimitiveNode, PrimitiveType: string(storage.ConstraintNodeKey), Properties: properties, Expression: entryText}, true, nil
		}
		if matches := blockTemporalPattern.FindStringSubmatch(entryText); matches != nil {
			properties := e.parseConstraintProperties(matches[1])
			if len(properties) != 3 {
				return nil, storage.ConstraintContractEntry{}, true, fmt.Errorf("TEMPORAL constraint requires 3 properties (key, valid_from, valid_to)")
			}
			constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintTemporal, EntityType: entityType, Label: labelOrType, Properties: properties}
			return constraint, storage.ConstraintContractEntry{Kind: storage.ConstraintContractKindPrimitiveNode, PrimitiveType: string(storage.ConstraintTemporal), Properties: properties, Expression: entryText}, true, nil
		}
	}
	if entityType == storage.ConstraintEntityRelationship {
		if matches := blockRelationshipKeyPattern.FindStringSubmatch(entryText); matches != nil {
			properties := e.parseConstraintProperties(matches[1])
			if len(properties) == 0 {
				return nil, storage.ConstraintContractEntry{}, true, fmt.Errorf("RELATIONSHIP KEY constraint requires properties")
			}
			constraint := &storage.Constraint{Name: entryName, Type: storage.ConstraintRelationshipKey, EntityType: entityType, Label: labelOrType, Properties: properties}
			return constraint, storage.ConstraintContractEntry{Kind: storage.ConstraintContractKindPrimitiveRelationship, PrimitiveType: string(storage.ConstraintRelationshipKey), Properties: properties, Expression: entryText}, true, nil
		}
	}

	return nil, storage.ConstraintContractEntry{}, false, nil
}

func (e *StorageExecutor) parseConstraintContractPropertyType(entryText, variable string, entityType storage.ConstraintEntityType, labelOrType, entryName string) (*storage.PropertyTypeConstraint, storage.ConstraintContractEntry, bool, error) {
	matches := blockPropertyTypePattern.FindStringSubmatch(entryText)
	if matches == nil {
		return nil, storage.ConstraintContractEntry{}, false, nil
	}
	property := normalizeIdentifierToken(matches[1])
	expectedType, err := parsePropertyType(matches[2])
	if err != nil {
		return nil, storage.ConstraintContractEntry{}, true, err
	}
	kind := storage.ConstraintContractKindPrimitiveNode
	if entityType == storage.ConstraintEntityRelationship {
		kind = storage.ConstraintContractKindPrimitiveRelationship
	}
	constraint := &storage.PropertyTypeConstraint{Name: entryName, EntityType: entityType, Label: labelOrType, Property: property, ExpectedType: expectedType}
	return constraint, storage.ConstraintContractEntry{Kind: kind, PrimitiveType: string(storage.ConstraintPropertyType), Property: property, Properties: []string{property}, ExpectedType: string(expectedType), Expression: entryText}, true, nil
}
