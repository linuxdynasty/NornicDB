// Schema command parsing and execution for Cypher.
//
// This file implements Neo4j schema management commands:
//   - CREATE CONSTRAINT
//   - CREATE INDEX
//   - CREATE RANGE INDEX
//   - CREATE FULLTEXT INDEX
//   - CREATE VECTOR INDEX
package cypher

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var (
	identifierCompatPattern  = `(?:` + "`" + `[^` + "`" + `]+` + "`" + `|[A-Za-z_][A-Za-z0-9_]*)`
	createIndexLegacyPattern = regexp.MustCompile(
		`(?i)^CREATE\s+INDEX(?:\s+(` + identifierCompatPattern + `))?(?:\s+IF\s+NOT\s+EXISTS)?\s+ON\s+:(` + identifierCompatPattern + `)\s*\(([^)]+)\)\s*$`)
	createIndexForCompatPattern = regexp.MustCompile(
		`(?i)^CREATE\s+INDEX(?:\s+(` + identifierCompatPattern + `))?(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(\w+)\s*:\s*(` + identifierCompatPattern + `)\s*\)\s+ON\s+(.+)$`)
	fulltextIndexCompatPattern = regexp.MustCompile(
		`(?i)^CREATE\s+FULLTEXT\s+INDEX\s+(` + identifierCompatPattern + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(?\s*(\w+)\s*:\s*(` + identifierCompatPattern + `)\s*\)?\s+ON(?:\s+EACH)?\s+(.+)$`)
)

// executeSchemaCommand handles CREATE CONSTRAINT and CREATE INDEX commands.
func (e *StorageExecutor) executeSchemaCommand(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	// Order matters: check more specific patterns first
	if strings.Contains(upper, "CREATE CONSTRAINT") {
		return e.executeCreateConstraint(ctx, cypher)
	} else if strings.Contains(upper, "DROP CONSTRAINT") {
		return e.executeDropConstraint(ctx, cypher)
	} else if strings.Contains(upper, "CREATE FULLTEXT INDEX") {
		return e.executeCreateFulltextIndex(ctx, cypher)
	} else if strings.Contains(upper, "CREATE VECTOR INDEX") {
		return e.executeCreateVectorIndex(ctx, cypher)
	} else if strings.Contains(upper, "CREATE RANGE INDEX") {
		return e.executeCreateRangeIndex(ctx, cypher)
	} else if strings.Contains(upper, "CREATE INDEX") {
		return e.executeCreateIndex(ctx, cypher)
	}

	return nil, fmt.Errorf("unknown schema command: %s", cypher)
}

// executeCreateConstraint handles CREATE CONSTRAINT commands.
//
// Supported syntax (Neo4j 5.x):
//
//	CREATE CONSTRAINT constraint_name IF NOT EXISTS FOR (n:Label) REQUIRE n.property IS UNIQUE
//
// Supported syntax (Neo4j 4.x):
//
//	CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT n.property IS UNIQUE
func (e *StorageExecutor) executeCreateConstraint(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// NODE KEY constraints (Neo4j 5.x)
	if matches := constraintNamedForRequireNodeKey.FindStringSubmatch(cypher); matches != nil {
		constraintName := matches[1]
		label := matches[3]
		properties := e.parseConstraintProperties(matches[4])
		if len(properties) == 0 {
			return nil, fmt.Errorf("NODE KEY constraint requires properties")
		}

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintNodeKey,
			Label:      label,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintUnnamedForRequireNodeKey.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		properties := e.parseConstraintProperties(matches[3])
		if len(properties) == 0 {
			return nil, fmt.Errorf("NODE KEY constraint requires properties")
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_node_key", strings.ToLower(label), strings.ToLower(strings.Join(properties, "_")))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintNodeKey,
			Label:      label,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintOnAssertNodeKey.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		properties := e.parseConstraintProperties(matches[3])
		if len(properties) == 0 {
			return nil, fmt.Errorf("NODE KEY constraint requires properties")
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_node_key", strings.ToLower(label), strings.ToLower(strings.Join(properties, "_")))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintNodeKey,
			Label:      label,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Temporal no-overlap constraints (NornicDB extension)
	if matches := constraintNamedForRequireTemporal.FindStringSubmatch(cypher); matches != nil {
		constraintName := matches[1]
		label := matches[3]
		properties := e.parseConstraintProperties(matches[4])
		if len(properties) != 3 {
			return nil, fmt.Errorf("TEMPORAL constraint requires 3 properties (key, valid_from, valid_to)")
		}

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintTemporal,
			Label:      label,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintUnnamedForRequireTemporal.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		properties := e.parseConstraintProperties(matches[3])
		if len(properties) != 3 {
			return nil, fmt.Errorf("TEMPORAL constraint requires 3 properties (key, valid_from, valid_to)")
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_temporal", strings.ToLower(label), strings.ToLower(strings.Join(properties, "_")))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintTemporal,
			Label:      label,
			Properties: properties,
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// EXISTS / NOT NULL constraints
	if matches := constraintNamedForRequireNotNull.FindStringSubmatch(cypher); matches != nil {
		constraintName := matches[1]
		label := matches[3]
		property := matches[5]
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintExists,
			Label:      label,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintUnnamedForRequireNotNull.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		property := matches[4]
		constraintName := fmt.Sprintf("constraint_%s_%s_exists", strings.ToLower(label), strings.ToLower(property))
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintExists,
			Label:      label,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintOnAssertExists.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		property := matches[4]
		constraintName := fmt.Sprintf("constraint_%s_%s_exists", strings.ToLower(label), strings.ToLower(property))
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintExists,
			Label:      label,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintOnAssertNotNull.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		property := matches[4]
		constraintName := fmt.Sprintf("constraint_%s_%s_exists", strings.ToLower(label), strings.ToLower(property))
		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintExists,
			Label:      label,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddConstraint(constraint); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Property type constraints
	if matches := constraintNamedForRequireType.FindStringSubmatch(cypher); matches != nil {
		constraintName := matches[1]
		label := matches[3]
		property := matches[5]
		expectedType, err := parsePropertyType(matches[6])
		if err != nil {
			return nil, err
		}
		ptc := storage.PropertyTypeConstraint{
			Name:         constraintName,
			Label:        label,
			Property:     property,
			ExpectedType: expectedType,
		}
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(e.storage, ptc); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddPropertyTypeConstraint(constraintName, label, property, expectedType); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintUnnamedForRequireType.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		property := matches[4]
		expectedType, err := parsePropertyType(matches[5])
		if err != nil {
			return nil, err
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_type", strings.ToLower(label), strings.ToLower(property))
		ptc := storage.PropertyTypeConstraint{
			Name:         constraintName,
			Label:        label,
			Property:     property,
			ExpectedType: expectedType,
		}
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(e.storage, ptc); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddPropertyTypeConstraint(constraintName, label, property, expectedType); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	if matches := constraintOnAssertType.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		property := matches[4]
		expectedType, err := parsePropertyType(matches[5])
		if err != nil {
			return nil, err
		}
		constraintName := fmt.Sprintf("constraint_%s_%s_type", strings.ToLower(label), strings.ToLower(property))
		ptc := storage.PropertyTypeConstraint{
			Name:         constraintName,
			Label:        label,
			Property:     property,
			ExpectedType: expectedType,
		}
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(e.storage, ptc); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddPropertyTypeConstraint(constraintName, label, property, expectedType); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Pattern 1 (Neo4j 5.x): CREATE CONSTRAINT name IF NOT EXISTS FOR (n:Label) REQUIRE n.property IS UNIQUE
	// Uses pre-compiled pattern from regex_patterns.go
	if matches := constraintNamedForRequire.FindStringSubmatch(cypher); matches != nil {
		constraintName := matches[1]
		label := matches[3]
		property := matches[5]

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintUnique,
			Label:      label,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddUniqueConstraint(constraintName, label, property); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Pattern 2 (Neo4j 5.x without name): CREATE CONSTRAINT IF NOT EXISTS FOR (n:Label) REQUIRE n.property IS UNIQUE
	// Uses pre-compiled pattern from regex_patterns.go
	if matches := constraintUnnamedForRequire.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		property := matches[4]
		constraintName := fmt.Sprintf("constraint_%s_%s", strings.ToLower(label), strings.ToLower(property))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintUnique,
			Label:      label,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddUniqueConstraint(constraintName, label, property); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Pattern 3 (Neo4j 4.x): CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT n.property IS UNIQUE
	// Uses pre-compiled pattern from regex_patterns.go
	if matches := constraintOnAssert.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		property := matches[4]
		constraintName := fmt.Sprintf("constraint_%s_%s", strings.ToLower(label), strings.ToLower(property))

		constraint := storage.Constraint{
			Name:       constraintName,
			Type:       storage.ConstraintUnique,
			Label:      label,
			Properties: []string{property},
		}

		if err := storage.ValidateConstraintOnCreationForEngine(e.storage, constraint); err != nil {
			return nil, err
		}
		if err := e.storage.GetSchema().AddUniqueConstraint(constraintName, label, property); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	return nil, fmt.Errorf("invalid CREATE CONSTRAINT syntax")
}

// executeDropConstraint handles DROP CONSTRAINT commands.
func (e *StorageExecutor) executeDropConstraint(ctx context.Context, cypher string) (*ExecuteResult, error) {
	matches := dropConstraintPattern.FindStringSubmatch(cypher)
	if matches == nil {
		return nil, fmt.Errorf("invalid DROP CONSTRAINT syntax")
	}
	name := matches[1]

	if err := e.storage.GetSchema().DropConstraint(name); err != nil {
		// If IF EXISTS was used, swallow missing constraint errors.
		if strings.Contains(strings.ToUpper(cypher), "IF EXISTS") && strings.Contains(err.Error(), "does not exist") {
			return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
		}
		return nil, err
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// executeCreateIndex handles CREATE INDEX commands.
//
// Supported syntax:
//
//	CREATE INDEX index_name IF NOT EXISTS FOR (n:Label) ON (n.property)
//	CREATE INDEX index_name IF NOT EXISTS FOR (n:Label) ON (n.prop1, n.prop2)
//	CREATE INDEX IF NOT EXISTS FOR (n:Label) ON (n.prop1, n.prop2, n.prop3)
//
// Supports both single-property and composite (multi-property) indexes.
func (e *StorageExecutor) executeCreateIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Pattern: CREATE INDEX name IF NOT EXISTS FOR (n:Label) ON (n.property[, n.property2, ...])
	// Uses pre-compiled patterns from regex_patterns.go
	if matches := indexNamedFor.FindStringSubmatch(cypher); matches != nil {
		indexName := matches[1]
		label := matches[3]
		propertiesStr := matches[4] // e.g., "n.prop1, n.prop2"

		// Parse properties (single or multiple)
		properties := e.parseQualifiedIndexProperties(propertiesStr)
		if len(properties) == 0 {
			return nil, fmt.Errorf("no properties specified for index")
		}

		// Add index to schema (supports composite indexes)
		if err := e.storage.GetSchema().AddPropertyIndex(indexName, label, properties); err != nil {
			return nil, err
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Try without index name
	if matches := indexUnnamedFor.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		propertiesStr := matches[3] // e.g., "n.prop1, n.prop2"

		// Parse properties
		properties := e.parseQualifiedIndexProperties(propertiesStr)
		if len(properties) == 0 {
			return nil, fmt.Errorf("no properties specified for index")
		}

		// Generate index name based on label and properties
		propsJoined := strings.Join(properties, "_")
		indexName := fmt.Sprintf("index_%s_%s", strings.ToLower(label), strings.ToLower(propsJoined))

		// Add index
		if err := e.storage.GetSchema().AddPropertyIndex(indexName, label, properties); err != nil {
			return nil, err
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Neo4j legacy syntax: CREATE INDEX [name] ON :Label(prop[, prop2])
	if matches := createIndexLegacyPattern.FindStringSubmatch(strings.TrimSpace(cypher)); matches != nil {
		indexName := normalizeIdentifierToken(matches[1])
		label := normalizeIdentifierToken(matches[2])
		properties := e.parseIndexProperties(matches[3])
		if len(properties) == 0 {
			return nil, fmt.Errorf("no properties specified for index")
		}
		if indexName == "" {
			propsJoined := strings.Join(properties, "_")
			indexName = fmt.Sprintf("index_%s_%s", strings.ToLower(label), strings.ToLower(propsJoined))
		}
		if err := e.storage.GetSchema().AddPropertyIndex(indexName, label, properties); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Neo4j 5 compat variant: CREATE INDEX [name] FOR (n:Label) ON n.prop
	if matches := createIndexForCompatPattern.FindStringSubmatch(strings.TrimSpace(cypher)); matches != nil {
		indexName := normalizeIdentifierToken(matches[1])
		label := normalizeIdentifierToken(matches[3])
		onPart := strings.TrimSpace(matches[4])
		if strings.HasPrefix(onPart, "(") && strings.HasSuffix(onPart, ")") {
			onPart = strings.TrimSpace(onPart[1 : len(onPart)-1])
		}
		properties := e.parseQualifiedIndexProperties(onPart)
		if len(properties) == 0 {
			return nil, fmt.Errorf("no properties specified for index")
		}
		if indexName == "" {
			propsJoined := strings.Join(properties, "_")
			indexName = fmt.Sprintf("index_%s_%s", strings.ToLower(label), strings.ToLower(propsJoined))
		}
		if err := e.storage.GetSchema().AddPropertyIndex(indexName, label, properties); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	return nil, fmt.Errorf("invalid CREATE INDEX syntax")
}

// executeCreateRangeIndex handles CREATE RANGE INDEX commands.
//
// Supported syntax:
//
//	CREATE RANGE INDEX index_name IF NOT EXISTS FOR (n:Label) ON (n.property)
//
// Range indexes optimize queries with range predicates (>, <, >=, <=, BETWEEN).
func (e *StorageExecutor) executeCreateRangeIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Reuse generic CREATE INDEX regexes by normalizing RANGE syntax.
	normalized := strings.Replace(strings.ToUpper(cypher), "CREATE RANGE INDEX", "CREATE INDEX", 1)

	// Pattern: CREATE RANGE INDEX name IF NOT EXISTS FOR (n:Label) ON (n.property)
	// Reuse the standard index pattern - same structure
	if matches := indexNamedFor.FindStringSubmatch(normalized); matches != nil {
		indexName := matches[1]
		label := matches[3]
		propertiesStr := matches[4]

		// Range index only supports single property
		properties := e.parseQualifiedIndexProperties(propertiesStr)
		if len(properties) != 1 {
			return nil, fmt.Errorf("RANGE INDEX only supports single property, got %d", len(properties))
		}

		err := e.storage.GetSchema().AddRangeIndex(indexName, label, properties[0])
		if err != nil {
			return nil, fmt.Errorf("failed to create range index: %w", err)
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Unnamed range index
	if matches := indexUnnamedFor.FindStringSubmatch(normalized); matches != nil {
		label := matches[2]
		propertiesStr := matches[3]

		properties := e.parseQualifiedIndexProperties(propertiesStr)
		if len(properties) != 1 {
			return nil, fmt.Errorf("RANGE INDEX only supports single property, got %d", len(properties))
		}

		indexName := fmt.Sprintf("range_idx_%s_%s", strings.ToLower(label), properties[0])
		err := e.storage.GetSchema().AddRangeIndex(indexName, label, properties[0])
		if err != nil {
			return nil, fmt.Errorf("failed to create range index: %w", err)
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	return nil, fmt.Errorf("invalid CREATE RANGE INDEX syntax")
}

// parseIndexProperties parses property list from index ON clause.
//
// Handles both single and composite property syntax:
//   - "n.property"           -> ["property"]
//   - "n.prop1, n.prop2"     -> ["prop1", "prop2"]
//   - "n.a, n.b, n.c"        -> ["a", "b", "c"]
func (e *StorageExecutor) parseIndexProperties(propertiesStr string) []string {
	return e.parseIndexPropertiesWithMode(propertiesStr, true)
}

func (e *StorageExecutor) parseQualifiedIndexProperties(propertiesStr string) []string {
	return e.parseIndexPropertiesWithMode(propertiesStr, false)
}

func (e *StorageExecutor) parseIndexPropertiesWithMode(propertiesStr string, allowBare bool) []string {
	// Split by comma
	parts := strings.Split(propertiesStr, ",")
	var properties []string

	for _, part := range parts {
		part = strings.TrimSpace(part)
		// Extract property name after dot (e.g., "n.prop" -> "prop")
		if dotIdx := strings.LastIndex(part, "."); dotIdx >= 0 && dotIdx < len(part)-1 {
			propName := normalizeIdentifierToken(part[dotIdx+1:])
			if propName != "" {
				properties = append(properties, propName)
			}
		} else if allowBare {
			// Also support bare property names used by legacy syntax ON :Label(prop)
			propName := normalizeIdentifierToken(part)
			if propName != "" {
				properties = append(properties, propName)
			}
		}
	}

	return properties
}

func (e *StorageExecutor) parseConstraintProperties(propertiesStr string) []string {
	parts := strings.Split(propertiesStr, ",")
	properties := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if dotIdx := strings.LastIndex(part, "."); dotIdx >= 0 && dotIdx < len(part)-1 {
			propName := strings.TrimSpace(part[dotIdx+1:])
			if propName != "" {
				properties = append(properties, propName)
			}
		}
	}
	return properties
}

func parsePropertyType(typeName string) (storage.PropertyType, error) {
	switch strings.ToUpper(strings.TrimSpace(typeName)) {
	case "STRING":
		return storage.PropertyTypeString, nil
	case "INTEGER", "INT":
		return storage.PropertyTypeInteger, nil
	case "FLOAT":
		return storage.PropertyTypeFloat, nil
	case "BOOLEAN", "BOOL":
		return storage.PropertyTypeBoolean, nil
	case "DATE":
		return storage.PropertyTypeDate, nil
	case "DATETIME", "ZONED DATETIME", "ZONEDDATETIME":
		return storage.PropertyTypeZonedDateTime, nil
	case "LOCAL DATETIME", "LOCALDATETIME":
		return storage.PropertyTypeLocalDateTime, nil
	default:
		return "", fmt.Errorf("unsupported property type: %s", typeName)
	}
}

// executeCreateFulltextIndex handles CREATE FULLTEXT INDEX commands.
//
// Supported syntax:
//
//	CREATE FULLTEXT INDEX index_name IF NOT EXISTS
//	FOR (n:Label) ON EACH [n.prop1, n.prop2]
func (e *StorageExecutor) executeCreateFulltextIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Pattern: CREATE FULLTEXT INDEX name IF NOT EXISTS FOR (n:Label) ON EACH [n.prop1, n.prop2]
	// Uses pre-compiled pattern from regex_patterns.go
	matches := fulltextIndexPattern.FindStringSubmatch(cypher)

	if matches == nil {
		compat := fulltextIndexCompatPattern.FindStringSubmatch(strings.TrimSpace(cypher))
		if compat == nil {
			return nil, fmt.Errorf("invalid CREATE FULLTEXT INDEX syntax: %s", cypher)
		}

		indexName := normalizeIdentifierToken(compat[1])
		label := normalizeIdentifierToken(compat[3])
		propsRaw := strings.TrimSpace(compat[4])
		if strings.HasPrefix(propsRaw, "[") && strings.HasSuffix(propsRaw, "]") {
			propsRaw = strings.TrimSpace(propsRaw[1 : len(propsRaw)-1])
		}
		properties := e.parseQualifiedIndexProperties(propsRaw)
		if len(properties) == 0 {
			return nil, fmt.Errorf("no properties found in fulltext index definition")
		}
		schema := e.storage.GetSchema()
		if schema == nil {
			return nil, fmt.Errorf("schema manager not available")
		}
		if err := schema.AddFulltextIndex(indexName, []string{label}, properties); err != nil {
			return nil, fmt.Errorf("failed to add fulltext index: %w", err)
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	indexName := matches[1]
	label := matches[3]
	propertiesStr := matches[4]

	// Parse properties: "n.prop1, n.prop2" -> ["prop1", "prop2"]
	properties := e.parseQualifiedIndexProperties(propertiesStr)

	if len(properties) == 0 {
		return nil, fmt.Errorf("no properties found in fulltext index definition")
	}

	// Add fulltext index
	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, fmt.Errorf("schema manager not available")
	}

	if err := schema.AddFulltextIndex(indexName, []string{label}, properties); err != nil {
		return nil, fmt.Errorf("failed to add fulltext index: %w", err)
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

func normalizeIdentifierToken(v string) string {
	s := strings.TrimSpace(v)
	if len(s) >= 2 && strings.HasPrefix(s, "`") && strings.HasSuffix(s, "`") {
		s = s[1 : len(s)-1]
		s = strings.ReplaceAll(s, "``", "`")
	}
	return strings.TrimSpace(s)
}

// executeCreateVectorIndex handles CREATE VECTOR INDEX commands.
//
// Supported syntax:
//
//	CREATE VECTOR INDEX index_name IF NOT EXISTS
//	FOR (n:Label) ON (n.property)
//	OPTIONS {indexConfig: {`vector.dimensions`: 1024, `vector.similarity_function`: 'cosine'}}
func (e *StorageExecutor) executeCreateVectorIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Pattern: CREATE VECTOR INDEX name IF NOT EXISTS FOR (n:Label) ON (n.property)
	// Uses pre-compiled patterns from regex_patterns.go
	matches := vectorIndexPattern.FindStringSubmatch(cypher)

	if matches == nil {
		return nil, fmt.Errorf("invalid CREATE VECTOR INDEX syntax")
	}

	indexName := matches[1]
	label := matches[3]
	property := matches[5]

	// Parse OPTIONS if present - use configured default dimensions
	dimensions := e.GetDefaultEmbeddingDimensions()
	similarityFunc := "cosine" // Default

	if strings.Contains(cypher, "OPTIONS") {
		// Extract dimensions using pre-compiled pattern
		if dimMatches := vectorDimensionsPattern.FindStringSubmatch(cypher); dimMatches != nil {
			if dim, err := strconv.Atoi(dimMatches[1]); err == nil {
				dimensions = dim
			}
		}

		// Extract similarity function using pre-compiled pattern
		if simMatches := vectorSimilarityPattern.FindStringSubmatch(cypher); simMatches != nil {
			similarityFunc = simMatches[1]
		}
	}

	// Add vector index
	if err := e.storage.GetSchema().AddVectorIndex(indexName, label, property, dimensions, similarityFunc); err != nil {
		return nil, err
	}

	e.registerVectorSpace(indexName, label, property, dimensions, similarityFunc)

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}
