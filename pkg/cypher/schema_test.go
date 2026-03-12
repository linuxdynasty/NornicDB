package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type nilSchemaEngine struct {
	storage.Engine
}

func (n *nilSchemaEngine) GetSchema() *storage.SchemaManager { return nil }

func TestCreateUniqueConstraint(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create unique constraint
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE", nil)
	if err != nil {
		t.Fatalf("Failed to create constraint: %v", err)
	}

	// Verify constraint exists
	constraints := store.GetSchema().GetConstraints()
	if len(constraints) != 1 {
		t.Fatalf("Expected 1 constraint, got %d", len(constraints))
	}
	if constraints[0].Label != "Node" || constraints[0].Property != "id" {
		t.Errorf("Unexpected constraint: Label=%s, Property=%s", constraints[0].Label, constraints[0].Property)
	}

	// Test constraint enforcement - first node should succeed
	_, err = exec.Execute(ctx, "CREATE (n:Node {id: 'test-1', name: 'Test'})", nil)
	if err != nil {
		t.Fatalf("Failed to create first node: %v", err)
	}

	// Second node with same ID should fail
	_, err = exec.Execute(ctx, "CREATE (n:Node {id: 'test-1', name: 'Test2'})", nil)
	if err == nil {
		t.Fatal("Expected constraint violation, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "constraint violation") {
		t.Errorf("Expected constraint violation error, got: %v", err)
	}
}

func TestCreateIndex(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create property index
	_, err := exec.Execute(ctx, "CREATE INDEX node_type IF NOT EXISTS FOR (n:Node) ON (n.type)", nil)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Verify index exists
	indexes := store.GetSchema().GetIndexes()
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}
}

func TestCreateFulltextIndex(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create fulltext index
	query := "CREATE FULLTEXT INDEX node_search IF NOT EXISTS FOR (n:Node) ON EACH [n.properties]"
	_, err := exec.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Failed to create fulltext index: %v", err)
	}

	// Verify index exists
	indexes := store.GetSchema().GetIndexes()
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}

	idx := indexes[0].(map[string]interface{})
	if idx["type"] != "FULLTEXT" {
		t.Errorf("Expected FULLTEXT index, got: %v", idx["type"])
	}
}

func TestCreateVectorIndex(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create vector index
	query := `CREATE VECTOR INDEX node_embedding_index IF NOT EXISTS
		FOR (n:Node) ON (n.embedding)
		OPTIONS {indexConfig: {` + "`vector.dimensions`" + `: 1024}}`
	_, err := exec.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Failed to create vector index: %v", err)
	}

	// Verify index exists
	indexes := store.GetSchema().GetIndexes()
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}
}

func TestMimirInitialization(t *testing.T) {
	// Test the actual Mimir initialization queries
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Unique constraint on node IDs
	_, err := exec.Execute(ctx, `CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE`, nil)
	if err != nil {
		t.Fatalf("Failed to create node_id_unique constraint: %v", err)
	}

	// Full-text search index
	_, err = exec.Execute(ctx, `CREATE FULLTEXT INDEX node_search IF NOT EXISTS FOR (n:Node) ON EACH [n.properties]`, nil)
	if err != nil {
		t.Fatalf("Failed to create node_search index: %v", err)
	}

	// Type index for fast filtering
	_, err = exec.Execute(ctx, `CREATE INDEX node_type IF NOT EXISTS FOR (n:Node) ON (n.type)`, nil)
	if err != nil {
		t.Fatalf("Failed to create node_type index: %v", err)
	}

	// Vector index
	_, err = exec.Execute(ctx, `CREATE VECTOR INDEX node_embedding_index IF NOT EXISTS FOR (n:Node) ON (n.embedding) OPTIONS {indexConfig: {`+"`vector.dimensions`"+`: 1024}}`, nil)
	if err != nil {
		t.Fatalf("Failed to create node_embedding_index: %v", err)
	}

	// Verify all schemas created
	constraints := store.GetSchema().GetConstraints()
	if len(constraints) != 1 {
		t.Errorf("Expected 1 constraint, got %d", len(constraints))
	}

	indexes := store.GetSchema().GetIndexes()
	if len(indexes) != 3 {
		t.Errorf("Expected 3 indexes, got %d", len(indexes))
	}

	// Test that constraint works
	_, err = exec.Execute(ctx, "CREATE (n:Node {id: 'test-1'})", nil)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Duplicate should fail
	_, err = exec.Execute(ctx, "CREATE (n:Node {id: 'test-1'})", nil)
	if err == nil {
		t.Fatal("Expected constraint violation for duplicate ID")
	}
}

func TestConstraintWithoutName(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create constraint without explicit name
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE n.email IS UNIQUE", nil)
	if err != nil {
		t.Fatalf("Failed to create constraint: %v", err)
	}

	// Verify constraint exists with generated name
	constraints := store.GetSchema().GetConstraints()
	if len(constraints) != 1 {
		t.Fatalf("Expected 1 constraint, got %d", len(constraints))
	}
}

func TestIndexWithoutName(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create index without explicit name
	_, err := exec.Execute(ctx, "CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.name)", nil)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Verify index exists
	indexes := store.GetSchema().GetIndexes()
	if len(indexes) != 1 {
		t.Fatalf("Expected 1 index, got %d", len(indexes))
	}
}

func TestIdempotentSchemaCreation(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create constraint twice - should not error with IF NOT EXISTS
	query := "CREATE CONSTRAINT test_constraint IF NOT EXISTS FOR (n:Test) REQUIRE n.id IS UNIQUE"

	_, err := exec.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("First constraint creation failed: %v", err)
	}

	_, err = exec.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Second constraint creation failed: %v", err)
	}

	// Should still have only one constraint
	constraints := store.GetSchema().GetConstraints()
	if len(constraints) != 1 {
		t.Errorf("Expected 1 constraint, got %d", len(constraints))
	}
}

func TestSchemaErrorCases(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	t.Run("InvalidConstraintSyntax", func(t *testing.T) {
		// Missing REQUIRE clause
		_, err := exec.Execute(ctx, "CREATE CONSTRAINT test FOR (n:Node)", nil)
		if err == nil {
			t.Error("Expected error for invalid syntax")
		}
	})

	t.Run("InvalidIndexSyntax", func(t *testing.T) {
		// Missing ON clause
		_, err := exec.Execute(ctx, "CREATE INDEX test FOR (n:Node)", nil)
		if err == nil {
			t.Error("Expected error for invalid syntax")
		}
	})

	t.Run("InvalidFulltextSyntax", func(t *testing.T) {
		// Missing ON EACH clause
		_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX test FOR (n:Node)", nil)
		if err == nil {
			t.Error("Expected error for invalid syntax")
		}
	})

	t.Run("InvalidVectorSyntax", func(t *testing.T) {
		// Missing ON clause
		_, err := exec.Execute(ctx, "CREATE VECTOR INDEX test FOR (n:Node)", nil)
		if err == nil {
			t.Error("Expected error for invalid syntax")
		}
	})

	t.Run("FulltextNoPropertiesFound", func(t *testing.T) {
		// Property token missing "n." should parse as zero extracted properties.
		_, err := exec.executeCreateFulltextIndex(ctx, "CREATE FULLTEXT INDEX bad_ft FOR (n:Node) ON EACH [content]")
		if err == nil || !strings.Contains(err.Error(), "no properties found in fulltext index definition") {
			t.Fatalf("expected no properties error, got: %v", err)
		}
	})

	t.Run("DuplicateFulltextAndVectorIndex", func(t *testing.T) {
		_, err := exec.executeCreateFulltextIndex(ctx, "CREATE FULLTEXT INDEX dup_ft FOR (n:Node) ON EACH [n.content]")
		if err != nil {
			t.Fatalf("failed to create baseline fulltext index: %v", err)
		}
		_, err = exec.executeCreateFulltextIndex(ctx, "CREATE FULLTEXT INDEX dup_ft FOR (n:Node) ON EACH [n.content]")
		if err != nil {
			t.Fatalf("duplicate fulltext index should be idempotent, got error: %v", err)
		}

		_, err = exec.executeCreateVectorIndex(ctx, "CREATE VECTOR INDEX dup_vec FOR (n:Node) ON (n.embedding)")
		if err != nil {
			t.Fatalf("failed to create baseline vector index: %v", err)
		}
		_, err = exec.executeCreateVectorIndex(ctx, "CREATE VECTOR INDEX dup_vec FOR (n:Node) ON (n.embedding)")
		if err != nil {
			t.Fatalf("duplicate vector index should be idempotent, got error: %v", err)
		}
	})
}

func TestCreateExistsConstraint(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT person_name_required IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS NOT NULL", nil)
	if err != nil {
		t.Fatalf("Failed to create EXISTS constraint: %v", err)
	}

	allConstraints := store.GetSchema().GetAllConstraints()
	if len(allConstraints) != 1 {
		t.Fatalf("Expected 1 constraint, got %d", len(allConstraints))
	}
	if allConstraints[0].Type != storage.ConstraintExists {
		t.Fatalf("Expected EXISTS constraint, got %s", allConstraints[0].Type)
	}

	_, err = exec.Execute(ctx, "CREATE (:Person {age: 42})", nil)
	if err == nil {
		t.Fatal("Expected EXISTS constraint violation, got nil")
	}
}

func TestCreateNodeKeyConstraint(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT user_key IF NOT EXISTS FOR (u:User) REQUIRE (u.username, u.domain) IS NODE KEY", nil)
	if err != nil {
		t.Fatalf("Failed to create NODE KEY constraint: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:User {username: 'alice', domain: 'example.com'})", nil)
	if err != nil {
		t.Fatalf("Failed to create first user: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:User {username: 'alice', domain: 'example.com'})", nil)
	if err == nil {
		t.Fatal("Expected NODE KEY constraint violation, got nil")
	}
}

func TestCanonicalBootstrapFactVersionConstraints(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	queries := []string{
		"CREATE CONSTRAINT fact_version_valid_from_type IF NOT EXISTS FOR (n:FactVersion) REQUIRE n.valid_from IS :: ZONED DATETIME",
		"CREATE CONSTRAINT fact_version_valid_to_type IF NOT EXISTS FOR (n:FactVersion) REQUIRE n.valid_to IS :: ZONED DATETIME",
		"CREATE CONSTRAINT fact_version_asserted_at_type IF NOT EXISTS FOR (n:FactVersion) REQUIRE n.asserted_at IS :: ZONED DATETIME",
		"CREATE CONSTRAINT fact_version_fact_key_valid_from_node_key IF NOT EXISTS FOR (n:FactVersion) REQUIRE (n.fact_key, n.valid_from) IS NODE KEY",
	}

	for _, query := range queries {
		if _, err := exec.Execute(ctx, query, nil); err != nil {
			t.Fatalf("Failed to execute canonical bootstrap constraint query %q: %v", query, err)
		}
	}

	allConstraints := store.GetSchema().GetAllConstraints()
	if len(allConstraints) != 1 {
		t.Fatalf("Expected 1 standard constraint (NODE KEY), got %d", len(allConstraints))
	}

	if allConstraints[0].Type != storage.ConstraintNodeKey {
		t.Fatalf("Expected NODE KEY constraint, got %s", allConstraints[0].Type)
	}

	propertyTypeConstraints := store.GetSchema().GetAllPropertyTypeConstraints()
	if len(propertyTypeConstraints) != 3 {
		t.Fatalf("Expected 3 property type constraints, got %d", len(propertyTypeConstraints))
	}
}

func TestCreateTemporalConstraint(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT fact_temporal IF NOT EXISTS FOR (v:FactVersion) REQUIRE (v.fact_key, v.valid_from, v.valid_to) IS TEMPORAL NO OVERLAP", nil)
	if err != nil {
		t.Fatalf("Failed to create TEMPORAL constraint: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:FactVersion {fact_key: 'k1', valid_from: datetime('2024-01-01T00:00:00Z'), valid_to: datetime('2024-02-01T00:00:00Z')})", nil)
	if err != nil {
		t.Fatalf("Failed to create first FactVersion: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:FactVersion {fact_key: 'k1', valid_from: datetime('2024-01-15T00:00:00Z'), valid_to: datetime('2024-03-01T00:00:00Z')})", nil)
	if err == nil {
		t.Fatal("Expected TEMPORAL constraint violation, got nil")
	}

	_, err = exec.Execute(ctx, "CREATE (:FactVersion {fact_key: 'k1', valid_from: datetime('2024-02-01T00:00:00Z'), valid_to: datetime('2024-03-01T00:00:00Z')})", nil)
	if err != nil {
		t.Fatalf("Expected non-overlapping FactVersion to succeed: %v", err)
	}
}

func TestCreateTypeConstraint(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT user_age_type IF NOT EXISTS FOR (u:User) REQUIRE u.age IS :: INTEGER", nil)
	if err != nil {
		t.Fatalf("Failed to create type constraint: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:User {age: 30})", nil)
	if err != nil {
		t.Fatalf("Expected valid integer, got error: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:User {age: 'thirty'})", nil)
	if err == nil {
		t.Fatal("Expected type constraint violation, got nil")
	}
}

func TestCreateTypeConstraintWithTypedKeyword(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT event_ts_type IF NOT EXISTS FOR (e:Event) REQUIRE e.ts IS TYPED ZONED DATETIME", nil)
	if err != nil {
		t.Fatalf("Failed to create type constraint with TYPED syntax: %v", err)
	}
}

func TestTemporalTypeConstraintSemantics_ZonedVsLocal(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT event_ts_zoned IF NOT EXISTS FOR (e:Event) REQUIRE e.ts IS :: ZONED DATETIME", nil)
	if err != nil {
		t.Fatalf("Failed to create zoned datetime type constraint: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:Event {ts: datetime('2025-11-27T10:30:00Z')})", nil)
	if err != nil {
		t.Fatalf("Expected zoned datetime value to satisfy zoned constraint, got: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:Event {ts: localdatetime()})", nil)
	if err == nil {
		t.Fatal("Expected localdatetime() to violate ZONED DATETIME constraint")
	}

	_, err = exec.Execute(ctx, "CREATE CONSTRAINT meeting_local_type IF NOT EXISTS FOR (m:Meeting) REQUIRE m.start IS :: LOCAL DATETIME", nil)
	if err != nil {
		t.Fatalf("Failed to create local datetime type constraint: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:Meeting {start: localdatetime()})", nil)
	if err != nil {
		t.Fatalf("Expected localdatetime() to satisfy LOCAL DATETIME constraint, got: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE (:Meeting {start: datetime('2025-11-27T10:30:00Z')})", nil)
	if err == nil {
		t.Fatal("Expected zoned datetime to violate LOCAL DATETIME constraint")
	}
}

func TestDropConstraint(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT drop_me IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE", nil)
	if err != nil {
		t.Fatalf("Failed to create constraint: %v", err)
	}

	_, err = exec.Execute(ctx, "DROP CONSTRAINT drop_me", nil)
	if err != nil {
		t.Fatalf("Failed to drop constraint: %v", err)
	}

	allConstraints := store.GetSchema().GetAllConstraints()
	if len(allConstraints) != 0 {
		t.Fatalf("Expected 0 constraints after drop, got %d", len(allConstraints))
	}
}

func TestVectorIndexWithDifferentOptions(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name      string
		query     string
		wantDims  int
		wantSimFn string
	}{
		{
			name:      "WithOptions",
			query:     "CREATE VECTOR INDEX vec1 FOR (n:Node) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 512, `vector.similarity_function`: 'euclidean'}}",
			wantDims:  512,
			wantSimFn: "euclidean",
		},
		{
			name:      "DefaultOptions",
			query:     "CREATE VECTOR INDEX vec2 FOR (n:Node) ON (n.vec)",
			wantDims:  1024,     // default
			wantSimFn: "cosine", // default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := exec.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Fatalf("Failed to create vector index: %v", err)
			}
		})
	}

	// Verify both were created
	indexes := store.GetSchema().GetIndexes()
	vectorCount := 0
	for _, idx := range indexes {
		m := idx.(map[string]interface{})
		if m["type"] == "VECTOR" {
			vectorCount++
		}
	}
	if vectorCount != 2 {
		t.Errorf("Expected 2 vector indexes, got %d", vectorCount)
	}
}

func TestFulltextIndexMultipleProperties(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Index with multiple properties
	query := "CREATE FULLTEXT INDEX multi_search FOR (n:Document) ON EACH [n.title, n.content, n.description]"
	_, err := exec.Execute(ctx, query, nil)
	if err != nil {
		t.Fatalf("Failed to create fulltext index with multiple properties: %v", err)
	}

	idx, exists := store.GetSchema().GetFulltextIndex("multi_search")
	if !exists {
		t.Fatal("Index not found")
	}

	if len(idx.Properties) != 3 {
		t.Errorf("Expected 3 properties, got %d", len(idx.Properties))
	}

	expectedProps := map[string]bool{"title": true, "content": true, "description": true}
	for _, prop := range idx.Properties {
		if !expectedProps[prop] {
			t.Errorf("Unexpected property: %s", prop)
		}
	}
}

func TestConstraintEnforcementMultipleProperties(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Create constraints on different properties
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT user_email FOR (n:User) REQUIRE n.email IS UNIQUE", nil)
	if err != nil {
		t.Fatalf("Failed to create email constraint: %v", err)
	}

	_, err = exec.Execute(ctx, "CREATE CONSTRAINT user_username FOR (n:User) REQUIRE n.username IS UNIQUE", nil)
	if err != nil {
		t.Fatalf("Failed to create username constraint: %v", err)
	}

	// Create user with both properties
	_, err = exec.Execute(ctx, "CREATE (u:User {email: 'test@example.com', username: 'testuser'})", nil)
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Duplicate email should fail
	_, err = exec.Execute(ctx, "CREATE (u:User {email: 'test@example.com', username: 'different'})", nil)
	if err == nil {
		t.Error("Expected constraint violation for duplicate email")
	}

	// Duplicate username should fail
	_, err = exec.Execute(ctx, "CREATE (u:User {email: 'different@example.com', username: 'testuser'})", nil)
	if err == nil {
		t.Error("Expected constraint violation for duplicate username")
	}

	// Both different should succeed
	_, err = exec.Execute(ctx, "CREATE (u:User {email: 'another@example.com', username: 'anotheruser'})", nil)
	if err != nil {
		t.Errorf("Unexpected error for unique values: %v", err)
	}
}

// TestSchemaCommandsNoOp tests that schema commands don't error (they're no-ops)
func TestSchemaCommandExecution(t *testing.T) {
	baseStore := storage.NewMemoryEngine()

	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// These should all execute without error as no-ops
	tests := []struct {
		name  string
		query string
	}{
		{"constraint_neo4j5", "CREATE CONSTRAINT test IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE"},
		{"constraint_neo4j4", "CREATE CONSTRAINT IF NOT EXISTS ON (n:Node) ASSERT n.id IS UNIQUE"},
		{"index", "CREATE INDEX test_idx IF NOT EXISTS FOR (n:Node) ON (n.type)"},
		{"fulltext_index", "CREATE FULLTEXT INDEX node_search IF NOT EXISTS FOR (n:Node) ON EACH [n.content]"},
		{"vector_index", "CREATE VECTOR INDEX emb_idx IF NOT EXISTS FOR (n:Node) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 1024}}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := exec.Execute(ctx, tt.query, nil)
			if err != nil {
				t.Errorf("%s failed: %v", tt.name, err)
			}
		})
	}
}

func TestSchemaCommandDispatcherAndTypeParserBranches(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	defer store.Close()
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Unknown schema command branch.
	_, err := exec.executeSchemaCommand(ctx, "CREATE UNKNOWN THING")
	if err == nil {
		t.Fatal("expected unknown schema command error")
	}

	// DROP CONSTRAINT IF EXISTS branch should swallow missing constraint errors.
	_, err = exec.executeDropConstraint(ctx, "DROP CONSTRAINT missing_name IF EXISTS")
	if err != nil {
		t.Fatalf("DROP CONSTRAINT IF EXISTS should not error for missing constraint: %v", err)
	}

	typeCases := map[string]storage.PropertyType{
		"STRING":         storage.PropertyTypeString,
		"INT":            storage.PropertyTypeInteger,
		"INTEGER":        storage.PropertyTypeInteger,
		"FLOAT":          storage.PropertyTypeFloat,
		"BOOL":           storage.PropertyTypeBoolean,
		"BOOLEAN":        storage.PropertyTypeBoolean,
		"DATE":           storage.PropertyTypeDate,
		"DATETIME":       storage.PropertyTypeZonedDateTime,
		"ZONED DATETIME": storage.PropertyTypeZonedDateTime,
		"LOCAL DATETIME": storage.PropertyTypeLocalDateTime,
		"LOCALDATETIME":  storage.PropertyTypeLocalDateTime,
		"ZONEDDATETIME":  storage.PropertyTypeZonedDateTime,
	}
	for input, want := range typeCases {
		got, err := parsePropertyType(input)
		if err != nil {
			t.Fatalf("parsePropertyType(%q) unexpected error: %v", input, err)
		}
		if got != want {
			t.Fatalf("parsePropertyType(%q) = %q, want %q", input, got, want)
		}
	}
	if _, err := parsePropertyType("UNSUPPORTED_TYPE"); err == nil {
		t.Fatal("expected parsePropertyType unsupported type error")
	}
}

func TestCreateConstraint_SyntaxVariantCoverage(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	valid := []string{
		"CREATE CONSTRAINT IF NOT EXISTS ON (u:LegacyExists) ASSERT exists(u.email)",
		"CREATE CONSTRAINT IF NOT EXISTS ON (u:LegacyNotNull) ASSERT u.email IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS ON (u:LegacyUnique) ASSERT u.email IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS ON (u:LegacyNodeKey) ASSERT (u.a, u.b) IS NODE KEY",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (t:TempUnnamed) REQUIRE (t.key, t.from, t.to) IS TEMPORAL NO OVERLAP",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (p:TypedUnnamed) REQUIRE p.age IS :: INTEGER",
		"CREATE CONSTRAINT IF NOT EXISTS ON (p:TypedLegacy) ASSERT p.age IS TYPED INTEGER",
	}
	for _, q := range valid {
		_, err := exec.Execute(ctx, q, nil)
		if err != nil {
			t.Fatalf("expected valid constraint syntax to pass for %q: %v", q, err)
		}
	}

	_, err := exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT bad_node_key FOR (u:BrokenNK) REQUIRE (u) IS NODE KEY")
	if err == nil || !strings.Contains(err.Error(), "NODE KEY constraint requires properties") {
		t.Fatalf("expected NODE KEY property validation error, got: %v", err)
	}

	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT bad_temporal FOR (t:BrokenTemporal) REQUIRE (t.key, t.from) IS TEMPORAL")
	if err == nil || !strings.Contains(err.Error(), "TEMPORAL constraint requires 3 properties") {
		t.Fatalf("expected TEMPORAL property-count validation error, got: %v", err)
	}
}

func TestCreateConstraint_EachParserPattern(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	queries := []string{
		// NODE KEY variants
		"CREATE CONSTRAINT nk_named IF NOT EXISTS FOR (n:NKNamed) REQUIRE (n.k1, n.k2) IS NODE KEY",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:NKUnnamed) REQUIRE (n.k1, n.k2) IS NODE KEY",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:NKAssert) ASSERT (n.k1, n.k2) IS NODE KEY",

		// TEMPORAL variants
		"CREATE CONSTRAINT t_named IF NOT EXISTS FOR (n:TNamed) REQUIRE (n.key, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:TUnnamed) REQUIRE (n.key, n.valid_from, n.valid_to) IS TEMPORAL",

		// EXISTS / NOT NULL variants
		"CREATE CONSTRAINT nn_named IF NOT EXISTS FOR (n:NNNamed) REQUIRE n.email IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:NNUnnamed) REQUIRE n.email IS NOT NULL",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:NNExists) ASSERT exists(n.email)",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:NNAssert) ASSERT n.email IS NOT NULL",

		// TYPE variants
		"CREATE CONSTRAINT tp_named IF NOT EXISTS FOR (n:TPNamed) REQUIRE n.age IS :: INTEGER",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:TPUnnamed) REQUIRE n.age IS TYPED INTEGER",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:TPAssert) ASSERT n.age IS :: INTEGER",

		// UNIQUE variants
		"CREATE CONSTRAINT uq_named IF NOT EXISTS FOR (n:UQNamed) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS FOR (n:UQUnnamed) REQUIRE n.id IS UNIQUE",
		"CREATE CONSTRAINT IF NOT EXISTS ON (n:UQAssert) ASSERT n.id IS UNIQUE",
	}

	for _, q := range queries {
		_, err := exec.executeCreateConstraint(ctx, q)
		if err != nil {
			t.Fatalf("expected query to match a CREATE CONSTRAINT parser pattern, got error for %q: %v", q, err)
		}
	}
}

func TestCreateConstraint_ValidationAndDuplicateErrorBranches(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:DupUser {email:'a@x'}), (:DupUser {email:'a@x'})", nil)
	if err != nil {
		t.Fatalf("failed to seed duplicate users: %v", err)
	}
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT uq_dup IF NOT EXISTS FOR (n:DupUser) REQUIRE n.email IS UNIQUE")
	if err == nil {
		t.Fatal("expected unique constraint validation error on existing duplicate data")
	}

	_, err = exec.Execute(ctx, "CREATE (:NullUser {name:'x'})", nil)
	if err != nil {
		t.Fatalf("failed to seed null property user: %v", err)
	}
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT nn_dup IF NOT EXISTS FOR (n:NullUser) REQUIRE n.email IS NOT NULL")
	if err == nil {
		t.Fatal("expected NOT NULL constraint validation error on existing null property data")
	}

	_, err = exec.Execute(ctx, "CREATE (:TypedBad {age:'not-an-int'})", nil)
	if err != nil {
		t.Fatalf("failed to seed wrong-typed data: %v", err)
	}
	_, err = exec.executeCreateConstraint(ctx, "CREATE CONSTRAINT tp_dup IF NOT EXISTS FOR (n:TypedBad) REQUIRE n.age IS :: INTEGER")
	if err == nil {
		t.Fatal("expected type constraint validation error on existing wrong-typed data")
	}

}

func TestCreateIndex_BranchCoverage(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Named index branch (composite properties).
	_, err := exec.executeCreateIndex(ctx, "CREATE INDEX idx_person_name_age FOR (n:Person) ON (n.name, n.age)")
	if err != nil {
		t.Fatalf("expected named composite index creation to succeed: %v", err)
	}

	// Unnamed index branch (auto-generated name).
	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX FOR (n:Product) ON (n.sku, n.region)")
	if err != nil {
		t.Fatalf("expected unnamed composite index creation to succeed: %v", err)
	}

	// Named no-properties branch.
	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX idx_empty FOR (n:EmptyIdx) ON (n)")
	if err == nil {
		t.Fatal("expected no properties specified for named index")
	}

	// Unnamed no-properties branch.
	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX FOR (n:EmptyIdx2) ON (n)")
	if err == nil {
		t.Fatal("expected no properties specified for unnamed index")
	}

	// Invalid syntax fallback branch.
	_, err = exec.executeCreateIndex(ctx, "CREATE INDEX idx_invalid")
	if err == nil {
		t.Fatal("expected invalid CREATE INDEX syntax error")
	}
}

func TestCreateConstraint_DuplicateNamedDefinitionErrors(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "unique",
			query: "CREATE CONSTRAINT uq_dup_name FOR (n:DupUnique) REQUIRE n.id IS UNIQUE",
		},
		{
			name:  "exists_not_null",
			query: "CREATE CONSTRAINT nn_dup_name FOR (n:DupExists) REQUIRE n.email IS NOT NULL",
		},
		{
			name:  "node_key",
			query: "CREATE CONSTRAINT nk_dup_name FOR (n:DupNodeKey) REQUIRE (n.k1, n.k2) IS NODE KEY",
		},
		{
			name:  "temporal",
			query: "CREATE CONSTRAINT tp_dup_name FOR (n:DupTemporal) REQUIRE (n.key, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP",
		},
		{
			name:  "property_type",
			query: "CREATE CONSTRAINT ty_dup_name FOR (n:DupType) REQUIRE n.age IS :: INTEGER",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := exec.executeCreateConstraint(ctx, tt.query)
			if err != nil {
				t.Fatalf("first create constraint should succeed: %v", err)
			}
			_, err = exec.executeCreateConstraint(ctx, tt.query)
			if err != nil {
				t.Fatalf("duplicate named constraint should be idempotent for %s: %v", tt.name, err)
			}
		})
	}
}

func TestCreateRangeIndex_ErrorBranches(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX idx_age FOR (n:Person) ON (n.age)")
	if err != nil {
		t.Fatalf("failed to create baseline range index: %v", err)
	}
	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX idx_age FOR (n:Person) ON (n.score)")
	if err != nil {
		t.Fatalf("expected conflicting named range index to be idempotent, got: %v", err)
	}

	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX FOR (n:Product) ON (n.price)")
	if err != nil {
		t.Fatalf("failed to create unnamed range index: %v", err)
	}
	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX FOR (n:Product) ON (n.cost)")
	if err != nil {
		t.Fatalf("expected second unnamed range index with different generated name to succeed, got: %v", err)
	}

	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX idx_multi FOR (n:Person) ON (n.age, n.score)")
	if err == nil || !strings.Contains(err.Error(), "only supports single property") {
		t.Fatalf("expected single-property validation error, got: %v", err)
	}

	_, err = exec.executeCreateRangeIndex(ctx, "CREATE RANGE INDEX")
	if err == nil || !strings.Contains(err.Error(), "invalid CREATE RANGE INDEX syntax") {
		t.Fatalf("expected invalid syntax error, got: %v", err)
	}
}

func TestCreateFulltextIndex_SchemaAndDuplicateErrors(t *testing.T) {
	ctx := context.Background()

	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	_, err := exec.executeCreateFulltextIndex(ctx, "CREATE FULLTEXT INDEX dup_ft FOR (n:Doc) ON EACH [n.title]")
	if err != nil {
		t.Fatalf("failed to create baseline fulltext index: %v", err)
	}
	_, err = exec.executeCreateFulltextIndex(ctx, "CREATE FULLTEXT INDEX dup_ft FOR (n:Doc) ON EACH [n.body]")
	if err != nil {
		t.Fatalf("expected conflicting fulltext index to be idempotent, got: %v", err)
	}

	nilSchema := &nilSchemaEngine{Engine: store}
	execNilSchema := NewStorageExecutor(nilSchema)
	_, err = execNilSchema.executeCreateFulltextIndex(ctx, "CREATE FULLTEXT INDEX nil_schema FOR (n:Doc) ON EACH [n.title]")
	if err == nil || !strings.Contains(err.Error(), "schema manager not available") {
		t.Fatalf("expected nil schema error, got: %v", err)
	}
}

func TestCreateVectorIndex_DuplicateErrorBranch(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	query := "CREATE VECTOR INDEX vec_dup FOR (n:Doc) ON (n.embedding)"
	_, err := exec.executeCreateVectorIndex(ctx, query)
	if err != nil {
		t.Fatalf("failed to create baseline vector index: %v", err)
	}
	_, err = exec.executeCreateVectorIndex(ctx, "CREATE VECTOR INDEX vec_dup FOR (n:Doc) ON (n.altEmbedding)")
	if err != nil {
		t.Fatalf("expected conflicting vector index to be idempotent, got: %v", err)
	}
}
