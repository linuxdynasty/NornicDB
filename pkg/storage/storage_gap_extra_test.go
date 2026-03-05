package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type temporalStringer string

func (t temporalStringer) String() string { return string(t) }

type lookupEngine struct {
	Engine
	ids []NodeID
	err error
}

func (l *lookupEngine) ForEachNodeIDByLabel(_ string, visit func(NodeID) bool) error {
	if l.err != nil {
		return l.err
	}
	for _, id := range l.ids {
		if !visit(id) {
			break
		}
	}
	return nil
}

type captureLogger struct {
	entries []map[string]any
}

func (l *captureLogger) Log(level string, msg string, fields map[string]any) {
	entry := map[string]any{
		"level": level,
		"msg":   msg,
	}
	for k, v := range fields {
		entry[k] = v
	}
	l.entries = append(l.entries, entry)
}

func newIsolatedBadgerEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func TestTemporalHelpers(t *testing.T) {
	now := time.Date(2026, 3, 4, 12, 34, 56, 0, time.UTC)

	t.Run("coerce temporal values", func(t *testing.T) {
		ptr := now
		tests := []struct {
			name  string
			value interface{}
			want  time.Time
			ok    bool
		}{
			{"time", now, now, true},
			{"time pointer", &ptr, now, true},
			{"nil time pointer", (*time.Time)(nil), time.Time{}, false},
			{"rfc3339 string", now.Format(time.RFC3339), now, true},
			{"stringer", temporalStringer(now.Format(time.RFC3339Nano)), now, true},
			{"int64 unix", now.Unix(), now, true},
			{"int unix", int(now.Unix()), now, true},
			{"float64 unix", float64(now.Unix()), now, true},
			{"unsupported", []byte("bad"), time.Time{}, false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, ok := coerceTemporalTime(tt.value)
				assert.Equal(t, tt.ok, ok)
				if tt.ok {
					assert.True(t, got.Equal(tt.want.UTC()), "got=%s want=%s", got, tt.want.UTC())
				}
			})
		}
	})

	t.Run("parse temporal string variants", func(t *testing.T) {
		cases := []string{
			now.Format(time.RFC3339Nano),
			now.Format(time.RFC3339),
			now.Format("2006-01-02T15:04:05"),
			now.Format("2006-01-02 15:04:05"),
			now.Format("2006-01-02"),
		}
		for _, raw := range cases {
			parsed, ok := parseTemporalString(raw)
			require.True(t, ok, raw)
			require.False(t, parsed.IsZero())
		}
		_, ok := parseTemporalString(" ")
		require.False(t, ok)
		_, ok = parseTemporalString("not-a-time")
		require.False(t, ok)
	})

	t.Run("interval overlap", func(t *testing.T) {
		base := temporalInterval{start: now, end: now.Add(2 * time.Hour), hasEnd: true, nodeID: "a"}
		overlap := temporalInterval{start: now.Add(time.Hour), end: now.Add(3 * time.Hour), hasEnd: true, nodeID: "b"}
		disjoint := temporalInterval{start: now.Add(3 * time.Hour), end: now.Add(4 * time.Hour), hasEnd: true, nodeID: "c"}
		openEnded := temporalInterval{start: now.Add(time.Hour), hasEnd: false, nodeID: "d"}

		assert.True(t, intervalsOverlap(base, overlap))
		assert.False(t, intervalsOverlap(base, disjoint))
		assert.True(t, intervalsOverlap(base, openEnded))
		assert.False(t, intervalsOverlap(temporalInterval{}, overlap))
	})
}

func TestLabelNodeIDLookupHelpers(t *testing.T) {
	t.Run("invalid engine", func(t *testing.T) {
		_, err := FirstNodeIDByLabel(nil, "Person")
		require.ErrorIs(t, err, ErrInvalidData)

		ids, err := NodeIDsByLabel(nil, "Person", 1)
		require.ErrorIs(t, err, ErrInvalidData)
		require.Nil(t, ids)
	})

	t.Run("lookup engine path", func(t *testing.T) {
		base := NewMemoryEngine()
		engine := &lookupEngine{Engine: base, ids: []NodeID{"n1", "n2", "n3"}}

		id, err := FirstNodeIDByLabel(engine, "Person")
		require.NoError(t, err)
		require.Equal(t, NodeID("n1"), id)

		ids, err := NodeIDsByLabel(engine, "Person", 2)
		require.NoError(t, err)
		require.Equal(t, []NodeID{"n1", "n2"}, ids)
	})

	t.Run("lookup engine errors and empty", func(t *testing.T) {
		wantErr := errors.New("boom")
		engine := &lookupEngine{Engine: NewMemoryEngine(), err: wantErr}
		_, err := FirstNodeIDByLabel(engine, "Person")
		require.ErrorIs(t, err, wantErr)

		engine = &lookupEngine{Engine: NewMemoryEngine()}
		_, err = FirstNodeIDByLabel(engine, "Person")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("fallback engine path", func(t *testing.T) {
		engine := NewNamespacedEngine(NewMemoryEngine(), "test")
		_, err := engine.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "n2", Labels: []string{"Person"}})
		require.NoError(t, err)

		id, err := FirstNodeIDByLabel(engine, "Person")
		require.NoError(t, err)
		require.NotEmpty(t, id)

		ids, err := NodeIDsByLabel(engine, "Person", 1)
		require.NoError(t, err)
		require.Len(t, ids, 1)

		allIDs, err := NodeIDsByLabel(engine, "Missing", 0)
		require.NoError(t, err)
		require.Empty(t, allIDs)
	})
}

func TestSchemaDefinitionPersistenceHelpers(t *testing.T) {
	t.Run("export and replace round trip", func(t *testing.T) {
		sm := NewSchemaManager()
		require.NoError(t, sm.AddUniqueConstraint("user_email", "User", "email"))
		require.NoError(t, sm.AddConstraint(Constraint{Name: "user_name_exists", Type: ConstraintExists, Label: "User", Properties: []string{"name"}}))
		require.NoError(t, sm.AddPropertyTypeConstraint("user_age_type", "User", "age", PropertyTypeInteger))
		require.NoError(t, sm.AddPropertyIndex("user_name_idx", "User", []string{"name"}))
		require.NoError(t, sm.AddCompositeIndex("user_loc_idx", "User", []string{"country", "city"}))
		require.NoError(t, sm.AddFulltextIndex("user_search_idx", []string{"User"}, []string{"bio"}))
		require.NoError(t, sm.AddVectorIndex("user_embedding_idx", "User", "embedding", 3, "cosine"))
		require.NoError(t, sm.AddRangeIndex("user_age_range", "User", "age"))

		def := sm.ExportDefinition()
		require.NotNil(t, def)
		require.Equal(t, schemaDefinitionVersion, def.Version)
		require.Len(t, def.Constraints, 2)
		require.Len(t, def.PropertyTypeConstraints, 1)
		require.Len(t, def.PropertyIndexes, 1)
		require.Len(t, def.CompositeIndexes, 1)
		require.Len(t, def.FulltextIndexes, 1)
		require.Len(t, def.VectorIndexes, 1)
		require.Len(t, def.RangeIndexes, 1)

		restored := NewSchemaManager()
		require.NoError(t, restored.ReplaceFromDefinition(def))

		constraints := restored.GetConstraints()
		require.Len(t, constraints, 1)
		require.NoError(t, restored.CheckUniqueConstraint("User", "email", "alice@example.com", ""))
		restored.RegisterUniqueValue("User", "email", "alice@example.com", "node-1")
		require.Error(t, restored.CheckUniqueConstraint("User", "email", "alice@example.com", ""))
		_, ok := restored.GetCompositeIndex("user_loc_idx")
		require.True(t, ok)
		_, ok = restored.GetFulltextIndex("user_search_idx")
		require.True(t, ok)
		_, ok = restored.GetVectorIndex("user_embedding_idx")
		require.True(t, ok)
	})

	t.Run("replace nil definition noops", func(t *testing.T) {
		sm := NewSchemaManager()
		require.NoError(t, sm.ReplaceFromDefinition(nil))
	})
}

func TestBadgerSchemaHelpers(t *testing.T) {
	t.Run("parse schema namespace from key", func(t *testing.T) {
		ns, ok := parseSchemaNamespaceFromKey(schemaKey("alpha"))
		require.True(t, ok)
		require.Equal(t, "alpha", ns)

		_, ok = parseSchemaNamespaceFromKey([]byte{prefixSchema})
		require.False(t, ok)
		_, ok = parseSchemaNamespaceFromKey([]byte{prefixNode, 'a', 0})
		require.False(t, ok)
		_, ok = parseSchemaNamespaceFromKey([]byte{prefixSchema, 0})
		require.False(t, ok)
	})

	t.Run("persist and load schema definitions", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)

		def := &SchemaDefinition{
			Constraints: []Constraint{{
				Name:       "user_email",
				Type:       ConstraintUnique,
				Label:      "User",
				Properties: []string{"email"},
			}},
			PropertyIndexes: []SchemaPropertyIndexDef{{
				Name:       "user_name_idx",
				Label:      "User",
				Properties: []string{"name"},
			}},
		}
		require.NoError(t, engine.persistSchemaDefinition("testns", def))

		engine.schemasMu.Lock()
		engine.schemas = make(map[string]*SchemaManager)
		engine.schemasMu.Unlock()

		require.NoError(t, engine.loadPersistedSchemas())

		sm := engine.GetSchemaForNamespace("testns")
		require.NotNil(t, sm)
		require.Len(t, sm.GetConstraints(), 1)
		require.Len(t, sm.GetIndexes(), 1)
	})

	t.Run("persist schema validation errors", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)

		require.Error(t, engine.persistSchemaDefinition("", &SchemaDefinition{}))
		require.Error(t, engine.persistSchemaDefinition("ns", nil))
	})

	t.Run("load persisted schema invalid key and invalid json", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			require.NoError(t, txn.Set([]byte{prefixSchema, 0}, []byte(`{}`)))
			return nil
		}))
		require.Error(t, engine.loadPersistedSchemas())

		engine2 := newIsolatedBadgerEngine(t)
		require.NoError(t, engine2.withUpdate(func(txn *badger.Txn) error {
			require.NoError(t, txn.Set(schemaKey("broken"), []byte(`{not-json`)))
			return nil
		}))
		require.Error(t, engine2.loadPersistedSchemas())
	})

	t.Run("rebuild unique constraint values", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)

		node1 := &Node{ID: "nornic:u1", Labels: []string{"User"}, Properties: map[string]interface{}{"email": "alice@example.com"}}
		node2 := &Node{ID: "nornic:u2", Labels: []string{"User"}, Properties: map[string]interface{}{"email": "bob@example.com"}}
		_, err := engine.CreateNode(node1)
		require.NoError(t, err)
		_, err = engine.CreateNode(node2)
		require.NoError(t, err)

		sm := NewSchemaManager()
		require.NoError(t, sm.AddUniqueConstraint("user_email", "User", "email"))
		require.NoError(t, engine.rebuildUniqueConstraintValues("nornic", sm))
		require.Error(t, sm.CheckUniqueConstraint("User", "email", "alice@example.com", ""))
		require.NoError(t, engine.rebuildUniqueConstraintValues("", sm))
		require.NoError(t, engine.rebuildUniqueConstraintValues("nornic", nil))
	})

	t.Run("get schema default and cached namespace", func(t *testing.T) {
		engine := newIsolatedBadgerEngine(t)

		defaultSchema := engine.GetSchemaForNamespace("")
		require.NotNil(t, defaultSchema)
		require.Same(t, defaultSchema, engine.GetSchemaForNamespace("nornic"))

		custom := engine.GetSchemaForNamespace("custom")
		require.Same(t, custom, engine.GetSchemaForNamespace("custom"))
	})
}

func TestWALDiagnosticsHelpers(t *testing.T) {
	t.Run("backup corrupted wal", func(t *testing.T) {
		dir := t.TempDir()
		logger := &captureLogger{}
		wal := &WAL{config: &WALConfig{Dir: dir, Logger: logger}}

		walPath := filepath.Join(dir, "wal.log")
		require.NoError(t, os.WriteFile(walPath, []byte("corrupted-data"), 0644))

		backupPath := wal.backupCorruptedWAL(walPath)
		require.NotEmpty(t, backupPath)
		data, err := os.ReadFile(backupPath)
		require.NoError(t, err)
		require.Equal(t, "corrupted-data", string(data))

		missing := wal.backupCorruptedWAL(filepath.Join(dir, "missing.log"))
		require.Empty(t, missing)
		require.NotEmpty(t, logger.entries)
	})

	t.Run("report corruption writes diagnostics and callback", func(t *testing.T) {
		dir := t.TempDir()
		logger := &captureLogger{}
		var callbackDiag *CorruptionDiagnostics
		var callbackErr error

		wal := &WAL{
			config: &WALConfig{
				Dir:    dir,
				Logger: logger,
				OnCorruption: func(diag *CorruptionDiagnostics, cause error) {
					callbackDiag = diag
					callbackErr = cause
				},
			},
		}

		diag := &CorruptionDiagnostics{
			Timestamp:      time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC),
			WALPath:        filepath.Join(dir, "wal.log"),
			CorruptedSeq:   7,
			FileSize:       42,
			LastGoodSeq:    6,
			ExpectedCRC:    11,
			ActualCRC:      22,
			BackupPath:     filepath.Join(dir, "wal-corrupted-backup.log"),
			SuspectedCause: "disk",
			RecoveryAction: "truncate",
		}
		cause := fmt.Errorf("checksum mismatch")

		wal.reportCorruption(diag, cause)

		require.True(t, wal.degraded.Load())
		stored, ok := wal.lastCorruption.Load().(*CorruptionDiagnostics)
		require.True(t, ok)
		require.Equal(t, diag, stored)
		require.Equal(t, diag, callbackDiag)
		require.EqualError(t, callbackErr, cause.Error())
		require.NotEmpty(t, logger.entries)
		require.Equal(t, "error", logger.entries[0]["level"])

		diagnosticPath := filepath.Join(dir, "wal-corruption-20260304-100000.json")
		_, err := os.Stat(diagnosticPath)
		require.NoError(t, err)

		wal.reportCorruption(nil, nil)
	})
}
