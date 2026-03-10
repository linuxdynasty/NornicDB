package search

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestFulltextIndexV2_BasicLifecycle(t *testing.T) {
	idx := NewFulltextIndexV2()
	idx.Index("doc1", "hello world distributed systems")
	idx.Index("doc2", "hello nornicdb graph database")

	results := idx.Search("hello graph", 10)
	require.NotEmpty(t, results)
	require.Equal(t, 2, idx.Count())

	idx.Remove("doc1")
	require.Equal(t, 1, idx.Count())
}

func TestFulltextIndexV2_PrefixBounded(t *testing.T) {
	t.Setenv("NORNICDB_BM25_PREFIX_MAX_EXPANSIONS", "2")
	t.Setenv("NORNICDB_BM25_PREFIX_MIN_LEN", "3")

	idx := NewFulltextIndexV2()
	idx.Index("a", "prescription refill reminder")
	idx.Index("b", "prescribe dosage reminder")
	idx.Index("c", "prescribed medicine")
	idx.Index("d", "pressure issue")

	results := idx.Search("pres", 10)
	require.NotEmpty(t, results)
}

func TestFulltextIndexV2_StableTopK(t *testing.T) {
	v2 := NewFulltextIndexV2()

	docs := []FulltextBatchEntry{
		{ID: "d1", Text: "graph database nornicdb embeddings"},
		{ID: "d2", Text: "prescription refill status and dosage"},
		{ID: "d3", Text: "where are my prescriptions and refill history"},
		{ID: "d4", Text: "medical translation memory and dictionary"},
		{ID: "d5", Text: "hybrid search bm25 and vector fusion"},
	}
	v2.IndexBatch(docs)

	query := "prescriptions refill"
	r1 := v2.Search(query, 3)
	r2 := v2.Search(query, 3)
	require.NotEmpty(t, r1)
	require.NotEmpty(t, r2)
	require.Equal(t, len(r1), len(r2))
	for i := range r1 {
		require.Equal(t, r1[i].ID, r2[i].ID)
	}
}

func TestFulltextIndexV2_SaveLoadAndMigrateV1(t *testing.T) {
	path := t.TempDir() + "/bm25"

	v2 := NewFulltextIndexV2()
	v2.Index("doc1", "hello world")
	v2.Index("doc2", "hello graph world")
	require.NoError(t, v2.Save(path))

	loaded := NewFulltextIndexV2()
	require.NoError(t, loaded.Load(path))
	require.Equal(t, 2, loaded.Count())
	require.NotEmpty(t, loaded.Search("hello", 10))

	legacyPath := t.TempDir() + "/bm25_legacy"
	legacySnap := bm25V1Snapshot{
		Version:   "1.0.0",
		Documents: map[string]string{"docA": "legacy migration path", "docB": "legacy bm25 test"},
		InvertedIndex: map[string]map[string]int{
			"legacy":    {"docA": 1, "docB": 1},
			"migration": {"docA": 1},
			"path":      {"docA": 1},
			"bm25":      {"docB": 1},
			"test":      {"docB": 1},
		},
		DocLengths:   map[string]int{"docA": 3, "docB": 3},
		AvgDocLength: 3,
		DocCount:     2,
	}
	f, err := os.Create(legacyPath)
	require.NoError(t, err)
	require.NoError(t, msgpack.NewEncoder(f).Encode(&legacySnap))
	require.NoError(t, f.Close())

	fromLegacy := NewFulltextIndexV2()
	require.NoError(t, fromLegacy.Load(legacyPath))
	require.Equal(t, 2, fromLegacy.Count())
	require.NotEmpty(t, fromLegacy.Search("legacy", 10))
}

func TestFulltextIndexV2_DirtySaveNoCopyPhraseClear(t *testing.T) {
	idx := NewFulltextIndexV2()
	require.False(t, idx.IsDirty())

	idx.Index("doc1", "the quick brown fox jumps")
	idx.Index("doc2", "quick brown is common phrase")
	require.True(t, idx.IsDirty())

	phrase := idx.PhraseSearch("quick brown", 10)
	require.Len(t, phrase, 2)

	path := filepath.Join(t.TempDir(), "bm25v2")
	require.NoError(t, idx.SaveNoCopy(path))
	require.False(t, idx.IsDirty())

	idx.Clear()
	require.Equal(t, 0, idx.Count())
	require.True(t, idx.IsDirty())

	// Empty clear path should still be safe.
	idx.Clear()
	require.Equal(t, 0, idx.Count())
}

func TestFulltextIndexV2_LoadDecodeFailureClears(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bm25v2_corrupt")
	require.NoError(t, os.WriteFile(path, []byte("not-msgpack"), 0644))

	idx := NewFulltextIndexV2()
	idx.Index("doc1", "hello world")
	require.NoError(t, idx.Load(path))
	require.Equal(t, 0, idx.Count())
}

func TestFulltextIndexV2_MinIntHelper(t *testing.T) {
	require.Equal(t, 1, minInt(1, 2))
	require.Equal(t, -5, minInt(-5, 3))
	require.Equal(t, 7, minInt(7, 7))
}
