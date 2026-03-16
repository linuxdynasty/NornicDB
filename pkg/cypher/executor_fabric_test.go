package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestStripLeadingWithImportsForFabricRecord_MatchIsStripped(t *testing.T) {
	query := "WITH textKey128 MATCH (tt) WHERE tt.textKey128 = textKey128 RETURN collect(tt) AS texts"
	got := stripLeadingWithImportsForFabricRecord(query, map[string]interface{}{"textKey128": "h1"})
	want := "MATCH (tt) WHERE tt.textKey128 = $textKey128 RETURN collect(tt) AS texts"
	if got != want {
		t.Fatalf("expected stripped query %q, got %q", want, got)
	}
}

func TestStripLeadingWithImportsForFabricRecord_UnwindPipelineIsStripped(t *testing.T) {
	query := "WITH rows UNWIND rows AS r WITH collect(DISTINCT r.textKey128) AS keys RETURN keys"
	got := stripLeadingWithImportsForFabricRecord(query, map[string]interface{}{"rows": []interface{}{}})
	want := "UNWIND $rows AS r WITH collect(DISTINCT r.textKey128) AS keys RETURN keys"
	if got != want {
		t.Fatalf("expected stripped query %q, got %q", want, got)
	}
}

func TestStripLeadingWithImportsForFabricRecord_UseClauseIsStripped(t *testing.T) {
	query := "WITH textKey128 USE translations.txr MATCH (tt) WHERE tt.translationId = textKey128 RETURN collect(tt) AS texts"
	got := stripLeadingWithImportsForFabricRecord(query, map[string]interface{}{"textKey128": "abc"})
	want := "USE translations.txr MATCH (tt) WHERE tt.translationId = $textKey128 RETURN collect(tt) AS texts"
	if got != want {
		t.Fatalf("expected stripped query %q, got %q", want, got)
	}
}

func TestFindLeadingWithEndLocalAndSplitCommaTopLevelLocal(t *testing.T) {
	query := "WITH a, collect({k: b, t: c}) AS rows USE translations.txr MATCH (n) RETURN n"
	idx := findLeadingWithEndLocal(query)
	if idx <= 0 {
		t.Fatalf("expected positive with-end index, got %d", idx)
	}
	parts := splitCommaTopLevelLocal("a, collect({k: b, t: c}) AS rows, `quoted`")
	if len(parts) != 3 {
		t.Fatalf("expected 3 top-level parts, got %#v", parts)
	}
}

func TestCypherFabricExecutor_ExecuteQuery_DelegatesAndReturnsRows(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	exec := NewStorageExecutor(baseStore)

	cf := &cypherFabricExecutor{
		base:       exec,
		authToken:  "",
		autoCommit: true,
	}

	cols, rows, err := cf.ExecuteQuery(context.Background(), "nornic", baseStore, "RETURN 1 AS one", nil)
	if err != nil {
		t.Fatalf("unexpected ExecuteQuery error: %v", err)
	}
	if len(cols) != 1 || cols[0] != "one" {
		t.Fatalf("unexpected columns: %#v", cols)
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("unexpected rows: %#v", rows)
	}
}
