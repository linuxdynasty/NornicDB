package cypher

import "testing"

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
