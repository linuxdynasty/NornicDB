package fabric

import (
	"testing"
)

func TestPlan_SimpleQueryNoUse(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	p := NewFabricPlanner(catalog)

	frag, err := p.Plan("MATCH (n) RETURN n", "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	exec, ok := frag.(*FragmentExec)
	if !ok {
		t.Fatalf("expected FragmentExec, got %T", frag)
	}
	if exec.GraphName != "nornic" {
		t.Errorf("expected nornic, got %s", exec.GraphName)
	}
	if exec.Query != "MATCH (n) RETURN n" {
		t.Errorf("expected original query, got %s", exec.Query)
	}
	if exec.IsWrite {
		t.Error("expected IsWrite=false for read query")
	}
}

func TestPlan_LeadingUseClause(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	p := NewFabricPlanner(catalog)

	frag, err := p.Plan("USE nornic MATCH (n) RETURN n", "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	exec, ok := frag.(*FragmentExec)
	if !ok {
		t.Fatalf("expected FragmentExec, got %T", frag)
	}
	if exec.GraphName != "nornic" {
		t.Errorf("expected nornic, got %s", exec.GraphName)
	}
	if exec.Query != "MATCH (n) RETURN n" {
		t.Errorf("expected remaining query, got %s", exec.Query)
	}
}

func TestPlan_LeadingUseDotted(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic.tr", &LocationRemote{DBName: "nornic_tr", URI: "bolt://a:7687"})
	p := NewFabricPlanner(catalog)

	frag, err := p.Plan("USE nornic.tr MATCH (n) RETURN n", "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	exec, ok := frag.(*FragmentExec)
	if !ok {
		t.Fatalf("expected FragmentExec, got %T", frag)
	}
	if exec.GraphName != "nornic.tr" {
		t.Errorf("expected nornic.tr, got %s", exec.GraphName)
	}
}

func TestPlan_CallUseSubquery(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	catalog.Register("nornic.tr", &LocationRemote{DBName: "tr", URI: "bolt://a:7687"})
	p := NewFabricPlanner(catalog)

	query := `USE nornic
CALL {
  USE nornic.tr
  MATCH (t:Translation)
  RETURN t.id AS translationId
}
RETURN translationId`

	frag, err := p.Plan(query, "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be an Apply chain: Apply(input=Apply(input=Init, inner=Exec(nornic.tr)), inner=Exec(trailing RETURN))
	apply, ok := frag.(*FragmentApply)
	if !ok {
		t.Fatalf("expected FragmentApply at root, got %T", frag)
	}

	// The trailing RETURN should be in the outer apply.
	innerExec, ok := apply.Inner.(*FragmentExec)
	if !ok {
		t.Fatalf("expected FragmentExec for trailing RETURN, got %T", apply.Inner)
	}
	if innerExec.GraphName != "nornic" {
		t.Errorf("expected trailing fragment on nornic, got %s", innerExec.GraphName)
	}

	// The inner apply contains the CALL { USE nornic.tr ... } block.
	innerApply, ok := apply.Input.(*FragmentApply)
	if !ok {
		t.Fatalf("expected FragmentApply for CALL block, got %T", apply.Input)
	}

	callExec, ok := innerApply.Inner.(*FragmentExec)
	if !ok {
		t.Fatalf("expected FragmentExec for CALL body, got %T", innerApply.Inner)
	}
	if callExec.GraphName != "nornic.tr" {
		t.Errorf("expected nornic.tr, got %s", callExec.GraphName)
	}
}

func TestPlan_MultipleCallUseSubqueries(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	catalog.Register("nornic.tr", &LocationRemote{DBName: "tr", URI: "bolt://a:7687"})
	catalog.Register("nornic.txt", &LocationRemote{DBName: "txt", URI: "bolt://b:7687"})
	p := NewFabricPlanner(catalog)

	query := `USE nornic
CALL {
  USE nornic.tr
  MATCH (t:Translation)
  RETURN t.id AS translationId, t.textKey AS textKey
}
CALL {
  USE nornic.txt
  WITH translationId
  MATCH (tt:TranslationText)
  WHERE tt.translationId = translationId
  RETURN collect(tt) AS texts
}
RETURN translationId, textKey, texts`

	frag, err := p.Plan(query, "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Count the depth of FragmentApply nesting.
	applyCount := 0
	current := frag
	for {
		apply, ok := current.(*FragmentApply)
		if !ok {
			break
		}
		applyCount++
		current = apply.Input
	}
	// Two CALL blocks + one trailing RETURN = 3 apply nodes.
	if applyCount != 3 {
		t.Errorf("expected 3 nested Apply fragments, got %d", applyCount)
	}
}

func TestPlan_CorrelatedWithImports(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	catalog.Register("db.shard", &LocationLocal{DBName: "shard"})
	p := NewFabricPlanner(catalog)

	query := `USE db
CALL {
  USE db.shard
  WITH myVar
  MATCH (n) WHERE n.id = myVar
  RETURN n
}
RETURN n`

	frag, err := p.Plan(query, "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Walk to the CALL block's FragmentExec.
	apply := frag.(*FragmentApply)
	innerApply := apply.Input.(*FragmentApply)
	callExec := innerApply.Inner.(*FragmentExec)

	// The init of the exec should have import columns from WITH.
	init, ok := callExec.Input.(*FragmentInit)
	if !ok {
		t.Fatalf("expected FragmentInit as exec input, got %T", callExec.Input)
	}
	if len(init.ImportColumns) != 1 || init.ImportColumns[0] != "myVar" {
		t.Errorf("expected import columns [myVar], got %v", init.ImportColumns)
	}
}

func TestPlan_WriteDetection(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	p := NewFabricPlanner(catalog)

	tests := []struct {
		query   string
		isWrite bool
	}{
		{"MATCH (n) RETURN n", false},
		{"CREATE (n:Test)", true},
		{"MERGE (n:Test {id: 1})", true},
		{"MATCH (n) DELETE n", true},
		{"MATCH (n) DETACH DELETE n", true},
		{"MATCH (n) SET n.x = 1", true},
		{"MATCH (n) REMOVE n.x", true},
	}

	for _, tt := range tests {
		frag, err := p.Plan(tt.query, "db")
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tt.query, err)
		}
		exec := frag.(*FragmentExec)
		if exec.IsWrite != tt.isWrite {
			t.Errorf("query %q: expected IsWrite=%v, got %v", tt.query, tt.isWrite, exec.IsWrite)
		}
	}
}

func TestPlan_EmptyQuery(t *testing.T) {
	p := NewFabricPlanner(NewCatalog())
	_, err := p.Plan("", "db")
	if err == nil {
		t.Fatal("expected error for empty query")
	}
}

func TestPlan_UseWithoutDbName(t *testing.T) {
	p := NewFabricPlanner(NewCatalog())
	_, err := p.Plan("USE ", "db")
	if err == nil {
		t.Fatal("expected error for USE without database name")
	}
}

func TestPlan_CallWithoutUse_NotFabric(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	p := NewFabricPlanner(catalog)

	// CALL {} without USE inside is a regular subquery, not fabric.
	query := `CALL {
  MATCH (n) RETURN n
}
RETURN n`

	frag, err := p.Plan(query, "db")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be a simple FragmentExec (no decomposition).
	_, ok := frag.(*FragmentExec)
	if !ok {
		t.Fatalf("expected FragmentExec for non-fabric CALL, got %T", frag)
	}
}

func TestParseLeadingUse_NoUse(t *testing.T) {
	db, remaining, hasUse, err := parseLeadingUse("MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasUse {
		t.Error("expected hasUse=false")
	}
	if db != "" {
		t.Errorf("expected empty db, got %s", db)
	}
	if remaining != "MATCH (n) RETURN n" {
		t.Errorf("expected original query, got %s", remaining)
	}
}

func TestParseLeadingUse_SimpleUse(t *testing.T) {
	db, remaining, hasUse, err := parseLeadingUse("USE mydb MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasUse {
		t.Error("expected hasUse=true")
	}
	if db != "mydb" {
		t.Errorf("expected mydb, got %s", db)
	}
	if remaining != "MATCH (n) RETURN n" {
		t.Errorf("expected 'MATCH (n) RETURN n', got %q", remaining)
	}
}

func TestParseLeadingUse_DottedName(t *testing.T) {
	db, remaining, hasUse, err := parseLeadingUse("USE comp.shard MATCH (n)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasUse {
		t.Error("expected hasUse=true")
	}
	if db != "comp.shard" {
		t.Errorf("expected comp.shard, got %s", db)
	}
	if remaining != "MATCH (n)" {
		t.Errorf("expected 'MATCH (n)', got %q", remaining)
	}
}

func TestParseLeadingUse_BacktickQuoted(t *testing.T) {
	db, _, hasUse, err := parseLeadingUse("USE `my db` MATCH (n)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasUse {
		t.Error("expected hasUse=true")
	}
	if db != "my db" {
		t.Errorf("expected 'my db', got %s", db)
	}
}

func TestParseLeadingUse_NotUseKeyword(t *testing.T) {
	// "USER" starts with "USE" but is not USE.
	_, _, hasUse, _ := parseLeadingUse("USER foo")
	if hasUse {
		t.Error("expected hasUse=false for USER keyword")
	}

	_, _, hasUse, _ = parseLeadingUse("USING PERIODIC COMMIT")
	if hasUse {
		t.Error("expected hasUse=false for USING keyword")
	}
}

func TestFindMatchingBrace(t *testing.T) {
	tests := []struct {
		input string
		pos   int
		want  int
	}{
		{"{}", 0, 1},
		{"{ x }", 0, 4},
		{"{ { nested } }", 0, 13},
		{"{ 'str with }' }", 0, 15},
		{`{ "str with }" }`, 0, 15},
	}

	for _, tt := range tests {
		got, err := findMatchingBrace(tt.input, tt.pos)
		if err != nil {
			t.Errorf("findMatchingBrace(%q, %d): unexpected error: %v", tt.input, tt.pos, err)
			continue
		}
		if got != tt.want {
			t.Errorf("findMatchingBrace(%q, %d) = %d, want %d", tt.input, tt.pos, got, tt.want)
		}
	}
}

func TestFindMatchingBrace_Unmatched(t *testing.T) {
	_, err := findMatchingBrace("{ unclosed", 0)
	if err == nil {
		t.Fatal("expected error for unmatched brace")
	}
}

func TestExtractWithImports(t *testing.T) {
	tests := []struct {
		body string
		want []string
	}{
		{"WITH translationId MATCH (n)", []string{"translationId"}},
		{"WITH a, b MATCH (n)", []string{"a", "b"}},
		{"MATCH (n) RETURN n", nil},
		{"WITH x RETURN x", []string{"x"}},
		{"WITHOUT something", nil},
	}

	for _, tt := range tests {
		got := extractWithImports(tt.body)
		if len(got) != len(tt.want) {
			t.Errorf("extractWithImports(%q) = %v, want %v", tt.body, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("extractWithImports(%q)[%d] = %s, want %s", tt.body, i, got[i], tt.want[i])
			}
		}
	}
}

func TestQueryIsWrite(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{"MATCH (n) RETURN n", false},
		{"CREATE (n:Test)", true},
		{"MERGE (n:Test {id: 1})", true},
		{"MATCH (n) DELETE n", true},
		{"MATCH (n) DETACH DELETE n", true},
		{"MATCH (n) SET n.x = 1 RETURN n", true},
		{"MATCH (n) REMOVE n.x RETURN n", true},
		{"RETURN 1", false},
	}

	for _, tt := range tests {
		got := queryIsWrite(tt.query)
		if got != tt.want {
			t.Errorf("queryIsWrite(%q) = %v, want %v", tt.query, got, tt.want)
		}
	}
}
