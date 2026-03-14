package fabric

import (
	"strings"
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

func TestPlan_PreservesOuterClausesAroundCallUseBlocks(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	catalog.Register("nornic.tr", &LocationRemote{DBName: "tr", URI: "bolt://a:7687"})
	p := NewFabricPlanner(catalog)

	query := `USE nornic
MATCH (seed:Seed) RETURN seed.id AS seedId
CALL {
  USE nornic.tr
  WITH seedId
  MATCH (t:Translation {id: seedId})
  RETURN t.text AS translated
}
RETURN seedId, translated`

	frag, err := p.Plan(query, "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rootApply, ok := frag.(*FragmentApply)
	if !ok {
		t.Fatalf("expected FragmentApply root, got %T", frag)
	}

	trailingExec, ok := rootApply.Inner.(*FragmentExec)
	if !ok {
		t.Fatalf("expected trailing FragmentExec, got %T", rootApply.Inner)
	}
	if trailingExec.GraphName != "nornic" {
		t.Fatalf("expected trailing exec on nornic, got %s", trailingExec.GraphName)
	}

	callApply, ok := rootApply.Input.(*FragmentApply)
	if !ok {
		t.Fatalf("expected call apply, got %T", rootApply.Input)
	}
	callExec, ok := callApply.Inner.(*FragmentExec)
	if !ok {
		t.Fatalf("expected call exec, got %T", callApply.Inner)
	}
	if callExec.GraphName != "nornic.tr" {
		t.Fatalf("expected call exec on nornic.tr, got %s", callExec.GraphName)
	}

	prefixApply, ok := callApply.Input.(*FragmentApply)
	if !ok {
		t.Fatalf("expected prefix apply before call block, got %T", callApply.Input)
	}
	prefixExec, ok := prefixApply.Inner.(*FragmentExec)
	if !ok {
		t.Fatalf("expected prefix exec, got %T", prefixApply.Inner)
	}
	if prefixExec.GraphName != "nornic" {
		t.Fatalf("expected prefix exec on nornic, got %s", prefixExec.GraphName)
	}
	if prefixExec.Query == "" {
		t.Fatalf("expected preserved prefix query")
	}
}

func TestPlan_UnionPartUse(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("g1", &LocationLocal{DBName: "g1"})
	catalog.Register("g2", &LocationLocal{DBName: "g2"})
	p := NewFabricPlanner(catalog)

	frag, err := p.Plan("USE g1 RETURN 1 AS x UNION ALL USE g2 RETURN 2 AS x", "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	u, ok := frag.(*FragmentUnion)
	if !ok {
		t.Fatalf("expected FragmentUnion, got %T", frag)
	}
	if u.Distinct {
		t.Fatalf("expected UNION ALL to set Distinct=false")
	}

	lhs, ok := u.LHS.(*FragmentExec)
	if !ok {
		t.Fatalf("expected lhs FragmentExec, got %T", u.LHS)
	}
	rhs, ok := u.RHS.(*FragmentExec)
	if !ok {
		t.Fatalf("expected rhs FragmentExec, got %T", u.RHS)
	}
	if lhs.GraphName != "g1" || rhs.GraphName != "g2" {
		t.Fatalf("unexpected union graph routing lhs=%s rhs=%s", lhs.GraphName, rhs.GraphName)
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

func TestPlan_CallWithThenUseSubquery(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("translations", &LocationLocal{DBName: "translations"})
	catalog.Register("translations.tr", &LocationLocal{DBName: "nornic_tr"})
	catalog.Register("translations.txr", &LocationLocal{DBName: "nornic_txt"})
	p := NewFabricPlanner(catalog)

	query := `USE translations
CALL {
  USE translations.tr
  MATCH (t)
  RETURN t.textKey AS textKey, t.textKey128 AS textKey128
  LIMIT 2
}
CALL {
  WITH textKey128
  USE translations.txr
  MATCH (tt)
  WHERE tt.textKey128 = textKey128
  RETURN collect(tt) AS texts
}
RETURN textKey, textKey128, texts`

	frag, err := p.Plan(query, "translations")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	root, ok := frag.(*FragmentApply)
	if !ok {
		t.Fatalf("expected FragmentApply root, got %T", frag)
	}
	var execs []*FragmentExec
	var walk func(Fragment)
	walk = func(f Fragment) {
		switch n := f.(type) {
		case *FragmentExec:
			execs = append(execs, n)
		case *FragmentApply:
			walk(n.Input)
			walk(n.Inner)
		case *FragmentUnion:
			walk(n.LHS)
			walk(n.RHS)
		}
	}
	walk(root)

	var foundTR, foundTXR bool
	for _, ex := range execs {
		switch ex.GraphName {
		case "translations.tr":
			foundTR = true
		case "translations.txr":
			foundTXR = true
			if startsWithFold(strings.TrimSpace(ex.Query), "USE") {
				t.Fatalf("expected planner to strip USE from second call body, got query: %s", ex.Query)
			}
			if !startsWithFold(strings.TrimSpace(ex.Query), "WITH") {
				t.Fatalf("expected second call query to preserve WITH imports, got: %s", ex.Query)
			}
		}
	}
	if !foundTR {
		names := make([]string, 0, len(execs))
		for _, ex := range execs {
			names = append(names, ex.GraphName+"::"+strings.TrimSpace(ex.Query))
		}
		t.Fatalf("expected a call fragment routed to translations.tr; got %v", names)
	}
	if !foundTXR {
		names := make([]string, 0, len(execs))
		for _, ex := range execs {
			names = append(names, ex.GraphName+"::"+strings.TrimSpace(ex.Query))
		}
		t.Fatalf("expected a call fragment routed to translations.txr; got %v", names)
	}
}

func TestCallBlockContainsFabricUse_WithThenUse(t *testing.T) {
	body := `
WITH textKey128
USE translations.txr
MATCH (tt)
WHERE tt.textKey128 = textKey128
RETURN collect(tt) AS texts
`
	ok, err := callBlockContainsFabricUse(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected WITH ... USE ... block to be detected as fabric block")
	}
}

func TestCallBlockContainsFabricUse_WithPipelineThenUse(t *testing.T) {
	body := `
WITH rows
UNWIND rows AS r
WITH collect(DISTINCT r.textKey128) AS keys
USE translations.txr
MATCH (tt)
WHERE tt.textKey128 IN keys
RETURN collect(tt) AS texts
`
	ok, err := callBlockContainsFabricUse(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected WITH/UNWIND/.../USE block to be detected as fabric block")
	}
}

func TestPlan_MidQueryUseAfterPipeline(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("translations", &LocationLocal{DBName: "translations"})
	catalog.Register("translations.txr", &LocationLocal{DBName: "caremark_txt"})
	p := NewFabricPlanner(catalog)

	query := `WITH rows
UNWIND rows AS r
WITH collect(DISTINCT r.textKey128) AS keys
USE translations.txr
MATCH (tt:MongoDocument)
WHERE tt.textKey128 IN keys
RETURN tt.textKey128 AS k, collect(tt) AS texts`

	frag, err := p.Plan(query, "translations")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	root, ok := frag.(*FragmentApply)
	if !ok {
		t.Fatalf("expected apply root for mid-query USE, got %T", frag)
	}
	prefixExec, ok := root.Input.(*FragmentApply)
	if !ok {
		t.Fatalf("expected prefix apply, got %T", root.Input)
	}
	pe, ok := prefixExec.Inner.(*FragmentExec)
	if !ok {
		t.Fatalf("expected prefix exec, got %T", prefixExec.Inner)
	}
	if pe.GraphName != "translations" {
		t.Fatalf("expected prefix graph translations, got %s", pe.GraphName)
	}
	up := strings.TrimSpace(strings.ToUpper(pe.Query))
	if !(strings.HasSuffix(up, "RETURN KEYS") || strings.HasSuffix(up, "RETURN *")) {
		t.Fatalf("expected prefix query to materialize row bindings via RETURN alias, got %q", pe.Query)
	}
	innerExec, ok := root.Inner.(*FragmentExec)
	if !ok {
		t.Fatalf("expected inner exec after USE, got %T", root.Inner)
	}
	if innerExec.GraphName != "translations.txr" {
		t.Fatalf("expected USE-routed graph translations.txr, got %s", innerExec.GraphName)
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

func TestPlan_NestedCallUseSubquery(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	catalog.Register("nornic.tr", &LocationRemote{DBName: "tr", URI: "bolt://a:7687"})
	p := NewFabricPlanner(catalog)

	query := `USE nornic
WITH "t-1" AS outerId
CALL {
  WITH outerId
  CALL {
    USE nornic.tr
    WITH outerId
    MATCH (t:Translation {id: outerId})
    RETURN t.id AS translationId
  }
  RETURN translationId
}
RETURN translationId`

	frag, err := p.Plan(query, "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	nestedExec := findExecByGraphName(frag, "nornic.tr")
	if nestedExec == nil {
		t.Fatalf("expected nested nornic.tr fragment in plan tree")
	}

	init, ok := nestedExec.Input.(*FragmentInit)
	if !ok {
		t.Fatalf("expected nested exec input to be FragmentInit, got %T", nestedExec.Input)
	}
	if len(init.ImportColumns) != 1 || init.ImportColumns[0] != "outerId" {
		t.Fatalf("expected nested import columns [outerId], got %v", init.ImportColumns)
	}
}

func TestPlan_SubqueryUseTargetOutOfScope(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("comp1", &LocationLocal{DBName: "comp1"})
	catalog.Register("comp1.a", &LocationLocal{DBName: "comp1_a"})
	catalog.Register("comp2", &LocationLocal{DBName: "comp2"})
	catalog.Register("comp2.b", &LocationLocal{DBName: "comp2_b"})
	p := NewFabricPlanner(catalog)

	query := `USE comp1
CALL {
  USE comp2.b
  RETURN 1 AS x
}
RETURN x`

	_, err := p.Plan(query, "comp1")
	if err == nil {
		t.Fatal("expected out-of-scope USE error")
	}
}

func TestPlan_SubqueryUseTargetNotFound(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	p := NewFabricPlanner(catalog)

	query := `USE nornic
CALL {
  USE nornic.missing
  RETURN 1 AS x
}
RETURN x`

	_, err := p.Plan(query, "nornic")
	if err == nil {
		t.Fatal("expected missing USE target error")
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

func TestParseLeadingUse_DynamicGraphReference(t *testing.T) {
	db, remaining, hasUse, err := parseLeadingUse("USE graph.byName('tenant_a') MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasUse {
		t.Fatal("expected hasUse=true")
	}
	if db != "tenant_a" {
		t.Fatalf("expected tenant_a, got %q", db)
	}
	if remaining != "MATCH (n) RETURN n" {
		t.Fatalf("unexpected remaining query: %q", remaining)
	}

	db, _, hasUse, err = parseLeadingUse("USE graph.byElementId('tenant_b') RETURN 1")
	if err != nil {
		t.Fatalf("unexpected byElementId error: %v", err)
	}
	if !hasUse || db != "tenant_b" {
		t.Fatalf("expected tenant_b from byElementId, got hasUse=%v db=%q", hasUse, db)
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
		{"with a, b match (n)", []string{"a", "b"}},
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

func findExecByGraphName(fragment Fragment, graphName string) *FragmentExec {
	switch f := fragment.(type) {
	case *FragmentExec:
		if f.GraphName == graphName {
			return f
		}
		return nil
	case *FragmentApply:
		if found := findExecByGraphName(f.Input, graphName); found != nil {
			return found
		}
		return findExecByGraphName(f.Inner, graphName)
	case *FragmentUnion:
		if found := findExecByGraphName(f.LHS, graphName); found != nil {
			return found
		}
		return findExecByGraphName(f.RHS, graphName)
	default:
		return nil
	}
}
