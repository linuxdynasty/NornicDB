package fabric

import (
	"context"
	"fmt"
	"strings"
	"unicode"
)

// FabricExecutor walks a Fragment tree and dispatches each FragmentExec
// to either a local or remote executor based on the graph location resolved
// from the Catalog.
//
// This mirrors Neo4j's FabricExecutor.java.
type FabricExecutor struct {
	catalog *Catalog
	local   *LocalFragmentExecutor
	remote  *RemoteFragmentExecutor
}

// NewFabricExecutor creates a fabric executor.
func NewFabricExecutor(catalog *Catalog, local *LocalFragmentExecutor, remote *RemoteFragmentExecutor) *FabricExecutor {
	return &FabricExecutor{
		catalog: catalog,
		local:   local,
		remote:  remote,
	}
}

// Execute runs a Fragment tree within the context of a FabricTransaction.
//
// Parameters:
//   - ctx: context for cancellation/deadline propagation
//   - tx: the distributed transaction (may be nil for auto-commit)
//   - fragment: the root of the Fragment tree to execute
//   - params: query parameters
//   - authToken: the caller's auth token for OIDC forwarding to remote shards
func (e *FabricExecutor) Execute(ctx context.Context, tx *FabricTransaction, fragment Fragment, params map[string]interface{}, authToken string) (*ResultStream, error) {
	switch f := fragment.(type) {
	case *FragmentInit:
		return e.executeInit(f)
	case *FragmentLeaf:
		return nil, fmt.Errorf("FragmentLeaf should be resolved to FragmentExec before execution")
	case *FragmentExec:
		return e.executeExec(ctx, tx, f, params, authToken)
	case *FragmentApply:
		return e.executeApply(ctx, tx, f, params, authToken)
	case *FragmentUnion:
		return e.executeUnion(ctx, tx, f, params, authToken)
	default:
		return nil, fmt.Errorf("unknown fragment type: %T", fragment)
	}
}

// executeInit produces a single empty row — the entry point of the Fragment tree.
func (e *FabricExecutor) executeInit(f *FragmentInit) (*ResultStream, error) {
	// A single empty row with no columns, matching Neo4j's Init semantics.
	return &ResultStream{
		Columns: f.Columns,
		Rows:    [][]interface{}{{}},
	}, nil
}

// executeExec dispatches a bound executable fragment to local or remote.
func (e *FabricExecutor) executeExec(ctx context.Context, tx *FabricTransaction, f *FragmentExec, params map[string]interface{}, authToken string) (*ResultStream, error) {
	// Resolve graph location from catalog.
	loc, err := e.catalog.Resolve(f.GraphName)
	if err != nil {
		return nil, fmt.Errorf("cannot route query: %w", err)
	}

	// Register with the distributed transaction if present.
	if tx != nil {
		participant := participantKeyFromLocation(loc)
		sub, err := tx.GetOrOpen(participant, f.IsWrite)
		if err != nil {
			return nil, err
		}
		ctx = WithFabricTransaction(ctx, tx)
		ctx = WithSubTransaction(ctx, sub)
	}

	switch l := loc.(type) {
	case *LocationLocal:
		if e.local == nil {
			return nil, fmt.Errorf("local executor not configured")
		}
		return e.local.Execute(ctx, l, f.Query, params)

	case *LocationRemote:
		if e.remote == nil {
			return nil, fmt.Errorf("remote executor not configured")
		}
		return e.remote.Execute(ctx, l, f.Query, params, authToken)

	default:
		return nil, fmt.Errorf("unsupported location type: %T", loc)
	}
}

func participantKeyFromLocation(loc Location) string {
	switch l := loc.(type) {
	case *LocationLocal:
		return "local:" + strings.TrimSpace(l.DBName)
	case *LocationRemote:
		return "remote:" + strings.TrimSpace(l.URI) + "|" + strings.TrimSpace(l.DBName)
	default:
		return fmt.Sprintf("unknown:%T", loc)
	}
}

// executeApply implements correlated subquery semantics:
// for each row from Input, execute Inner with imported variables.
func (e *FabricExecutor) executeApply(ctx context.Context, tx *FabricTransaction, f *FragmentApply, params map[string]interface{}, authToken string) (*ResultStream, error) {
	// Execute the outer (Input) fragment.
	inputResult, err := e.Execute(ctx, tx, f.Input, params, authToken)
	if err != nil {
		return nil, fmt.Errorf("apply input failed: %w", err)
	}

	result := &ResultStream{}
	innerFragment := rewriteFragmentWithImports(f.Inner)

	// For each input row, execute the inner fragment with imported variables.
	for _, inputRow := range inputResult.Rows {
		// Build parameter map for the inner execution by merging input columns
		// as parameters. This enables correlated subqueries where the inner
		// query references variables from the outer scope.
		innerParams := mergeRowParams(params, inputResult.Columns, inputRow)

		innerResult, err := e.Execute(ctx, tx, innerFragment, innerParams, authToken)
		if err != nil {
			return nil, fmt.Errorf("apply inner failed: %w", err)
		}

		if innerResult == nil || len(innerResult.Rows) == 0 {
			continue
		}

		// Combine input and inner columns/rows.
		if len(result.Columns) == 0 {
			result.Columns = combineColumns(inputResult.Columns, innerResult.Columns)
		}

		for _, innerRow := range innerResult.Rows {
			combined := make([]interface{}, 0, len(inputRow)+len(innerRow))
			combined = append(combined, inputRow...)
			combined = append(combined, innerRow...)
			result.Rows = append(result.Rows, combined)
		}
	}

	return result, nil
}

func importColumnsFromFragment(f Fragment) []string {
	if f == nil {
		return nil
	}
	if init, ok := f.(*FragmentInit); ok {
		if len(init.ImportColumns) > 0 {
			return init.ImportColumns
		}
		return init.Columns
	}
	return nil
}

func rewriteFragmentWithImports(fragment Fragment) Fragment {
	if fragment == nil {
		return nil
	}

	switch f := fragment.(type) {
	case *FragmentExec:
		importCols := importColumnsFromFragment(f.Input)
		if rewritten := rewriteLeadingWithImports(f.Query, importCols); rewritten != f.Query {
			copied := *f
			copied.Query = rewritten
			return &copied
		}
		return fragment
	case *FragmentApply:
		copied := *f
		copied.Input = rewriteFragmentWithImports(copied.Input)
		copied.Inner = rewriteFragmentWithImports(copied.Inner)
		return &copied
	case *FragmentUnion:
		copied := *f
		copied.LHS = rewriteFragmentWithImports(copied.LHS)
		copied.RHS = rewriteFragmentWithImports(copied.RHS)
		return &copied
	default:
		return fragment
	}
}

func rewriteLeadingWithImports(query string, importCols []string) string {
	if len(importCols) == 0 {
		return query
	}
	trimmed := strings.TrimSpace(query)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "WITH ") {
		return query
	}

	assignments := make([]string, 0, len(importCols))
	for _, col := range importCols {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		assignments = append(assignments, fmt.Sprintf("$%s AS %s", col, col))
	}
	if len(assignments) == 0 {
		return query
	}
	withEnd, ok := findLeadingWithClauseEnd(trimmed)
	if !ok || withEnd <= 0 {
		return query
	}
	rest := strings.TrimSpace(trimmed[withEnd:])
	if rest == "" {
		return "WITH " + strings.Join(assignments, ", ")
	}
	return "WITH " + strings.Join(assignments, ", ") + " " + rest
}

func findLeadingWithClauseEnd(query string) (int, bool) {
	if query == "" {
		return 0, false
	}
	i := skipLeadingSpace(query, 0)
	if !hasKeywordAt(query, i, "WITH") {
		return 0, false
	}
	i += len("WITH")
	i = skipLeadingSpace(query, i)

	depth := 0
	inSingle := false
	inDouble := false
	inBacktick := false

	for idx := i; idx < len(query); idx++ {
		ch := query[idx]

		switch {
		case inSingle:
			if ch == '\'' {
				if idx+1 < len(query) && query[idx+1] == '\'' {
					idx++
					continue
				}
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		switch ch {
		case '\'':
			inSingle = true
			continue
		case '"':
			inDouble = true
			continue
		case '`':
			inBacktick = true
			continue
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}

		if depth == 0 && isCypherClauseStart(query, idx) {
			return idx, true
		}
	}

	return len(query), true
}

func isCypherClauseStart(query string, idx int) bool {
	keywords := []string{
		"OPTIONAL MATCH",
		"DETACH DELETE",
		"ORDER BY",
		"LOAD CSV",
		"MATCH",
		"RETURN",
		"CALL",
		"CREATE",
		"MERGE",
		"UNWIND",
		"WHERE",
		"SET",
		"DELETE",
		"WITH",
		"FOREACH",
		"UNION",
		"LIMIT",
		"SKIP",
	}
	for _, kw := range keywords {
		if hasKeywordAt(query, idx, kw) {
			return true
		}
	}
	return false
}

func hasKeywordAt(query string, idx int, keyword string) bool {
	if idx < 0 || idx+len(keyword) > len(query) {
		return false
	}
	if idx > 0 {
		prev := rune(query[idx-1])
		if unicode.IsLetter(prev) || unicode.IsDigit(prev) || prev == '_' {
			return false
		}
	}
	segment := query[idx : idx+len(keyword)]
	if !strings.EqualFold(segment, keyword) {
		return false
	}
	end := idx + len(keyword)
	if end < len(query) {
		next := rune(query[end])
		if unicode.IsLetter(next) || unicode.IsDigit(next) || next == '_' {
			return false
		}
	}
	return true
}

func skipLeadingSpace(s string, idx int) int {
	for idx < len(s) && unicode.IsSpace(rune(s[idx])) {
		idx++
	}
	return idx
}

// executeUnion executes both branches and merges results.
func (e *FabricExecutor) executeUnion(ctx context.Context, tx *FabricTransaction, f *FragmentUnion, params map[string]interface{}, authToken string) (*ResultStream, error) {
	lhsResult, err := e.Execute(ctx, tx, f.LHS, params, authToken)
	if err != nil {
		return nil, fmt.Errorf("union LHS failed: %w", err)
	}

	rhsResult, err := e.Execute(ctx, tx, f.RHS, params, authToken)
	if err != nil {
		return nil, fmt.Errorf("union RHS failed: %w", err)
	}

	result := &ResultStream{
		Columns: f.Columns,
	}

	if lhsResult != nil {
		result.Rows = append(result.Rows, lhsResult.Rows...)
	}
	if rhsResult != nil {
		result.Rows = append(result.Rows, rhsResult.Rows...)
	}

	if f.Distinct {
		result.Rows = deduplicateRows(result.Rows)
	}

	return result, nil
}

// mergeRowParams creates a parameter map that includes both the original params
// and the column values from an input row (for correlated subqueries).
func mergeRowParams(params map[string]interface{}, columns []string, row []interface{}) map[string]interface{} {
	merged := make(map[string]interface{}, len(params)+len(columns))
	for k, v := range params {
		merged[k] = v
	}
	for i, col := range columns {
		if i < len(row) {
			merged[col] = row[i]
		}
	}
	return merged
}

// combineColumns merges two column lists, deduplicating names.
func combineColumns(outer, inner []string) []string {
	seen := make(map[string]bool, len(outer))
	result := make([]string, 0, len(outer)+len(inner))
	for _, col := range outer {
		seen[col] = true
		result = append(result, col)
	}
	for _, col := range inner {
		if !seen[col] {
			result = append(result, col)
		}
	}
	return result
}

// deduplicateRows removes duplicate rows based on string representation.
func deduplicateRows(rows [][]interface{}) [][]interface{} {
	seen := make(map[string]bool, len(rows))
	result := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		key := fmt.Sprintf("%v", row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}
