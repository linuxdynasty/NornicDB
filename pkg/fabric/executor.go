package fabric

import (
	"context"
	"fmt"
	"regexp"
	"strings"
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
		_, err := tx.GetOrOpen(f.GraphName, f.IsWrite)
		if err != nil {
			return nil, err
		}
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

// executeApply implements correlated subquery semantics:
// for each row from Input, execute Inner with imported variables.
func (e *FabricExecutor) executeApply(ctx context.Context, tx *FabricTransaction, f *FragmentApply, params map[string]interface{}, authToken string) (*ResultStream, error) {
	// Execute the outer (Input) fragment.
	inputResult, err := e.Execute(ctx, tx, f.Input, params, authToken)
	if err != nil {
		return nil, fmt.Errorf("apply input failed: %w", err)
	}

	result := &ResultStream{}

	// For each input row, execute the inner fragment with imported variables.
	for _, inputRow := range inputResult.Rows {
		// Build parameter map for the inner execution by merging input columns
		// as parameters. This enables correlated subqueries where the inner
		// query references variables from the outer scope.
		innerParams := mergeRowParams(params, inputResult.Columns, inputRow)

		innerFragment := f.Inner
		if execFrag, ok := f.Inner.(*FragmentExec); ok {
			importCols := importColumnsFromFragment(execFrag.Input)
			if rewritten := rewriteLeadingWithImports(execFrag.Query, importCols); rewritten != execFrag.Query {
				copied := *execFrag
				copied.Query = rewritten
				innerFragment = &copied
			}
		}

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

var leadingWithRegex = regexp.MustCompile(`(?is)^\\s*WITH\\s+(.+?)\\b(MATCH|RETURN|CALL|CREATE|MERGE|UNWIND|OPTIONAL|WHERE|SET|DELETE)\\b`)

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

	m := leadingWithRegex.FindStringSubmatchIndex(trimmed)
	if len(m) < 6 {
		return query
	}
	keyword := trimmed[m[4]:m[5]]
	rest := strings.TrimSpace(trimmed[m[5]:])
	return "WITH " + strings.Join(assignments, ", ") + " " + keyword + " " + rest
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
