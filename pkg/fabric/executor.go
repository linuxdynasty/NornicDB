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
		res, err := e.local.Execute(ctx, l, f.Query, params)
		if err != nil {
			return nil, err
		}
		if res != nil && len(res.Columns) == 0 {
			res.Columns = inferReturnColumnsFromQuery(f.Query)
		}
		return res, nil

	case *LocationRemote:
		if e.remote == nil {
			return nil, fmt.Errorf("remote executor not configured")
		}
		res, err := e.remote.Execute(ctx, l, f.Query, params, authToken)
		if err != nil {
			return nil, err
		}
		if res != nil && len(res.Columns) == 0 {
			res.Columns = inferReturnColumnsFromQuery(f.Query)
		}
		return res, nil

	default:
		return nil, fmt.Errorf("unsupported location type: %T", loc)
	}
}

func inferReturnColumnsFromQuery(query string) []string {
	upper := strings.ToUpper(query)
	returnIdx := strings.LastIndex(upper, "RETURN")
	if returnIdx < 0 {
		return nil
	}
	clause := strings.TrimSpace(query[returnIdx+len("RETURN"):])
	if clause == "" {
		return nil
	}
	// Trim trailing semicolon.
	clause = strings.TrimSuffix(clause, ";")
	// Strip top-level ORDER BY / SKIP / LIMIT tails.
	end := len(clause)
	paren, bracket, brace := 0, 0, 0
	inSingle, inDouble, inBacktick := false, false, false
	for i := 0; i < len(clause); i++ {
		ch := clause[i]
		switch {
		case inSingle:
			if ch == '\'' {
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
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '(':
			paren++
		case ')':
			if paren > 0 {
				paren--
			}
		case '[':
			bracket++
		case ']':
			if bracket > 0 {
				bracket--
			}
		case '{':
			brace++
		case '}':
			if brace > 0 {
				brace--
			}
		}
		if paren != 0 || bracket != 0 || brace != 0 {
			continue
		}
		if hasKeywordAt(clause, i, "ORDER BY") || hasKeywordAt(clause, i, "SKIP") || hasKeywordAt(clause, i, "LIMIT") {
			end = i
			break
		}
	}
	clause = strings.TrimSpace(clause[:end])
	parts := splitTopLevelCSV(clause)
	cols := make([]string, 0, len(parts))
	for _, p := range parts {
		item := strings.TrimSpace(p)
		if item == "" {
			continue
		}
		up := strings.ToUpper(item)
		if as := strings.LastIndex(up, " AS "); as >= 0 {
			alias := strings.TrimSpace(item[as+4:])
			if alias != "" {
				cols = append(cols, strings.Trim(alias, "`"))
				continue
			}
		}
		cols = append(cols, item)
	}
	return cols
}

func splitTopLevelCSV(s string) []string {
	var parts []string
	start := 0
	paren, bracket, brace := 0, 0, 0
	inSingle, inDouble, inBacktick := false, false, false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case inSingle:
			if ch == '\'' {
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
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '(':
			paren++
		case ')':
			if paren > 0 {
				paren--
			}
		case '[':
			bracket++
		case ']':
			if bracket > 0 {
				bracket--
			}
		case '{':
			brace++
		case '}':
			if brace > 0 {
				brace--
			}
		case ',':
			if paren == 0 && bracket == 0 && brace == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
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

	result := &ResultStream{Columns: append([]string(nil), f.Columns...)}
	innerFragment := rewriteFragmentWithImports(f.Inner)

	// For each input row, execute the inner fragment with imported variables.
	for _, inputRow := range inputResult.Rows {
		innerForRow := innerFragment
		if !fragmentHasStaticImports(innerFragment) {
			innerForRow = rewriteFragmentWithRuntimeImports(innerFragment, inputResult.Columns)
		}
		// Build parameter map for the inner execution by merging input columns
		// as parameters. This enables correlated subqueries where the inner
		// query references variables from the outer scope.
		innerParams := mergeRowParams(params, inputResult.Columns, inputRow)

		innerResult, err := e.Execute(ctx, tx, innerForRow, innerParams, authToken)
		if err != nil {
			return nil, fmt.Errorf("apply inner failed: %w", err)
		}

		if innerResult == nil {
			continue
		}

		// Combine input and inner columns/rows.
		if len(result.Columns) == 0 {
			result.Columns = combineColumns(inputResult.Columns, innerResult.Columns)
		}
		if len(innerResult.Rows) == 0 {
			continue
		}

		for _, innerRow := range innerResult.Rows {
			combined := combineRowsByColumns(result.Columns, inputResult.Columns, inputRow, innerResult.Columns, innerRow)
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

func rewriteFragmentWithRuntimeImports(fragment Fragment, runtimeCols []string) Fragment {
	if fragment == nil {
		return nil
	}
	switch f := fragment.(type) {
	case *FragmentExec:
		importCols := importColumnsFromFragment(f.Input)
		merged := mergeImportColumns(runtimeCols, importCols)
		if rewritten := rewriteLeadingWithImports(f.Query, merged); rewritten != f.Query {
			copied := *f
			copied.Query = rewritten
			return &copied
		}
		return fragment
	case *FragmentApply:
		copied := *f
		copied.Input = rewriteFragmentWithRuntimeImports(copied.Input, runtimeCols)
		copied.Inner = rewriteFragmentWithRuntimeImports(copied.Inner, runtimeCols)
		return &copied
	case *FragmentUnion:
		copied := *f
		copied.LHS = rewriteFragmentWithRuntimeImports(copied.LHS, runtimeCols)
		copied.RHS = rewriteFragmentWithRuntimeImports(copied.RHS, runtimeCols)
		return &copied
	default:
		return fragment
	}
}

func mergeImportColumns(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, col := range a {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		if _, ok := seen[col]; ok {
			continue
		}
		seen[col] = struct{}{}
		out = append(out, col)
	}
	for _, col := range b {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		if _, ok := seen[col]; ok {
			continue
		}
		seen[col] = struct{}{}
		out = append(out, col)
	}
	return out
}

func fragmentHasStaticImports(fragment Fragment) bool {
	switch f := fragment.(type) {
	case *FragmentExec:
		return len(importColumnsFromFragment(f.Input)) > 0
	case *FragmentApply:
		return fragmentHasStaticImports(f.Input) || fragmentHasStaticImports(f.Inner)
	case *FragmentUnion:
		return fragmentHasStaticImports(f.LHS) || fragmentHasStaticImports(f.RHS)
	default:
		return false
	}
}

func rewriteLeadingWithImports(query string, importCols []string) string {
	if len(importCols) == 0 {
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

	trimmed := strings.TrimSpace(query)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "WITH ") {
		return "WITH " + strings.Join(assignments, ", ") + " " + trimmed
	}

	withEnd, ok := findLeadingWithClauseEnd(trimmed)
	if !ok || withEnd <= 0 {
		return query
	}
	rest := strings.TrimSpace(trimmed[withEnd:])
	if rest == "" {
		return "WITH " + strings.Join(assignments, ", ")
	}
	upperRest := strings.ToUpper(rest)
	if strings.HasPrefix(upperRest, "MATCH ") || strings.HasPrefix(upperRest, "OPTIONAL MATCH ") {
		return substituteVarsWithParams(rest, importCols)
	}
	return "WITH " + strings.Join(assignments, ", ") + " " + rest
}

func substituteVarsWithParams(query string, vars []string) string {
	out := query
	for _, v := range vars {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		out = replaceStandaloneVar(out, v, "$"+v)
	}
	return out
}

func replaceStandaloneVar(s, ident, replacement string) string {
	if ident == "" || len(s) < len(ident) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(s); {
		ch := s[i]
		switch {
		case inSingle:
			b.WriteByte(ch)
			if ch == '\'' {
				inSingle = false
			}
			i++
			continue
		case inDouble:
			b.WriteByte(ch)
			if ch == '"' {
				inDouble = false
			}
			i++
			continue
		case inBacktick:
			b.WriteByte(ch)
			if ch == '`' {
				inBacktick = false
			}
			i++
			continue
		}

		if ch == '\'' {
			inSingle = true
			b.WriteByte(ch)
			i++
			continue
		}
		if ch == '"' {
			inDouble = true
			b.WriteByte(ch)
			i++
			continue
		}
		if ch == '`' {
			inBacktick = true
			b.WriteByte(ch)
			i++
			continue
		}

		if i+len(ident) <= len(s) && s[i:i+len(ident)] == ident {
			prevOK := i == 0 || (!isIdentChar(s[i-1]) && s[i-1] != '.')
			next := i + len(ident)
			nextOK := next >= len(s) || !isIdentChar(s[next])
			if prevOK && nextOK {
				b.WriteString(replacement)
				i = next
				continue
			}
		}
		b.WriteByte(ch)
		i++
	}
	return b.String()
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
		"USE",
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

func combineRowsByColumns(resultCols, outerCols []string, outerRow []interface{}, innerCols []string, innerRow []interface{}) []interface{} {
	valueByCol := make(map[string]interface{}, len(outerCols)+len(innerCols))
	for i, col := range outerCols {
		if i < len(outerRow) {
			valueByCol[col] = outerRow[i]
		}
	}
	for i, col := range innerCols {
		if i < len(innerRow) {
			// Inner values override on duplicate names, matching APPLY scoping behavior.
			valueByCol[col] = innerRow[i]
		}
	}

	combined := make([]interface{}, len(resultCols))
	for i, col := range resultCols {
		combined[i] = valueByCol[col]
	}
	return combined
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
