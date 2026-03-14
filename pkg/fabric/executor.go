package fabric

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
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
		recordBindings, _ := RecordBindingsFromContext(ctx)
		res, err := e.local.ExecuteWithRecord(ctx, l, f.Query, params, recordBindings)
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
	returnIdx := lastKeywordIndexFold(query, "RETURN")
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
		if as := lastAsIndexFold(item); as >= 0 {
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

func lastKeywordIndexFold(s, keyword string) int {
	if len(keyword) == 0 || len(s) < len(keyword) {
		return -1
	}
	for i := len(s) - len(keyword); i >= 0; i-- {
		if hasKeywordAt(s, i, keyword) {
			return i
		}
	}
	return -1
}

func lastAsIndexFold(s string) int {
	if len(s) < 4 {
		return -1
	}
	for i := len(s) - 4; i >= 0; i-- {
		if strings.EqualFold(s[i:i+4], " AS ") {
			return i
		}
	}
	return -1
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
	innerFragment := f.Inner
	outerIdx := buildColumnIndex(inputResult.Columns)

	// For each input row, execute the inner fragment with imported variables.
	for _, inputRow := range inputResult.Rows {
		innerCtx := WithRecordBindings(ctx, rowBindings(inputResult.Columns, inputRow))
		innerResult, err := e.Execute(innerCtx, tx, innerFragment, params, authToken)
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
		innerIdx := buildColumnIndex(innerResult.Columns)
		for _, innerRow := range innerResult.Rows {
			combined := combineRowsByIndexes(result.Columns, outerIdx, inputRow, innerIdx, innerRow)
			result.Rows = append(result.Rows, combined)
		}
	}

	return result, nil
}

func rowBindings(columns []string, row []interface{}) map[string]interface{} {
	if len(columns) == 0 || len(row) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		if i < len(row) && strings.TrimSpace(col) != "" {
			out[col] = row[i]
		}
	}
	return out
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
		assignments = append(assignments, "$"+col+" AS "+col)
	}
	if len(assignments) == 0 {
		return query
	}

	trimmed := strings.TrimSpace(query)
	if !startsWithFold(trimmed, "WITH ") {
		// For projection-only fragments produced by planner (e.g. trailing
		// RETURN after APPLY), import outer variables explicitly.
		if startsWithFold(trimmed, "RETURN ") {
			return "WITH " + strings.Join(assignments, ", ") + " " + trimmed
		}
		return query
	}

	withEnd, ok := findLeadingWithClauseEnd(trimmed)
	if !ok || withEnd <= 0 {
		return query
	}
	withClause := strings.TrimSpace(trimmed[:withEnd])
	rest := strings.TrimSpace(trimmed[withEnd:])
	if rest == "" {
		return "WITH " + strings.Join(assignments, ", ")
	}
	// Keep correlated semantics for simple import-only WITH clauses by
	// substituting imported vars directly into MATCH predicates.
	if (startsWithFold(rest, "MATCH ") || startsWithFold(rest, "OPTIONAL MATCH ")) &&
		isSimpleWithImportClause(withClause, importCols) {
		return substituteVarsWithParams(rest, importCols)
	}
	return "WITH " + strings.Join(assignments, ", ") + " " + rest
}

func isSimpleWithImportClause(withClause string, importCols []string) bool {
	if len(importCols) == 0 {
		return false
	}
	trimmed := strings.TrimSpace(withClause)
	if !startsWithFold(trimmed, "WITH ") {
		return false
	}
	lhs := strings.TrimSpace(trimmed[len("WITH "):])
	if lhs == "" {
		return false
	}
	parts := splitTopLevelCSV(lhs)
	if len(parts) != len(importCols) {
		return false
	}
	for i, p := range parts {
		if strings.TrimSpace(p) != strings.TrimSpace(importCols[i]) {
			return false
		}
	}
	return true
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
	for _, kw := range clauseStartKeywords {
		if hasKeywordAt(query, idx, kw) {
			return true
		}
	}
	return false
}

var clauseStartKeywords = [...]string{
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
	// Keep write branch execution sequential so shard write-routing remains deterministic.
	// Read-only UNION branches can execute concurrently and then merge in LHS/RHS order.
	if fragmentContainsWrite(f.LHS) || fragmentContainsWrite(f.RHS) {
		return e.executeUnionSequential(ctx, tx, f, params, authToken)
	}
	return e.executeUnionParallel(ctx, tx, f, params, authToken)
}

func (e *FabricExecutor) executeUnionSequential(ctx context.Context, tx *FabricTransaction, f *FragmentUnion, params map[string]interface{}, authToken string) (*ResultStream, error) {
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

func (e *FabricExecutor) executeUnionParallel(ctx context.Context, tx *FabricTransaction, f *FragmentUnion, params map[string]interface{}, authToken string) (*ResultStream, error) {
	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		lhs *ResultStream
		rhs *ResultStream
		err error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		res, runErr := e.Execute(ctx, tx, f.LHS, params, authToken)
		mu.Lock()
		defer mu.Unlock()
		if runErr != nil {
			err = fmt.Errorf("union LHS failed: %w", runErr)
			return
		}
		lhs = res
	}()
	go func() {
		defer wg.Done()
		res, runErr := e.Execute(ctx, tx, f.RHS, params, authToken)
		mu.Lock()
		defer mu.Unlock()
		if runErr != nil {
			if err == nil {
				err = fmt.Errorf("union RHS failed: %w", runErr)
			}
			return
		}
		rhs = res
	}()
	wg.Wait()
	if err != nil {
		return nil, err
	}
	result := &ResultStream{Columns: f.Columns}
	if lhs != nil {
		result.Rows = append(result.Rows, lhs.Rows...)
	}
	if rhs != nil {
		result.Rows = append(result.Rows, rhs.Rows...)
	}
	if f.Distinct {
		result.Rows = deduplicateRows(result.Rows)
	}
	return result, nil
}

func fragmentContainsWrite(fragment Fragment) bool {
	switch f := fragment.(type) {
	case *FragmentExec:
		return f.IsWrite
	case *FragmentApply:
		return fragmentContainsWrite(f.Input) || fragmentContainsWrite(f.Inner)
	case *FragmentUnion:
		return fragmentContainsWrite(f.LHS) || fragmentContainsWrite(f.RHS)
	case *FragmentLeaf:
		return fragmentContainsWrite(f.Input)
	case *FragmentInit:
		return false
	default:
		return false
	}
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
	return combineRowsByIndexes(resultCols, buildColumnIndex(outerCols), outerRow, buildColumnIndex(innerCols), innerRow)
}

func buildColumnIndex(cols []string) map[string]int {
	idx := make(map[string]int, len(cols))
	for i, col := range cols {
		if strings.TrimSpace(col) == "" {
			continue
		}
		idx[col] = i
	}
	return idx
}

func combineRowsByIndexes(resultCols []string, outerIdx map[string]int, outerRow []interface{}, innerIdx map[string]int, innerRow []interface{}) []interface{} {
	combined := make([]interface{}, len(resultCols))
	for i, col := range resultCols {
		if idx, ok := innerIdx[col]; ok && idx < len(innerRow) {
			combined[i] = innerRow[idx]
			continue
		}
		if idx, ok := outerIdx[col]; ok && idx < len(outerRow) {
			combined[i] = outerRow[idx]
		}
	}
	return combined
}

// deduplicateRows removes duplicate rows based on string representation.
func deduplicateRows(rows [][]interface{}) [][]interface{} {
	seen := make(map[uint64]struct{}, len(rows))
	result := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		key := hashRow(row)
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	return result
}

func hashRow(row []interface{}) uint64 {
	h := newFNV64a()
	for _, v := range row {
		writeAnyHash(&h, v)
		h.writeByte(0)
	}
	return h.sum64()
}

func writeAnyHash(h *fnv64a, v interface{}) {
	switch t := v.(type) {
	case nil:
		h.writeByte('n')
	case string:
		h.writeByte('s')
		h.writeString(t)
	case bool:
		h.writeByte('b')
		if t {
			h.writeByte(1)
		} else {
			h.writeByte(0)
		}
	case int:
		writeUint64Hash(h, uint64(t), 'i')
	case int64:
		writeUint64Hash(h, uint64(t), 'I')
	case float64:
		writeUint64Hash(h, math.Float64bits(t), 'f')
	case []interface{}:
		h.writeByte('a')
		for _, item := range t {
			writeAnyHash(h, item)
			h.writeByte(0)
		}
	case map[string]interface{}:
		h.writeByte('m')
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			h.writeString(k)
			h.writeByte('=')
			writeAnyHash(h, t[k])
			h.writeByte(0)
		}
	default:
		h.writeByte('x')
		h.writeString(fmt.Sprintf("%T:%v", t, t))
	}
}

func writeUint64Hash(h *fnv64a, n uint64, marker byte) {
	var b [9]byte
	b[0] = marker
	binary.LittleEndian.PutUint64(b[1:], n)
	h.writeBytes(b[:])
}

type fnv64a struct {
	sum uint64
}

func newFNV64a() fnv64a {
	return fnv64a{sum: 14695981039346656037}
}

func (h *fnv64a) writeByte(b byte) {
	const prime uint64 = 1099511628211
	h.sum ^= uint64(b)
	h.sum *= prime
}

func (h *fnv64a) writeBytes(bs []byte) {
	for _, b := range bs {
		h.writeByte(b)
	}
}

func (h *fnv64a) writeString(s string) {
	for i := 0; i < len(s); i++ {
		h.writeByte(s[i])
	}
}

func (h *fnv64a) sum64() uint64 {
	return h.sum
}
