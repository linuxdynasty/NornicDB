package fabric

import (
	"fmt"
	"strings"
)

// FabricPlanner decomposes a Cypher query into a Fragment tree by splitting
// at USE-clause boundaries. Without USE clauses, it produces a single
// FragmentExec targeting the session database — identical to community behavior.
//
// This mirrors Neo4j's FabricPlanner.scala.
type FabricPlanner struct {
	catalog *Catalog
}

func extractGraphReference(s string) (string, string, bool, error) {
	trimmed := strings.TrimSpace(s)
	lower := strings.ToLower(trimmed)

	for _, prefix := range []string{"graph.byname(", "graph.byelementid("} {
		if !strings.HasPrefix(lower, prefix) {
			continue
		}
		openIdx := strings.Index(trimmed, "(")
		if openIdx < 0 {
			return "", "", true, fmt.Errorf("invalid graph reference")
		}
		closeIdx, err := findMatchingParen(trimmed, openIdx)
		if err != nil {
			return "", "", true, err
		}
		args := strings.TrimSpace(trimmed[openIdx+1 : closeIdx])
		if args == "" {
			return "", "", true, fmt.Errorf("graph reference requires an argument")
		}
		arg, err := extractFirstGraphRefArg(args)
		if err != nil {
			return "", "", true, err
		}
		return arg, trimmed[closeIdx+1:], true, nil
	}

	return "", "", false, nil
}

func findMatchingParen(s string, pos int) (int, error) {
	if pos >= len(s) || s[pos] != '(' {
		return -1, fmt.Errorf("expected '(' at position %d", pos)
	}

	depth := 1
	inSingleQuote := false
	inDoubleQuote := false

	for i := pos + 1; i < len(s); i++ {
		ch := s[i]
		if ch == '\'' && !inDoubleQuote {
			if inSingleQuote {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			} else {
				inSingleQuote = true
			}
			continue
		}
		if ch == '"' && !inSingleQuote {
			if inDoubleQuote {
				if i+1 < len(s) && s[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			} else {
				inDoubleQuote = true
			}
			continue
		}
		if inSingleQuote || inDoubleQuote {
			continue
		}
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}

	return -1, fmt.Errorf("unmatched parenthesis")
}

func extractFirstGraphRefArg(args string) (string, error) {
	args = strings.TrimSpace(args)
	if args == "" {
		return "", fmt.Errorf("empty graph reference argument")
	}

	if args[0] == '\'' || args[0] == '"' {
		quote := args[0]
		for i := 1; i < len(args); i++ {
			if args[i] == quote {
				if i+1 < len(args) && args[i+1] == quote {
					i++
					continue
				}
				return args[1:i], nil
			}
		}
		return "", fmt.Errorf("unterminated graph reference string")
	}

	if args[0] == '`' {
		id, _, err := extractIdentifier(args)
		if err != nil {
			return "", err
		}
		return id, nil
	}

	id, _, err := extractIdentifier(args)
	if err != nil {
		return "", err
	}
	return id, nil
}

func splitTopLevelUnion(query string) ([]string, []bool, bool, error) {
	parts := make([]string, 0, 2)
	ops := make([]bool, 0, 1)
	start := 0
	inSingleQuote := false
	inDoubleQuote := false
	braceDepth := 0
	parenDepth := 0

	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '\'' && !inDoubleQuote {
			if inSingleQuote {
				if i+1 < len(query) && query[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			} else {
				inSingleQuote = true
			}
			continue
		}
		if ch == '"' && !inSingleQuote {
			if inDoubleQuote {
				if i+1 < len(query) && query[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			} else {
				inDoubleQuote = true
			}
			continue
		}
		if inSingleQuote || inDoubleQuote {
			continue
		}

		switch ch {
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		}
		if braceDepth != 0 || parenDepth != 0 {
			continue
		}

		if !keywordAt(query, i, "UNION") {
			continue
		}

		part := strings.TrimSpace(query[start:i])
		if part == "" {
			return nil, nil, false, fmt.Errorf("invalid UNION: empty branch")
		}
		parts = append(parts, part)

		i += len("UNION")
		for i < len(query) && isWhitespace(query[i]) {
			i++
		}
		distinct := true
		if keywordAt(query, i, "ALL") {
			distinct = false
			i += len("ALL")
		}
		ops = append(ops, distinct)
		start = i
		i--
	}

	if len(parts) == 0 {
		return nil, nil, false, nil
	}
	last := strings.TrimSpace(query[start:])
	if last == "" {
		return nil, nil, false, fmt.Errorf("invalid UNION: empty trailing branch")
	}
	parts = append(parts, last)
	return parts, ops, true, nil
}

func keywordAt(s string, idx int, keyword string) bool {
	if idx < 0 || idx+len(keyword) > len(s) {
		return false
	}
	if !strings.EqualFold(s[idx:idx+len(keyword)], keyword) {
		return false
	}
	if idx > 0 && isIdentChar(s[idx-1]) {
		return false
	}
	after := idx + len(keyword)
	if after < len(s) && isIdentChar(s[after]) {
		return false
	}
	return true
}

// NewFabricPlanner creates a planner backed by the given catalog.
func NewFabricPlanner(catalog *Catalog) *FabricPlanner {
	return &FabricPlanner{catalog: catalog}
}

// Plan decomposes a query into a Fragment tree.
//
// Parameters:
//   - query: the full Cypher query string
//   - sessionDB: the default database for the session (used when no USE clause is present)
//
// Returns a Fragment tree ready for execution by FabricExecutor.
func (p *FabricPlanner) Plan(query string, sessionDB string) (Fragment, error) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil, fmt.Errorf("empty query")
	}

	// Handle top-level UNION / UNION ALL by planning each branch independently.
	parts, ops, hasUnion, err := splitTopLevelUnion(trimmed)
	if err != nil {
		return nil, err
	}
	if hasUnion {
		lhs, err := p.planSingleQuery(parts[0], sessionDB)
		if err != nil {
			return nil, err
		}
		root := lhs
		for i := 1; i < len(parts); i++ {
			rhs, err := p.planSingleQuery(parts[i], sessionDB)
			if err != nil {
				return nil, err
			}
			root = &FragmentUnion{
				Init:     &FragmentInit{Columns: nil},
				LHS:      root,
				RHS:      rhs,
				Distinct: ops[i-1],
				Columns:  nil,
			}
		}
		return root, nil
	}

	return p.planSingleQuery(trimmed, sessionDB)
}

func (p *FabricPlanner) planSingleQuery(trimmed string, sessionDB string) (Fragment, error) {

	// Extract leading USE clause if present.
	topDB, remaining, hasTopUse, err := parseLeadingUse(trimmed)
	if err != nil {
		return nil, err
	}
	if !hasTopUse {
		topDB = sessionDB
		remaining = trimmed
	} else {
		if err := p.validateUseTarget(sessionDB, topDB); err != nil {
			return nil, err
		}
	}

	// Check whether the remaining query contains top-level CALL {} subqueries.
	callBlocks, err := extractTopLevelCallBlocks(remaining)
	if err != nil {
		return nil, err
	}
	fabricBlocks := make([]callSubqueryBlock, 0, len(callBlocks))
	for _, block := range callBlocks {
		isFabricBlock, err := callBlockContainsFabricUse(block.body)
		if err != nil {
			return nil, err
		}
		if isFabricBlock {
			fabricBlocks = append(fabricBlocks, block)
		}
	}

	if len(fabricBlocks) == 0 {
		// Simple case: single-graph query, no CALL {} blocks at this scope.
		isWrite := queryIsWrite(remaining)
		return &FragmentExec{
			Input:     &FragmentInit{Columns: nil},
			Query:     remaining,
			GraphName: topDB,
			Columns:   nil, // columns determined at execution time
			IsWrite:   isWrite,
		}, nil
	}

	// Multi-graph case: decompose into Apply chain.
	// The top-level USE sets the default graph; each CALL { USE ... } block
	// targets a different constituent.
	return p.planMultiGraph(topDB, remaining, fabricBlocks)
}

// planMultiGraph builds a Fragment tree for queries with top-level CALL {} subqueries.
// Each CALL block is planned recursively so nested USE variants are decomposed correctly.
func (p *FabricPlanner) planMultiGraph(topDB string, fullQuery string, blocks []callSubqueryBlock) (Fragment, error) {
	init := &FragmentInit{Columns: nil}
	var currentInput Fragment = init
	lastPos := 0

	for _, block := range blocks {
		// Preserve outer query segments before each CALL block.
		prefix := strings.TrimSpace(fullQuery[lastPos:block.startPos])
		if prefix != "" {
			prefixExec := &FragmentExec{
				Input:     &FragmentInit{Columns: nil},
				Query:     prefix,
				GraphName: topDB,
				Columns:   nil,
				IsWrite:   queryIsWrite(prefix),
			}
			currentInput = &FragmentApply{Input: currentInput, Inner: prefixExec, Columns: nil}
		}

		subDB, subBody, hasUse, err := parseLeadingUse(block.body)
		if err != nil {
			return nil, fmt.Errorf("invalid USE in CALL subquery: %w", err)
		}

		var (
			subqueryFragment Fragment
			importCols       []string
		)
		if hasUse {
			if err := p.validateUseTarget(topDB, subDB); err != nil {
				return nil, err
			}
			subqueryFragment, err = p.planSingleQuery(subBody, subDB)
			if err != nil {
				return nil, err
			}
			importCols = extractWithImports(subBody)
		} else {
			subqueryFragment, err = p.planSingleQuery(block.body, topDB)
			if err != nil {
				return nil, err
			}
			importCols = extractWithImports(block.body)
		}
		subqueryFragment = bindLeadingImportColumns(subqueryFragment, importCols)

		currentInput = &FragmentApply{
			Input:   currentInput,
			Inner:   subqueryFragment,
			Columns: nil, // determined at execution time
		}
		lastPos = block.endPos
	}

	// Preserve trailing outer query clauses after the final CALL block.
	trailingQuery := strings.TrimSpace(fullQuery[lastPos:])
	if strings.TrimSpace(trailingQuery) != "" {
		trailingExec := &FragmentExec{
			Input:     &FragmentInit{Columns: nil},
			Query:     trailingQuery,
			GraphName: topDB,
			Columns:   nil,
			IsWrite:   queryIsWrite(trailingQuery),
		}
		currentInput = &FragmentApply{
			Input:   currentInput,
			Inner:   trailingExec,
			Columns: nil,
		}
	}

	return currentInput, nil
}

// callSubqueryBlock represents a top-level CALL { ... } block for a single scope.
type callSubqueryBlock struct {
	// body is the Cypher body inside the CALL block (without the CALL { } wrapper).
	body string

	// startPos is the byte offset in the original query where CALL { starts.
	startPos int

	// endPos is the byte offset after the closing }.
	endPos int
}

func (p *FabricPlanner) validateUseTarget(sessionDB string, targetDB string) error {
	target := strings.TrimSpace(targetDB)
	if target == "" {
		return fmt.Errorf("USE clause requires a database name")
	}
	if p.catalog != nil {
		if _, err := p.catalog.Resolve(target); err != nil {
			return fmt.Errorf("invalid USE target '%s': %w", target, err)
		}
	}

	scopeRoot := compositeScopeRoot(sessionDB)
	targetRoot := compositeScopeRoot(target)
	if strings.Contains(target, ".") && p.inCompositeScope(sessionDB) && scopeRoot != "" && !strings.EqualFold(scopeRoot, targetRoot) {
		return fmt.Errorf("invalid USE target '%s': target is out of scope for composite '%s'", target, scopeRoot)
	}
	return nil
}

func (p *FabricPlanner) inCompositeScope(sessionDB string) bool {
	db := strings.TrimSpace(sessionDB)
	if db == "" {
		return false
	}
	if strings.Contains(db, ".") {
		return true
	}
	if p.catalog == nil {
		return false
	}
	prefix := strings.ToLower(db) + "."
	for _, graph := range p.catalog.ListGraphs() {
		if strings.HasPrefix(strings.ToLower(graph), prefix) {
			return true
		}
	}
	return false
}

func compositeScopeRoot(graph string) string {
	graph = strings.TrimSpace(graph)
	if graph == "" {
		return ""
	}
	if idx := strings.IndexByte(graph, '.'); idx >= 0 {
		return graph[:idx]
	}
	return graph
}

func bindLeadingImportColumns(fragment Fragment, importCols []string) Fragment {
	if len(importCols) == 0 || fragment == nil {
		return fragment
	}

	switch f := fragment.(type) {
	case *FragmentExec:
		copied := *f
		copied.Input = &FragmentInit{Columns: importCols, ImportColumns: importCols}
		return &copied
	case *FragmentApply:
		copied := *f
		copied.Input = bindLeadingImportColumns(copied.Input, importCols)
		return &copied
	case *FragmentUnion:
		copied := *f
		copied.LHS = bindLeadingImportColumns(copied.LHS, importCols)
		copied.RHS = bindLeadingImportColumns(copied.RHS, importCols)
		return &copied
	default:
		return fragment
	}
}

// parseLeadingUse extracts a leading USE clause from a query.
// Returns (database, remaining, hasUse, error).
func parseLeadingUse(query string) (string, string, bool, error) {
	trimmed := strings.TrimSpace(query)
	if !startsWithFold(trimmed, "USE") {
		return "", query, false, nil
	}

	// Must be followed by whitespace (not "USER" or "USING").
	if len(trimmed) > 3 && !isWhitespace(trimmed[3]) {
		return "", query, false, nil
	}

	rest := strings.TrimSpace(trimmed[3:])
	if rest == "" {
		return "", "", true, fmt.Errorf("USE clause requires a database name")
	}

	if graphRef, rem, ok, err := extractGraphReference(rest); ok {
		if err != nil {
			return "", "", true, fmt.Errorf("invalid USE clause: %w", err)
		}
		return graphRef, strings.TrimSpace(rem), true, nil
	}

	// Extract the database name (simple identifier or backtick-quoted).
	dbName, remaining, err := extractIdentifier(rest)
	if err != nil {
		return "", "", true, fmt.Errorf("invalid USE clause: %w", err)
	}

	return dbName, strings.TrimSpace(remaining), true, nil
}

// extractIdentifier extracts a simple or backtick-quoted identifier from the start of s.
// Returns (identifier, remaining, error).
func extractIdentifier(s string) (string, string, error) {
	if s == "" {
		return "", "", fmt.Errorf("expected identifier")
	}

	if s[0] == '`' {
		// Backtick-quoted identifier.
		var b strings.Builder
		for i := 1; i < len(s); i++ {
			if s[i] == '`' {
				if i+1 < len(s) && s[i+1] == '`' {
					b.WriteByte('`')
					i++
					continue
				}
				return b.String(), s[i+1:], nil
			}
			b.WriteByte(s[i])
		}
		return "", "", fmt.Errorf("unterminated backtick identifier")
	}

	// Simple identifier: letters, digits, underscores, dots (for composite.alias).
	end := 0
	for end < len(s) && (isIdentChar(s[end]) || s[end] == '.') {
		end++
	}
	if end == 0 {
		return "", "", fmt.Errorf("expected identifier, got '%c'", s[0])
	}

	return s[:end], s[end:], nil
}

// extractTopLevelCallBlocks finds CALL { ... } blocks in the current query scope.
func extractTopLevelCallBlocks(query string) ([]callSubqueryBlock, error) {
	var blocks []callSubqueryBlock
	inSingleQuote := false
	inDoubleQuote := false
	braceDepth := 0
	parenDepth := 0

	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '\'' && !inDoubleQuote {
			if inSingleQuote {
				if i+1 < len(query) && query[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			} else {
				inSingleQuote = true
			}
			continue
		}
		if ch == '"' && !inSingleQuote {
			if inDoubleQuote {
				if i+1 < len(query) && query[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			} else {
				inDoubleQuote = true
			}
			continue
		}
		if inSingleQuote || inDoubleQuote {
			continue
		}

		if ch == '/' && i+1 < len(query) && query[i+1] == '/' {
			for i < len(query) && query[i] != '\n' {
				i++
			}
			continue
		}
		if ch == '/' && i+1 < len(query) && query[i+1] == '*' {
			i += 2
			for i+1 < len(query) {
				if query[i] == '*' && query[i+1] == '/' {
					i++
					break
				}
				i++
			}
			continue
		}

		switch ch {
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		}
		if braceDepth != 0 || parenDepth != 0 {
			continue
		}
		if !keywordAt(query, i, "CALL") {
			continue
		}

		j := i + len("CALL")
		for j < len(query) && isWhitespace(query[j]) {
			j++
		}
		if j >= len(query) || query[j] != '{' {
			continue
		}

		closePos, err := findMatchingBrace(query, j)
		if err != nil {
			return nil, fmt.Errorf("unmatched brace in CALL subquery at position %d: %w", i, err)
		}
		body := strings.TrimSpace(query[j+1 : closePos])
		blocks = append(blocks, callSubqueryBlock{
			body:     body,
			startPos: i,
			endPos:   closePos + 1,
		})
		i = closePos
	}

	return blocks, nil
}

func callBlockContainsFabricUse(body string) (bool, error) {
	_, _, hasUse, err := parseLeadingUse(body)
	if err != nil {
		return false, fmt.Errorf("invalid USE in CALL subquery: %w", err)
	}
	if hasUse {
		return true, nil
	}

	nested, err := extractTopLevelCallBlocks(body)
	if err != nil {
		return false, err
	}
	for _, block := range nested {
		found, err := callBlockContainsFabricUse(block.body)
		if err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
	}
	return false, nil
}

// findMatchingBrace finds the position of the closing } matching the { at pos.
// Handles nested braces, string literals, and comments.
func findMatchingBrace(s string, pos int) (int, error) {
	if pos >= len(s) || s[pos] != '{' {
		return -1, fmt.Errorf("expected '{' at position %d", pos)
	}

	depth := 1
	inSingleQuote := false
	inDoubleQuote := false

	for i := pos + 1; i < len(s); i++ {
		ch := s[i]

		// Handle string literals (skip brace counting inside strings).
		if ch == '\'' && !inDoubleQuote {
			if inSingleQuote {
				// Check for escaped quote.
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inSingleQuote = false
			} else {
				inSingleQuote = true
			}
			continue
		}
		if ch == '"' && !inSingleQuote {
			if inDoubleQuote {
				if i+1 < len(s) && s[i+1] == '"' {
					i++
					continue
				}
				inDoubleQuote = false
			} else {
				inDoubleQuote = true
			}
			continue
		}

		if inSingleQuote || inDoubleQuote {
			continue
		}

		// Handle line comments (// ...).
		if ch == '/' && i+1 < len(s) && s[i+1] == '/' {
			// Skip to end of line.
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}

		// Handle block comments (/* ... */).
		if ch == '/' && i+1 < len(s) && s[i+1] == '*' {
			i += 2
			for i+1 < len(s) {
				if s[i] == '*' && s[i+1] == '/' {
					i++
					break
				}
				i++
			}
			continue
		}

		if ch == '{' {
			depth++
		} else if ch == '}' {
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}

	return -1, fmt.Errorf("unmatched brace (depth=%d remaining)", depth)
}

// extractWithImports parses a leading WITH clause to extract imported variable names.
// e.g. "WITH translationId MATCH ..." returns ["translationId"]
func extractWithImports(body string) []string {
	trimmed := strings.TrimSpace(body)
	if !startsWithFold(trimmed, "WITH") {
		return nil
	}

	// Must be followed by whitespace.
	if len(trimmed) > 4 && !isWhitespace(trimmed[4]) {
		return nil
	}

	rest := strings.TrimSpace(trimmed[4:])

	// Extract identifiers until we hit a keyword (MATCH, RETURN, CREATE, etc.).
	var imports []string
	parts := strings.FieldsFunc(rest, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})

	keywords := map[string]bool{
		"MATCH": true, "OPTIONAL": true, "CREATE": true, "MERGE": true,
		"DELETE": true, "DETACH": true, "SET": true, "REMOVE": true,
		"RETURN": true, "WITH": true, "WHERE": true, "ORDER": true,
		"SKIP": true, "LIMIT": true, "UNWIND": true, "CALL": true,
		"FOREACH": true, "LOAD": true, "USE": true,
	}

	for _, part := range parts {
		cleaned := strings.TrimSpace(part)
		if cleaned == "" {
			continue
		}
		if keywords[strings.ToUpper(cleaned)] {
			break
		}
		// Strip AS alias if present.
		if strings.EqualFold(cleaned, "AS") {
			continue
		}
		imports = append(imports, cleaned)
	}

	return imports
}

// queryIsWrite performs a simple heuristic check for write operations.
func queryIsWrite(query string) bool {
	upper := strings.ToUpper(query)
	writeKeywords := []string{"CREATE", "MERGE", "DELETE", "DETACH DELETE", "SET ", "REMOVE "}
	for _, kw := range writeKeywords {
		if strings.Contains(upper, kw) {
			return true
		}
	}
	return false
}

// startsWithFold checks if s starts with prefix (case-insensitive).
func startsWithFold(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return strings.EqualFold(s[:len(prefix)], prefix)
}

func isWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func isIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') || b == '_'
}
