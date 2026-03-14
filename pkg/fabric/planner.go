package fabric

import (
	"fmt"
	"regexp"
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

	// Extract leading USE clause if present.
	topDB, remaining, hasTopUse, err := parseLeadingUse(trimmed)
	if err != nil {
		return nil, err
	}
	if !hasTopUse {
		topDB = sessionDB
		remaining = trimmed
	}

	// Check whether the remaining query contains CALL {} subqueries with USE clauses.
	callBlocks, err := extractCallUseBlocks(remaining)
	if err != nil {
		return nil, err
	}

	if len(callBlocks) == 0 {
		// Simple case: single-graph query, no CALL { USE ... } blocks.
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
	return p.planMultiGraph(topDB, remaining, callBlocks)
}

// planMultiGraph builds a Fragment tree for queries with CALL { USE ... } subqueries.
//
// The pattern is:
//
//	USE composite
//	CALL { USE composite.alias1 MATCH ... RETURN ... }
//	CALL { USE composite.alias2 WITH imported MATCH ... RETURN ... }
//	RETURN ...
//
// This produces a chain of FragmentApply nodes:
//
//	Apply(outer=Exec(alias1), inner=Apply(outer=Exec(alias2), inner=...))
func (p *FabricPlanner) planMultiGraph(topDB string, fullQuery string, blocks []callUseBlock) (Fragment, error) {
	// Build the chain from inside out.
	// Start with the top-level init.
	init := &FragmentInit{Columns: nil}

	var currentInput Fragment = init

	for _, block := range blocks {
		// Each CALL { USE graph ... } block becomes a FragmentExec
		// wrapped in a FragmentApply so it receives input rows.
		isWrite := queryIsWrite(block.body)

		execFragment := &FragmentExec{
			Input:     &FragmentInit{Columns: block.importColumns, ImportColumns: block.importColumns},
			Query:     block.body,
			GraphName: block.graphName,
			Columns:   nil, // determined at execution time
			IsWrite:   isWrite,
		}

		currentInput = &FragmentApply{
			Input:   currentInput,
			Inner:   execFragment,
			Columns: nil, // determined at execution time
		}
	}

	// If there's a trailing RETURN clause (or other clauses after all CALL blocks),
	// add it as a final exec on the top-level database.
	trailingQuery := extractTrailingClauses(fullQuery, blocks)
	if strings.TrimSpace(trailingQuery) != "" {
		trailingExec := &FragmentExec{
			Input:     &FragmentInit{Columns: nil},
			Query:     trailingQuery,
			GraphName: topDB,
			Columns:   nil,
			IsWrite:   false,
		}
		currentInput = &FragmentApply{
			Input:   currentInput,
			Inner:   trailingExec,
			Columns: nil,
		}
	}

	return currentInput, nil
}

// callUseBlock represents a parsed CALL { USE graph.name ... } block.
type callUseBlock struct {
	// graphName is the USE target inside the CALL block.
	graphName string

	// body is the Cypher body inside the CALL block (after USE, without the CALL { } wrapper).
	body string

	// importColumns are the WITH-imported variables at the start of the body.
	importColumns []string

	// startPos is the byte offset in the original query where CALL { starts.
	startPos int

	// endPos is the byte offset after the closing }.
	endPos int
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

// callUseRe matches CALL followed by optional whitespace and {
var callUseRe = regexp.MustCompile(`(?i)\bCALL\s*\{`)

// extractCallUseBlocks finds all CALL { USE ... } subquery blocks in a query.
func extractCallUseBlocks(query string) ([]callUseBlock, error) {
	matches := callUseRe.FindAllStringIndex(query, -1)
	if len(matches) == 0 {
		return nil, nil
	}

	var blocks []callUseBlock
	for _, match := range matches {
		startPos := match[0]
		braceStart := match[1] - 1 // position of the {

		// Find the matching closing brace.
		closePos, err := findMatchingBrace(query, braceStart)
		if err != nil {
			return nil, fmt.Errorf("unmatched brace in CALL subquery at position %d: %w", startPos, err)
		}

		// Extract body between { and }
		body := strings.TrimSpace(query[braceStart+1 : closePos])

		// Check if the body starts with USE.
		subDB, subBody, hasUse, err := parseLeadingUse(body)
		if err != nil {
			return nil, fmt.Errorf("invalid USE in CALL subquery: %w", err)
		}
		if !hasUse {
			// CALL {} without USE — not a fabric subquery, skip it.
			continue
		}

		// Extract WITH-imported columns from the subquery body.
		importCols := extractWithImports(subBody)

		blocks = append(blocks, callUseBlock{
			graphName:     subDB,
			body:          subBody,
			importColumns: importCols,
			startPos:      startPos,
			endPos:        closePos + 1,
		})
	}

	return blocks, nil
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

// extractTrailingClauses extracts any query text that follows the last CALL { USE ... } block.
func extractTrailingClauses(fullQuery string, blocks []callUseBlock) string {
	if len(blocks) == 0 {
		return ""
	}

	lastBlock := blocks[len(blocks)-1]
	if lastBlock.endPos >= len(fullQuery) {
		return ""
	}

	return strings.TrimSpace(fullQuery[lastBlock.endPos:])
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
