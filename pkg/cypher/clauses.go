// Cypher clause implementations for NornicDB.
// This file contains implementations for WITH, UNWIND, UNION, OPTIONAL MATCH,
// FOREACH, and LOAD CSV clauses.

package cypher

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// findStandaloneWithIndex finds the index of a standalone "WITH" keyword
// that is NOT part of "STARTS WITH" or "ENDS WITH".
// Returns -1 if not found.
func findStandaloneWithIndex(s string) int {
	opts := defaultKeywordScanOpts()

	searchFrom := 0
	for {
		absolutePos := keywordIndexFrom(s, "WITH", searchFrom, opts)
		if absolutePos == -1 {
			return -1
		}
		if !prevWordEqualsIgnoreCase(s, absolutePos, "STARTS") && !prevWordEqualsIgnoreCase(s, absolutePos, "ENDS") {
			return absolutePos
		}
		searchFrom = absolutePos + 4
	}
}

func prevWordEqualsIgnoreCase(s string, pos int, word string) bool {
	if pos <= 0 {
		return false
	}
	i := pos - 1
	for i >= 0 && isASCIISpace(s[i]) {
		i--
	}
	if i < 0 {
		return false
	}
	end := i + 1
	for i >= 0 && isIdentByte(s[i]) {
		i--
	}
	start := i + 1
	if end-start != len(word) {
		return false
	}
	for j := 0; j < len(word); j++ {
		if asciiUpper(s[start+j]) != asciiUpper(word[j]) {
			return false
		}
	}
	return true
}

// findKeywordNotInBrackets finds the index of a keyword that is NOT inside brackets [] or parentheses ()
// This is used to avoid matching keywords inside list comprehensions like [x IN list WHERE x > 2]
// The keyword should be in the format " KEYWORD " with leading/trailing spaces.
// This function normalizes whitespace (tabs, newlines) to match.
func findKeywordNotInBrackets(s string, keyword string) int {
	opts := defaultKeywordScanOpts()
	opts.SkipBraces = false
	opts.Boundary = keywordBoundaryWhitespace

	keywordCore := strings.TrimSpace(keyword)
	if keywordCore == "" {
		return -1
	}
	return keywordIndexFrom(s, keywordCore, 0, opts)
}

// isWhitespace returns true if the rune is a whitespace character
func isWhitespace(ch byte) bool {
	return isASCIISpace(ch)
}

// ========================================
// WITH Clause
// ========================================

// executeWith handles WITH clause - intermediate result projection
func (e *StorageExecutor) executeWith(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	withIdx := findKeywordIndex(cypher, "WITH")
	if withIdx == -1 {
		return nil, fmt.Errorf("WITH clause not found in query: %q", truncateQuery(cypher, 80))
	}

	remainderStart := withIdx + 4
	// Skip all whitespace (spaces, tabs, newlines)
	for remainderStart < len(cypher) && isWhitespace(cypher[remainderStart]) {
		remainderStart++
	}

	// Use findKeywordIndex which handles whitespace/newlines properly
	nextClauseKeywords := []string{
		"MATCH", "OPTIONAL MATCH", "WHERE", "RETURN",
		"CREATE", "MERGE", "DELETE", "DETACH DELETE", "SET", "REMOVE",
		"UNWIND", "CALL", "FOREACH",
		"ORDER", "SKIP", "LIMIT",
	}
	nextClauseIdx := len(cypher)
	for _, keyword := range nextClauseKeywords {
		idx := findKeywordIndex(cypher[remainderStart:], keyword)
		if idx >= 0 && remainderStart+idx < nextClauseIdx {
			nextClauseIdx = remainderStart + idx
		}
	}

	withExpr := strings.TrimSpace(cypher[remainderStart:nextClauseIdx])
	boundVars := make(map[string]interface{})

	items := e.splitWithItems(withExpr)
	columns := make([]string, 0)
	values := make([]interface{}, 0)

	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		upperItem := strings.ToUpper(item)
		asIdx := strings.Index(upperItem, " AS ")
		var alias string
		var expr string
		if asIdx > 0 {
			expr = strings.TrimSpace(item[:asIdx])
			alias = strings.TrimSpace(item[asIdx+4:])
		} else {
			expr = item
			alias = item
		}

		trimmedExpr := strings.TrimSpace(expr)
		var val interface{}
		if strings.HasPrefix(trimmedExpr, "{") && strings.HasSuffix(trimmedExpr, "}") {
			val = e.evaluateMapLiteral(trimmedExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
		} else {
			val = e.evaluateExpressionWithContext(trimmedExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
		}
		boundVars[alias] = val
		columns = append(columns, alias)
		values = append(values, val)
	}

	if nextClauseIdx < len(cypher) {
		remainder := strings.TrimSpace(cypher[nextClauseIdx:])

		// If it's a RETURN clause, evaluate it with the bound variables
		if strings.HasPrefix(strings.ToUpper(remainder), "RETURN") {
			returnExpr := strings.TrimSpace(remainder[6:])

			// Parse return items
			returnItems := e.parseReturnItems(returnExpr)
			returnColumns := make([]string, len(returnItems))
			returnValues := make([]interface{}, len(returnItems))

			for i, item := range returnItems {
				if item.alias != "" {
					returnColumns[i] = item.alias
				} else {
					returnColumns[i] = item.expr
				}

				// First check if it's a direct reference to a bound variable
				if val, ok := boundVars[item.expr]; ok {
					returnValues[i] = val
				} else {
					// Substitute bound variables in the expression
					expr := item.expr
					for varName, varVal := range boundVars {
						// Replace the variable name in the expression
						// Handle list comprehension: [x IN varName WHERE ...] -> [x IN [1,2,3] WHERE ...]
						if strings.Contains(expr, varName) {
							// Convert value to string representation
							var replacement string
							switch v := varVal.(type) {
							case []interface{}:
								parts := make([]string, len(v))
								for j, elem := range v {
									switch e := elem.(type) {
									case string:
										parts[j] = fmt.Sprintf("'%s'", e)
									default:
										parts[j] = fmt.Sprintf("%v", e)
									}
								}
								replacement = "[" + strings.Join(parts, ", ") + "]"
							case string:
								replacement = fmt.Sprintf("'%s'", v)
							default:
								replacement = fmt.Sprintf("%v", v)
							}
							expr = strings.ReplaceAll(expr, varName, replacement)
						}
					}
					returnValues[i] = e.evaluateExpressionWithContext(expr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
				}
			}

			return &ExecuteResult{
				Columns: returnColumns,
				Rows:    [][]interface{}{returnValues},
			}, nil
		}

		// Substitute bound variables into remainder before delegating
		// e.g., WITH [[1,2],[3,4]] AS matrix UNWIND matrix ... -> UNWIND [[1,2],[3,4]] ...
		substitutedRemainder := remainder
		for varName, varVal := range boundVars {
			switch v := varVal.(type) {
			case []interface{}:
				parts := make([]string, len(v))
				for j, elem := range v {
					switch e := elem.(type) {
					case []interface{}:
						innerParts := make([]string, len(e))
						for k, innerElem := range e {
							switch ie := innerElem.(type) {
							case string:
								innerParts[k] = fmt.Sprintf("'%s'", ie)
							default:
								innerParts[k] = fmt.Sprintf("%v", ie)
							}
						}
						parts[j] = "[" + strings.Join(innerParts, ", ") + "]"
					case string:
						parts[j] = fmt.Sprintf("'%s'", e)
					default:
						parts[j] = fmt.Sprintf("%v", e)
					}
				}
				replacement := "[" + strings.Join(parts, ", ") + "]"
				substitutedRemainder = replaceStandaloneCypherIdentifier(substitutedRemainder, varName, replacement)
			case string:
				substitutedRemainder = replaceStandaloneCypherIdentifier(substitutedRemainder, varName, fmt.Sprintf("'%s'", v))
			case map[string]interface{}:
				substitutedRemainder = replaceStandaloneCypherIdentifier(substitutedRemainder, varName, mapToCypherLiteral(v))
			case map[interface{}]interface{}:
				substitutedRemainder = replaceStandaloneCypherIdentifier(substitutedRemainder, varName, mapToCypherLiteral(normalizeInterfaceMap(v)))
			case nil:
				substitutedRemainder = replaceStandaloneCypherIdentifier(substitutedRemainder, varName, "null")
			default:
				substitutedRemainder = replaceStandaloneCypherIdentifier(substitutedRemainder, varName, fmt.Sprintf("%v", v))
			}
		}
		return e.executeInternal(ctx, substitutedRemainder, nil)
	}

	return &ExecuteResult{
		Columns: columns,
		Rows:    [][]interface{}{values},
	}, nil
}

func normalizeInterfaceMap(input map[interface{}]interface{}) map[string]interface{} {
	output := make(map[string]interface{}, len(input))
	for k, v := range input {
		key := fmt.Sprintf("%v", k)
		output[key] = v
	}
	return output
}

func mapToCypherLiteral(m map[string]interface{}) string {
	var parts []string
	for k, v := range m {
		parts = append(parts, fmt.Sprintf("%s: %s", k, valueToCypherLiteral(v)))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func valueToCypherLiteral(v interface{}) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprintf("%v", val)
	case map[string]interface{}:
		return mapToCypherLiteral(val)
	case map[interface{}]interface{}:
		return mapToCypherLiteral(normalizeInterfaceMap(val))
	case []interface{}:
		items := make([]string, len(val))
		for i, item := range val {
			items[i] = valueToCypherLiteral(item)
		}
		return "[" + strings.Join(items, ", ") + "]"
	case nil:
		return "null"
	default:
		return fmt.Sprintf("%v", val)
	}
}

// splitWithItems splits WITH expressions respecting nested brackets and quotes
func (e *StorageExecutor) splitWithItems(expr string) []string {
	var items []string
	var current strings.Builder
	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for _, c := range expr {
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
			current.WriteRune(c)
		case c == '(' || c == '[' || c == '{':
			if !inQuote {
				depth++
			}
			current.WriteRune(c)
		case c == ')' || c == ']' || c == '}':
			if !inQuote {
				depth--
			}
			current.WriteRune(c)
		case c == ',' && depth == 0 && !inQuote:
			items = append(items, current.String())
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}
	if current.Len() > 0 {
		items = append(items, current.String())
	}
	return items
}

// ========================================
// UNWIND Clause
// ========================================

// executeUnwind handles UNWIND clause - list expansion
func (e *StorageExecutor) executeUnwind(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	// Check for double UNWIND - handle by recursively processing
	firstUnwind := findKeywordIndex(cypher, "UNWIND")
	if firstUnwind >= 0 {
		// Find the AS clause for the first UNWIND
		afterFirstUnwind := upper[firstUnwind+6:]
		firstAsIdx := strings.Index(afterFirstUnwind, " AS ")
		if firstAsIdx >= 0 {
			// Find where the variable ends (next space or UNWIND)
			varStart := firstAsIdx + 4
			restAfterAs := strings.TrimSpace(afterFirstUnwind[varStart:])
			varEndIdx := strings.IndexAny(restAfterAs, " \t\n")
			if varEndIdx > 0 {
				restAfterVar := strings.TrimSpace(restAfterAs[varEndIdx:])
				// Check if there's another UNWIND
				if strings.HasPrefix(strings.ToUpper(restAfterVar), "UNWIND") {
					// Handle double UNWIND by unwinding the first list and processing second UNWIND for each
					return e.executeDoubleUnwind(ctx, cypher)
				}
			}
		}
	}

	// Check for unsupported map keys() function
	if strings.Contains(upper, "KEYS(") && strings.Contains(upper, "UNWIND") {
		return nil, fmt.Errorf("keys() function with UNWIND is not supported in this context")
	}

	unwindIdx := findKeywordIndex(cypher, "UNWIND")
	if unwindIdx == -1 {
		return nil, fmt.Errorf("UNWIND clause not found in query: %q", truncateQuery(cypher, 80))
	}

	afterUnwind := cypher[unwindIdx+6:]
	asRelIdx := findKeywordNotInBrackets(afterUnwind, " AS ")
	if asRelIdx == -1 {
		return nil, fmt.Errorf("UNWIND requires AS clause (e.g., UNWIND [1,2,3] AS x)")
	}

	asIdx := unwindIdx + 6 + asRelIdx
	listExpr := strings.TrimSpace(cypher[unwindIdx+6 : asIdx])

	remainderStart := asIdx + len("AS")
	for remainderStart < len(cypher) && isASCIISpace(cypher[remainderStart]) {
		remainderStart++
	}
	remainder := strings.TrimSpace(cypher[remainderStart:])
	spaceIdx := strings.IndexAny(remainder, " \t\r\n")
	var variable string
	var restQuery string
	if spaceIdx > 0 {
		variable = strings.TrimSpace(remainder[:spaceIdx])
		restQuery = strings.TrimSpace(remainder[spaceIdx:])
	} else {
		variable = strings.TrimSpace(remainder)
		restQuery = ""
	}

	params := getParamsFromContext(ctx)
	var list interface{}
	if strings.HasPrefix(strings.TrimSpace(listExpr), "$") {
		paramName := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(listExpr), "$"))
		if paramName == "" {
			return nil, fmt.Errorf("UNWIND requires a valid parameter name after $")
		}
		if params == nil {
			return nil, fmt.Errorf("UNWIND parameter $%s requires parameters to be provided", paramName)
		}
		paramValue, exists := params[paramName]
		if !exists {
			return nil, fmt.Errorf("UNWIND parameter $%s not found in provided parameters", paramName)
		}
		list = paramValue
	} else {
		listExprEval := listExpr
		if params != nil {
			listExprEval = e.substituteParams(listExprEval, params)
		}
		list = e.evaluateExpressionWithContext(listExprEval, make(map[string]*storage.Node), make(map[string]*storage.Edge))
	}

	var items []interface{}
	switch v := list.(type) {
	case nil:
		// UNWIND null produces no rows (Neo4j compatible)
		items = []interface{}{}
	case []interface{}:
		items = v
	case []string:
		items = make([]interface{}, len(v))
		for i, s := range v {
			items[i] = s
		}
	case []int64:
		items = make([]interface{}, len(v))
		for i, n := range v {
			items[i] = n
		}
	case []float64:
		items = make([]interface{}, len(v))
		for i, n := range v {
			items[i] = n
		}
	case []map[string]interface{}:
		items = make([]interface{}, len(v))
		for i := range v {
			items[i] = v[i]
		}
	default:
		rv := reflect.ValueOf(list)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			items = make([]interface{}, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				items[i] = rv.Index(i).Interface()
			}
		} else {
			// Single value gets wrapped in a list
			items = []interface{}{list}
		}
	}

	// Handle UNWIND ... CREATE/MERGE ... pattern
	if restQuery != "" {
		upperRest := strings.ToUpper(strings.TrimSpace(restQuery))
		if strings.HasPrefix(upperRest, "CREATE") || strings.HasPrefix(upperRest, "MERGE") {
			result := &ExecuteResult{
				Columns: []string{},
				Rows:    [][]interface{}{},
				Stats:   &QueryStats{},
			}

			// Split mutation and RETURN parts
			returnIdx := findKeywordIndex(restQuery, "RETURN")
			var mutationPart, returnPart string
			if returnIdx > 0 {
				mutationPart = strings.TrimSpace(restQuery[:returnIdx])
				returnPart = strings.TrimSpace(restQuery[returnIdx:])
			} else {
				mutationPart = restQuery
				returnPart = ""
			}

			// Execute mutation for each unwound item
			for _, item := range items {
				// Reconstruct full query with RETURN.
				fullQuery := mutationPart
				if returnPart != "" {
					fullQuery += " " + returnPart
				}

				// For mutation paths that depend on map sources or preserve row alias semantics
				// in WITH pipelines, execute with per-item parameters instead of literal substitution.
				// This preserves complex map/string payloads and avoids corrupting clauses like
				// `WITH cc, row, csByID` when `row` is a map.
				useParamExecution := strings.Contains(mutationPart, "+=")

				var mutationResult *ExecuteResult
				var err error
				if useParamExecution {
					callParams := make(map[string]interface{}, len(params)+1)
					for k, v := range params {
						callParams[k] = v
					}
					callParams[variable] = item
					mutationResult, err = e.Execute(ctx, fullQuery, callParams)
				} else {
					// Replace variable references ONLY in the mutation clause
					mutationQuerySubstituted := e.replaceVariableInMutationQuery(mutationPart, variable, item)
					substitutedFull := mutationQuerySubstituted
					if returnPart != "" {
						substitutedFull += " " + returnPart
					}
					trimmed := strings.TrimSpace(substitutedFull)
					// Route MERGE-heavy mutation chains through context-aware MERGE execution.
					// This avoids brittle top-level MERGE parsing for shapes like:
					// MERGE (...) MERGE (...) SET ... MERGE (...) ...
					if strings.HasPrefix(strings.ToUpper(trimmed), "MERGE ") {
						// Queries containing MATCH after MERGE (e.g. MERGE ... MATCH ... MERGE rel)
						// are better handled by the regular executor route so MATCH bindings are
						// preserved for downstream relationship merges.
						if findKeywordIndexInContext(trimmed, "MATCH") > 0 {
							mutationResult, err = e.Execute(ctx, substitutedFull, params)
						} else {
							mutationResult, err = e.executeMergeWithContext(ctx, trimmed, make(map[string]*storage.Node), make(map[string]*storage.Edge))
						}
					} else {
						mutationResult, err = e.Execute(ctx, substitutedFull, params)
					}
				}
				if err != nil {
					return nil, fmt.Errorf("UNWIND mutation failed: %w", err)
				}

				// Accumulate stats when available. Some execution paths can return a
				// nil Stats pointer even when the side-effect succeeded.
				if mutationResult != nil && mutationResult.Stats != nil {
					result.Stats.NodesCreated += mutationResult.Stats.NodesCreated
					result.Stats.RelationshipsCreated += mutationResult.Stats.RelationshipsCreated
				}

				// If there's a RETURN clause, collect the result rows
				if mutationResult != nil && returnPart != "" && len(mutationResult.Rows) > 0 {
					// First iteration: set columns
					if len(result.Columns) == 0 {
						result.Columns = mutationResult.Columns
					}
					// Append all rows from this per-item execution
					result.Rows = append(result.Rows, mutationResult.Rows...)
				}
			}

			return result, nil
		}
	}

	// Handle UNWIND ... WITH collect(DISTINCT var.prop) AS alias RETURN alias
	// for batched key extraction pipelines used by Fabric APPLY execution.
	if restQuery != "" && strings.HasPrefix(strings.ToUpper(restQuery), "WITH ") {
		if res, ok := e.executeUnwindWithCollectProjection(variable, items, restQuery); ok {
			return res, nil
		}
	}

	// Handle UNWIND ... MATCH ... RETURN ... by evaluating MATCH per unwound value
	// and combining results. This avoids silently returning only unwound values when
	// a trailing MATCH pipeline is present.
	if restQuery != "" && strings.HasPrefix(strings.ToUpper(restQuery), "MATCH ") {
		normalizedRestQuery := normalizeMultiMatchWhereClauses(restQuery)
		// Fast path: set-based rewrite for correlated MATCH pipelines that return
		// COUNT(...). This avoids per-item subquery execution in UNWIND loops.
		if canApplySetBasedUnwindRewrite(normalizedRestQuery, items) {
			if rewritten, ok := rewriteUnwindCorrelationToIn(normalizedRestQuery, variable, "__unwind_items"); ok {
				rewritten = rewriteTopLevelMultiMatchToCartesianMatch(rewritten)
				callParams := make(map[string]interface{}, len(params)+1)
				for k, v := range params {
					callParams[k] = v
				}
				callParams["__unwind_items"] = items
				rewrittenResult, err := e.Execute(ctx, rewritten, callParams)
				if err == nil {
					return rewrittenResult, nil
				}
			}
		}

		returnItems := []returnItem{}
		if retIdx := findKeywordIndex(normalizedRestQuery, "RETURN"); retIdx > 0 {
			returnClause := strings.TrimSpace(normalizedRestQuery[retIdx+6:])
			returnEnd := len(returnClause)
			for _, keyword := range []string{"ORDER", "SKIP", "LIMIT"} {
				if idx := findKeywordIndexInContext(returnClause, keyword); idx >= 0 && idx < returnEnd {
					returnEnd = idx
				}
			}
			returnClause = strings.TrimSpace(returnClause[:returnEnd])
			returnItems = e.parseReturnItems(returnClause)
		}

		aggregateCountOnly := len(returnItems) == 1 && isAggregateFuncName(returnItems[0].expr, "count")
		var aggregatedCount int64
		result := &ExecuteResult{
			Columns: []string{},
			Rows:    [][]interface{}{},
			Stats:   &QueryStats{},
		}

		for _, item := range items {
			substitutedQuery := e.replaceVariableInQuery(normalizedRestQuery, variable, item)
			subResult, err := e.Execute(ctx, substitutedQuery, params)
			if err != nil {
				return nil, fmt.Errorf("UNWIND MATCH failed: %w", err)
			}
			if subResult == nil {
				continue
			}
			if len(result.Columns) == 0 {
				result.Columns = append([]string(nil), subResult.Columns...)
			}
			if aggregateCountOnly {
				if len(subResult.Rows) == 0 || len(subResult.Rows[0]) == 0 {
					continue
				}
				switch v := subResult.Rows[0][0].(type) {
				case int64:
					aggregatedCount += v
				case int:
					aggregatedCount += int64(v)
				case float64:
					aggregatedCount += int64(v)
				}
				continue
			}
			result.Rows = append(result.Rows, subResult.Rows...)
		}

		if aggregateCountOnly {
			if len(result.Columns) == 0 {
				if returnItems[0].alias != "" {
					result.Columns = []string{returnItems[0].alias}
				} else {
					result.Columns = []string{returnItems[0].expr}
				}
			}
			result.Rows = [][]interface{}{{aggregatedCount}}
		}
		return result, nil
	}

	if restQuery != "" && strings.HasPrefix(strings.ToUpper(restQuery), "RETURN") {
		returnClause := strings.TrimSpace(restQuery[6:])
		returnItems := e.parseReturnItems(returnClause)

		// Check if any return items are aggregation functions
		hasAggregation := false
		for _, item := range returnItems {
			upperExpr := strings.ToUpper(item.expr)
			if strings.HasPrefix(upperExpr, "SUM(") || strings.HasPrefix(upperExpr, "COUNT(") ||
				strings.HasPrefix(upperExpr, "AVG(") || strings.HasPrefix(upperExpr, "MIN(") ||
				strings.HasPrefix(upperExpr, "MAX(") || strings.HasPrefix(upperExpr, "COLLECT(") {
				hasAggregation = true
				break
			}
		}

		if hasAggregation {
			// Aggregate across all unwound items
			result := &ExecuteResult{
				Columns: make([]string, len(returnItems)),
				Rows:    [][]interface{}{make([]interface{}, len(returnItems))},
			}

			for i, item := range returnItems {
				if item.alias != "" {
					result.Columns[i] = item.alias
				} else {
					result.Columns[i] = item.expr
				}

				upperExpr := strings.ToUpper(item.expr)
				switch {
				case strings.HasPrefix(upperExpr, "SUM("):
					inner := item.expr[4 : len(item.expr)-1]
					var sum float64
					for _, it := range items {
						if inner == variable {
							if n, ok := toFloat64(it); ok {
								sum += n
							}
						}
					}
					result.Rows[0][i] = int64(sum) // Return as int64 for integer sums
				case strings.HasPrefix(upperExpr, "COUNT("):
					result.Rows[0][i] = int64(len(items))
				case strings.HasPrefix(upperExpr, "AVG("):
					inner := item.expr[4 : len(item.expr)-1]
					var sum float64
					var count int
					for _, it := range items {
						if inner == variable {
							if n, ok := toFloat64(it); ok {
								sum += n
								count++
							}
						}
					}
					if count > 0 {
						result.Rows[0][i] = sum / float64(count)
					} else {
						result.Rows[0][i] = nil
					}
				case strings.HasPrefix(upperExpr, "MIN("):
					inner := item.expr[4 : len(item.expr)-1]
					var min *float64
					for _, it := range items {
						if inner == variable {
							if n, ok := toFloat64(it); ok {
								if min == nil || n < *min {
									min = &n
								}
							}
						}
					}
					if min != nil {
						result.Rows[0][i] = *min
					}
				case strings.HasPrefix(upperExpr, "MAX("):
					inner := item.expr[4 : len(item.expr)-1]
					var max *float64
					for _, it := range items {
						if inner == variable {
							if n, ok := toFloat64(it); ok {
								if max == nil || n > *max {
									max = &n
								}
							}
						}
					}
					if max != nil {
						result.Rows[0][i] = *max
					}
				case strings.HasPrefix(upperExpr, "COLLECT("):
					inner, suffix, _ := extractFuncArgsWithSuffix(item.expr, "collect")
					collected := make([]interface{}, 0, len(items))
					for _, it := range items {
						if inner == variable {
							collected = append(collected, it)
						}
					}
					// Apply suffix (e.g., [..10] for slicing) if present
					if suffix != "" {
						result.Rows[0][i] = e.applyArraySuffix(collected, suffix)
					} else {
						result.Rows[0][i] = collected
					}
				}
			}
			return result, nil
		}

		// No aggregation - return individual rows
		result := &ExecuteResult{
			Columns: make([]string, len(returnItems)),
			Rows:    make([][]interface{}, 0, len(items)),
		}
		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns[i] = item.alias
			} else {
				result.Columns[i] = item.expr
			}
		}
		for _, item := range items {
			row := make([]interface{}, len(returnItems))
			rowValues := map[string]interface{}{variable: item}
			for i, ri := range returnItems {
				row[i] = e.evaluateExpressionFromValues(ri.expr, rowValues)
			}
			result.Rows = append(result.Rows, row)
		}
		return result, nil
	}

	result := &ExecuteResult{
		Columns: []string{variable},
		Rows:    make([][]interface{}, 0, len(items)),
	}
	for _, item := range items {
		result.Rows = append(result.Rows, []interface{}{item})
	}
	return result, nil
}

func rewriteUnwindCorrelationToIn(query string, variable string, paramName string) (string, bool) {
	if strings.TrimSpace(query) == "" || strings.TrimSpace(variable) == "" || strings.TrimSpace(paramName) == "" {
		return "", false
	}
	// Preserve join correlation semantics:
	//   a.prop = unwindVar AND b.prop = unwindVar
	// => a.prop IN $items AND b.prop = a.prop
	// so we do not produce cross-key cartesian joins.
	re := regexp.MustCompile(`(?i)([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)\s*=\s*` + regexp.QuoteMeta(variable) + `\b`)
	matches := re.FindAllStringSubmatchIndex(query, -1)
	if len(matches) == 0 {
		return "", false
	}
	firstExpr := strings.TrimSpace(query[matches[0][2]:matches[0][3]])
	if firstExpr == "" {
		return "", false
	}

	var b strings.Builder
	cursor := 0
	for i, m := range matches {
		start, end := m[0], m[1]
		lhs := strings.TrimSpace(query[m[2]:m[3]])
		b.WriteString(query[cursor:start])
		if i == 0 {
			b.WriteString(lhs)
			b.WriteString(" IN $")
			b.WriteString(paramName)
		} else {
			b.WriteString(lhs)
			b.WriteString(" = ")
			b.WriteString(firstExpr)
		}
		cursor = end
	}
	b.WriteString(query[cursor:])
	return b.String(), true
}

func rewriteTopLevelMultiMatchToCartesianMatch(query string) string {
	trimmed := strings.TrimSpace(query)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "MATCH ") {
		return query
	}
	returnIdx := findKeywordIndex(trimmed, "RETURN")
	if returnIdx <= 0 {
		return query
	}
	body := strings.TrimSpace(trimmed[:returnIdx])
	tail := strings.TrimSpace(trimmed[returnIdx:])
	whereIdx := findKeywordIndex(body, "WHERE")
	if whereIdx <= 0 {
		return query
	}
	patterns := strings.TrimSpace(body[:whereIdx])
	whereClause := strings.TrimSpace(body[whereIdx+len("WHERE"):])
	if patterns == "" || whereClause == "" {
		return query
	}

	upperPatterns := strings.ToUpper(patterns)
	if strings.Count(upperPatterns, "MATCH ") != 2 {
		return query
	}
	first := strings.TrimSpace(patterns[len("MATCH "):])
	secondIdx := findKeywordIndex(first, "MATCH")
	if secondIdx <= 0 {
		return query
	}
	left := strings.TrimSpace(first[:secondIdx])
	right := strings.TrimSpace(first[secondIdx+len("MATCH"):])
	if left == "" || right == "" {
		return query
	}
	return "MATCH " + left + ", " + right + " WHERE " + whereClause + " " + tail
}

func canApplySetBasedUnwindRewrite(query string, items []interface{}) bool {
	if strings.TrimSpace(query) == "" || len(items) == 0 {
		return false
	}
	upper := strings.ToUpper(query)
	// Keep rewrite on read-only MATCH ... RETURN count(...) pipelines.
	// Mutation clauses with correlated values (SET += row.props, MERGE/CREATE/DELETE/REMOVE)
	// must execute per-row to preserve semantics.
	if findKeywordIndex(query, "CREATE") >= 0 ||
		findKeywordIndex(query, "MERGE") >= 0 ||
		findKeywordIndex(query, "SET") >= 0 ||
		findKeywordIndex(query, "DELETE") >= 0 ||
		findKeywordIndex(query, "REMOVE") >= 0 {
		return false
	}
	if !strings.Contains(upper, "RETURN") || !strings.Contains(upper, "COUNT(") {
		return false
	}
	// Rewrites should preserve semantics. We only apply when unwind items are
	// distinct comparable values so IN-list matching does not collapse duplicates.
	seen := map[interface{}]struct{}{}
	for _, it := range items {
		if it == nil {
			if _, exists := seen[nil]; exists {
				return false
			}
			seen[nil] = struct{}{}
			continue
		}
		rv := reflect.ValueOf(it)
		if !rv.IsValid() || !rv.Type().Comparable() {
			return false
		}
		if _, exists := seen[it]; exists {
			return false
		}
		seen[it] = struct{}{}
	}
	return true
}

// normalizeMultiMatchWhereClauses rewrites top-level two-MATCH forms that place
// WHERE after each MATCH into a single terminal WHERE joined by AND:
// MATCH A WHERE wa MATCH B WHERE wb RETURN ...
// -> MATCH A MATCH B WHERE wa AND wb RETURN ...
func normalizeMultiMatchWhereClauses(query string) string {
	trimmed := strings.TrimSpace(query)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "MATCH ") {
		return query
	}

	returnIdx := findKeywordIndex(trimmed, "RETURN")
	if returnIdx <= 0 {
		return query
	}
	mainPart := strings.TrimSpace(trimmed[:returnIdx])
	tailPart := strings.TrimSpace(trimmed[returnIdx:])

	searchFrom := len("MATCH")
	secondMatchIdx := -1
	if searchFrom < len(mainPart) {
		if rel := findKeywordIndex(mainPart[searchFrom:], "MATCH"); rel >= 0 {
			secondMatchIdx = searchFrom + rel
		}
	}
	if secondMatchIdx <= 0 {
		return query
	}
	left := strings.TrimSpace(mainPart[:secondMatchIdx])
	right := strings.TrimSpace(mainPart[secondMatchIdx+len("MATCH"):])
	if !strings.HasPrefix(strings.ToUpper(left), "MATCH ") {
		return query
	}

	leftWhereIdx := findKeywordIndex(left, "WHERE")
	rightWhereIdx := findKeywordIndex(right, "WHERE")
	if leftWhereIdx <= 0 || rightWhereIdx <= 0 {
		return query
	}

	leftPattern := strings.TrimSpace(left[len("MATCH "):leftWhereIdx])
	leftWhere := strings.TrimSpace(left[leftWhereIdx+len("WHERE"):])
	rightPattern := strings.TrimSpace(right[:rightWhereIdx])
	rightWhere := strings.TrimSpace(right[rightWhereIdx+len("WHERE"):])

	if leftPattern == "" || rightPattern == "" || leftWhere == "" || rightWhere == "" {
		return query
	}

	var b strings.Builder
	b.WriteString("MATCH ")
	b.WriteString(leftPattern)
	b.WriteString(" MATCH ")
	b.WriteString(rightPattern)
	b.WriteString(" WHERE ")
	b.WriteString(leftWhere)
	b.WriteString(" AND ")
	b.WriteString(rightWhere)
	b.WriteString(" ")
	b.WriteString(tailPart)
	return b.String()
}

var unwindCollectDistinctProjectionPattern = regexp.MustCompile(`(?is)^\s*WITH\s+collect\s*\(\s*DISTINCT\s+([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*\)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s+RETURN\s+([A-Za-z_][A-Za-z0-9_]*)\s*$`)

func (e *StorageExecutor) executeUnwindWithCollectProjection(unwindVar string, items []interface{}, restQuery string) (*ExecuteResult, bool) {
	m := unwindCollectDistinctProjectionPattern.FindStringSubmatch(strings.TrimSpace(restQuery))
	if len(m) != 5 {
		return nil, false
	}
	srcVar := strings.TrimSpace(m[1])
	prop := strings.TrimSpace(m[2])
	alias := strings.TrimSpace(m[3])
	returnAlias := strings.TrimSpace(m[4])
	if !strings.EqualFold(srcVar, unwindVar) || !strings.EqualFold(alias, returnAlias) {
		return nil, false
	}

	seen := map[interface{}]struct{}{}
	values := make([]interface{}, 0, len(items))
	for _, it := range items {
		var v interface{}
		switch row := it.(type) {
		case map[string]interface{}:
			v = row[prop]
		case map[interface{}]interface{}:
			v = row[prop]
		default:
			continue
		}
		if _, exists := seen[v]; exists {
			continue
		}
		seen[v] = struct{}{}
		values = append(values, v)
	}

	return &ExecuteResult{
		Columns: []string{alias},
		Rows:    [][]interface{}{{values}},
	}, true
}

// executeDoubleUnwind handles double UNWIND clauses like:
// UNWIND [[1,2],[3,4]] AS pair UNWIND pair AS num RETURN num
func (e *StorageExecutor) executeDoubleUnwind(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	// Check for dependent range expressions (range(1, i) where i is from first UNWIND)
	if containsOutsideStrings(upper, "RANGE(") {
		// Find if second UNWIND uses range with first variable
		firstAsIdx := findKeywordIndex(cypher, "AS")
		if firstAsIdx >= 0 {
			afterAs := strings.TrimSpace(cypher[firstAsIdx+2:])
			varEnd := strings.IndexAny(afterAs, " \t\n")
			if varEnd > 0 {
				firstVar := strings.ToUpper(afterAs[:varEnd])
				restQuery := strings.ToUpper(afterAs[varEnd:])
				// Check if range() contains the first variable
				if containsOutsideStrings(restQuery, "RANGE(") && containsOutsideStrings(restQuery, firstVar) {
					return nil, fmt.Errorf("dependent range in double UNWIND is not supported")
				}
			}
		}
	}

	// Parse first UNWIND
	firstUnwindIdx := findKeywordIndex(cypher, "UNWIND")
	if firstUnwindIdx == -1 {
		return nil, fmt.Errorf("UNWIND clause not found")
	}

	afterFirst := cypher[firstUnwindIdx+6:]
	firstAsIdx := findKeywordNotInBrackets(afterFirst, " AS ")
	if firstAsIdx == -1 {
		return nil, fmt.Errorf("first UNWIND requires AS clause")
	}

	firstListExpr := strings.TrimSpace(afterFirst[:firstAsIdx])
	afterFirstAsStart := firstAsIdx + len("AS")
	for afterFirstAsStart < len(afterFirst) && isASCIISpace(afterFirst[afterFirstAsStart]) {
		afterFirstAsStart++
	}
	afterFirstAs := strings.TrimSpace(afterFirst[afterFirstAsStart:])

	// Get first variable name
	varEndIdx := strings.IndexAny(afterFirstAs, " \t\n")
	if varEndIdx == -1 {
		return nil, fmt.Errorf("malformed double UNWIND")
	}
	firstVar := afterFirstAs[:varEndIdx]
	restQuery := strings.TrimSpace(afterFirstAs[varEndIdx:])

	// Parse second UNWIND
	if !strings.HasPrefix(strings.ToUpper(restQuery), "UNWIND") {
		return nil, fmt.Errorf("expected second UNWIND")
	}

	afterSecond := restQuery[6:]
	secondAsIdx := findKeywordNotInBrackets(afterSecond, " AS ")
	if secondAsIdx == -1 {
		return nil, fmt.Errorf("second UNWIND requires AS clause")
	}

	secondListExpr := strings.TrimSpace(afterSecond[:secondAsIdx])
	afterSecondAsStart := secondAsIdx + len("AS")
	for afterSecondAsStart < len(afterSecond) && isASCIISpace(afterSecond[afterSecondAsStart]) {
		afterSecondAsStart++
	}
	afterSecondAs := strings.TrimSpace(afterSecond[afterSecondAsStart:])

	var secondVar, finalRest string
	varEndIdx2 := strings.IndexAny(afterSecondAs, " \t\n")
	if varEndIdx2 == -1 {
		secondVar = afterSecondAs
		finalRest = ""
	} else {
		secondVar = afterSecondAs[:varEndIdx2]
		finalRest = strings.TrimSpace(afterSecondAs[varEndIdx2:])
	}

	// Evaluate the first list
	firstList := e.evaluateExpressionWithContext(firstListExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))

	var outerItems []interface{}
	switch v := firstList.(type) {
	case []interface{}:
		outerItems = v
	case nil:
		outerItems = []interface{}{}
	default:
		outerItems = []interface{}{firstList}
	}

	// Collect all paired items (outer, inner) for cartesian or nested product
	type pairedItem struct {
		outer interface{}
		inner interface{}
	}
	var allPairedItems []pairedItem

	for _, outerItem := range outerItems {
		// The second UNWIND expression should reference the first variable
		// If secondListExpr == firstVar, use outerItem directly (nested case)
		var innerList interface{}
		if secondListExpr == firstVar {
			innerList = outerItem
		} else {
			// Cartesian product - evaluate second list independently
			innerList = e.evaluateExpressionWithContext(secondListExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
		}

		switch inner := innerList.(type) {
		case []interface{}:
			for _, innerItem := range inner {
				allPairedItems = append(allPairedItems, pairedItem{outer: outerItem, inner: innerItem})
			}
		case nil:
			// Skip
		default:
			allPairedItems = append(allPairedItems, pairedItem{outer: outerItem, inner: innerList})
		}
	}

	// Process RETURN clause
	if strings.HasPrefix(strings.ToUpper(finalRest), "RETURN") {
		returnClause := strings.TrimSpace(finalRest[6:])
		returnItems := e.parseReturnItems(returnClause)

		result := &ExecuteResult{
			Columns: make([]string, len(returnItems)),
			Rows:    make([][]interface{}, 0, len(allPairedItems)),
		}

		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns[i] = item.alias
			} else {
				result.Columns[i] = item.expr
			}
		}

		for _, paired := range allPairedItems {
			row := make([]interface{}, len(returnItems))
			for i, item := range returnItems {
				if item.expr == secondVar {
					row[i] = paired.inner
				} else if item.expr == firstVar {
					row[i] = paired.outer
				} else {
					row[i] = e.evaluateExpressionWithContext(item.expr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
				}
			}
			result.Rows = append(result.Rows, row)
		}

		return result, nil
	}

	// Default: return all paired items (inner values only)
	result := &ExecuteResult{
		Columns: []string{secondVar},
		Rows:    make([][]interface{}, len(allPairedItems)),
	}
	for i, paired := range allPairedItems {
		result.Rows[i] = []interface{}{paired.inner}
	}
	return result, nil
}

// ========================================
// UNION Clause
// ========================================

// executeUnion handles UNION / UNION ALL
// Supports both single UNION (query1 UNION query2) and chained UNIONs (query1 UNION query2 UNION query3 ...)
// Handles UNION with flexible whitespace (spaces, newlines, tabs)
func (e *StorageExecutor) executeUnion(ctx context.Context, cypher string, unionAll bool) (*ExecuteResult, error) {
	// Normalize whitespace for easier parsing (preserve structure but make UNION detection easier)
	// Replace newlines/tabs with spaces, then normalize multiple spaces to single space
	normalized := regexp.MustCompile(`\s+`).ReplaceAllString(cypher, " ")
	upper := strings.ToUpper(normalized)

	var separatorPattern *regexp.Regexp
	if unionAll {
		// Match "UNION ALL" with flexible whitespace
		separatorPattern = regexp.MustCompile(`(?i)\s+UNION\s+ALL\s+`)
	} else {
		// Match "UNION" with flexible whitespace (but not "UNION ALL")
		// We'll check manually to avoid matching "UNION ALL"
		separatorPattern = regexp.MustCompile(`(?i)\s+UNION\s+`)
	}

	// Find all UNION occurrences (handle chained UNIONs)
	var queries []string
	remaining := normalized
	lastIndex := 0

	for {
		matches := separatorPattern.FindStringIndex(upper[lastIndex:])
		if matches == nil {
			// No more UNIONs - add remaining query
			if strings.TrimSpace(remaining[lastIndex:]) != "" {
				queries = append(queries, strings.TrimSpace(remaining[lastIndex:]))
			}
			break
		}

		// Extract query before UNION
		unionStart := lastIndex + matches[0]
		unionEnd := lastIndex + matches[1]

		// For UNION (not UNION ALL), check if this is actually "UNION ALL"
		if !unionAll {
			// Check if the next characters after "UNION" are "ALL"
			if unionEnd < len(upper) {
				afterUnion := strings.TrimSpace(upper[unionEnd:])
				if strings.HasPrefix(afterUnion, "ALL") {
					// This is "UNION ALL", skip it (we're looking for plain UNION)
					// Find the end of "ALL"
					allEnd := unionEnd
					for allEnd < len(upper) && (upper[allEnd] == ' ' || upper[allEnd] == 'A' || upper[allEnd] == 'L') {
						if allEnd+1 < len(upper) && upper[allEnd] == 'L' && upper[allEnd+1] == 'L' {
							allEnd += 2
							break
						}
						allEnd++
					}
					lastIndex = allEnd
					continue
				}
			}
		}
		query := strings.TrimSpace(remaining[lastIndex:unionStart])
		if query != "" {
			queries = append(queries, query)
		}

		// Move past this UNION
		lastIndex = unionEnd
	}

	if len(queries) < 2 {
		return nil, fmt.Errorf("UNION clause not found in query: %q", truncateQuery(cypher, 80))
	}

	// Execute all queries and combine results
	var combinedResult *ExecuteResult
	seen := make(map[string]bool) // For UNION (distinct) deduplication

	for i, query := range queries {
		result, err := e.executeInternal(ctx, query, nil)
		if err != nil {
			return nil, fmt.Errorf("error in UNION query %d (%q): %w", i+1, truncateQuery(query, 50), err)
		}
		// Some execution branches can return empty column metadata when no rows are produced,
		// even though the query has an explicit RETURN/YIELD projection. For UNION semantics we
		// must validate/provide branch column shapes deterministically.
		if len(result.Columns) == 0 {
			result.Columns = e.inferExplainColumns(query)
			if len(result.Columns) == 0 {
				// UNION branch execution can legitimately return zero rows; still preserve
				// projected column shape from the branch RETURN clause.
				result.Columns = e.inferTopLevelReturnColumns(query)
			}
		}

		if combinedResult == nil {
			// First query - initialize result
			combinedResult = &ExecuteResult{
				Columns: result.Columns,
				Rows:    make([][]interface{}, 0),
			}
		} else {
			// Validate column count matches
			if len(combinedResult.Columns) != len(result.Columns) {
				return nil, fmt.Errorf("UNION queries must return the same number of columns (got %d and %d)", len(combinedResult.Columns), len(result.Columns))
			}
		}

		// Add rows from this query
		if unionAll {
			// UNION ALL - include all rows
			combinedResult.Rows = append(combinedResult.Rows, result.Rows...)
		} else {
			// UNION (distinct) - deduplicate rows
			for _, row := range result.Rows {
				key := fmt.Sprintf("%v", row)
				if !seen[key] {
					combinedResult.Rows = append(combinedResult.Rows, row)
					seen[key] = true
				}
			}
		}
	}

	if combinedResult == nil {
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	return combinedResult, nil
}

// ========================================
// OPTIONAL MATCH Clause
// ========================================

// executeOptionalMatch handles OPTIONAL MATCH - returns null for non-matches
func (e *StorageExecutor) executeOptionalMatch(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	upper := strings.ToUpper(cypher)
	optMatchIdx := strings.Index(upper, "OPTIONAL MATCH")
	if optMatchIdx == -1 {
		return nil, fmt.Errorf("OPTIONAL MATCH not found in query: %q", truncateQuery(cypher, 80))
	}

	modifiedQuery := cypher[:optMatchIdx] + "MATCH" + cypher[optMatchIdx+14:]

	result, err := e.executeMatch(ctx, modifiedQuery)

	// Handle error case - return result with null values
	if err != nil {
		// Default to a single null row if we can't determine columns
		return &ExecuteResult{
			Columns: []string{"result"},
			Rows:    [][]interface{}{{nil}},
		}, nil
	}

	// Handle empty result - return null row preserving columns
	if len(result.Rows) == 0 {
		nullRow := make([]interface{}, len(result.Columns))
		for i := range nullRow {
			nullRow[i] = nil
		}
		return &ExecuteResult{
			Columns: result.Columns,
			Rows:    [][]interface{}{nullRow},
		}, nil
	}

	return result, nil
}

// joinedRow represents a row from a left outer join between MATCH and OPTIONAL MATCH
type joinedRow struct {
	initialNode  *storage.Node
	relatedNode  *storage.Node
	relationship *storage.Edge
}

// optionalRelPattern holds parsed relationship info for OPTIONAL MATCH
type optionalRelPattern struct {
	sourceVar   string
	relType     string
	relVar      string
	targetVar   string
	targetLabel string
	targetProps map[string]interface{}
	direction   string // "out", "in", "both"
}

// optionalRelResult holds a node and its connecting edge for OPTIONAL MATCH
type optionalRelResult struct {
	node *storage.Node
	edge *storage.Edge
}

// executeCompoundMatchOptionalMatch handles MATCH ... OPTIONAL MATCH ... WITH ... RETURN queries
// This implements left outer join semantics for relationship traversals with aggregation support
func (e *StorageExecutor) executeCompoundMatchOptionalMatch(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	// Find OPTIONAL MATCH position
	optMatchIdx := findKeywordIndex(cypher, "OPTIONAL MATCH")
	if optMatchIdx == -1 {
		return nil, fmt.Errorf("OPTIONAL MATCH not found in compound query: %q", truncateQuery(cypher, 80))
	}

	// Find WITH or RETURN after OPTIONAL MATCH
	remainingAfterOptMatch := cypher[optMatchIdx+14:] // Skip "OPTIONAL MATCH"
	withIdx := findKeywordIndex(remainingAfterOptMatch, "WITH")
	returnIdx := findKeywordIndex(remainingAfterOptMatch, "RETURN")

	// Determine where OPTIONAL MATCH pattern ends
	optMatchEndIdx := len(remainingAfterOptMatch)
	if withIdx > 0 && (returnIdx == -1 || withIdx < returnIdx) {
		optMatchEndIdx = withIdx
	} else if returnIdx > 0 {
		optMatchEndIdx = returnIdx
	}

	optMatchPattern := strings.TrimSpace(remainingAfterOptMatch[:optMatchEndIdx])
	restOfQuery := ""
	if optMatchEndIdx < len(remainingAfterOptMatch) {
		restOfQuery = strings.TrimSpace(remainingAfterOptMatch[optMatchEndIdx:])
	}

	// Parse the initial MATCH clause section (everything between MATCH and OPTIONAL MATCH)
	// This may contain: node pattern, WHERE clause, and WITH DISTINCT
	initialSection := strings.TrimSpace(cypher[5:optMatchIdx]) // Get original case, skip "MATCH"

	// Extract WHERE clause if present (between node pattern and WITH DISTINCT/OPTIONAL MATCH)
	var whereClause string
	whereIdx := findKeywordIndex(initialSection, "WHERE")

	// Find standalone WITH (not part of "STARTS WITH" or "ENDS WITH")
	firstWithIdx := findStandaloneWithIndex(initialSection)

	// Determine the node pattern end
	nodePatternEnd := len(initialSection)
	if whereIdx > 0 {
		nodePatternEnd = whereIdx
	} else if firstWithIdx > 0 {
		nodePatternEnd = firstWithIdx
	}

	nodePatternStr := strings.TrimSpace(initialSection[:nodePatternEnd])
	nodePattern := e.parseNodePattern(nodePatternStr)
	if nodePattern.variable == "" {
		return nil, fmt.Errorf("could not parse node pattern from MATCH clause: %q", truncateQuery(nodePatternStr, 50))
	}

	// Extract WHERE clause content if present
	if whereIdx > 0 {
		whereEnd := len(initialSection)
		if firstWithIdx > whereIdx {
			whereEnd = firstWithIdx
		}
		whereClause = strings.TrimSpace(initialSection[whereIdx+5 : whereEnd]) // Skip "WHERE"
	}

	// Get all nodes matching the initial pattern
	var initialNodes []*storage.Node
	var err error
	if len(nodePattern.labels) > 0 {
		initialNodes, err = e.storage.GetNodesByLabel(nodePattern.labels[0])
	} else {
		initialNodes, err = e.storage.AllNodes()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get initial nodes: %w", err)
	}

	// Filter by properties if any
	if len(nodePattern.properties) > 0 {
		filtered := make([]*storage.Node, 0)
		for _, node := range initialNodes {
			match := true
			for k, v := range nodePattern.properties {
				if node.Properties[k] != v {
					match = false
					break
				}
			}
			if match {
				filtered = append(filtered, node)
			}
		}
		initialNodes = filtered
	}

	// Apply WHERE clause filtering if present
	if whereClause != "" {
		initialNodes = e.filterNodes(initialNodes, nodePattern.variable, whereClause)
	}

	// Parse the OPTIONAL MATCH relationship pattern
	relPattern := e.parseOptionalRelPattern(optMatchPattern)

	// Fast path: OPTIONAL MATCH incoming count aggregation (Northwind-style).
	// Avoid building joinedRows and per-node edge scans.
	if res, ok, err := e.tryFastCompoundOptionalMatchCount(initialNodes, nodePattern, relPattern, restOfQuery); ok || err != nil {
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	// Build result rows - this is left outer join semantics
	var joinedRows []joinedRow

	for _, node := range initialNodes {
		// Try to find related nodes via the relationship
		relatedNodes := e.findOptionalRelatedNodes(node, optMatchPattern, relPattern)

		if len(relatedNodes) == 0 {
			// No match - add row with null for the optional part (left outer join)
			joinedRows = append(joinedRows, joinedRow{
				initialNode:  node,
				relatedNode:  nil,
				relationship: nil,
			})
		} else {
			// Add a row for each match
			for _, related := range relatedNodes {
				joinedRows = append(joinedRows, joinedRow{
					initialNode:  node,
					relatedNode:  related.node,
					relationship: related.edge,
				})
			}
		}
	}

	// Now process WITH and RETURN clauses
	if strings.HasPrefix(strings.ToUpper(restOfQuery), "WITH") {
		optMatchAfterWith := findKeywordIndex(restOfQuery, "OPTIONAL MATCH")
		returnAfterWith := findKeywordIndex(restOfQuery, "RETURN")
		if optMatchAfterWith > 0 && (returnAfterWith == -1 || optMatchAfterWith < returnAfterWith) {
			return e.executeJoinedRowsWithOptionalMatch(joinedRows, nodePattern.variable, relPattern.targetVar, relPattern.relVar, restOfQuery)
		}
		return e.processWithAggregation(joinedRows, nodePattern.variable, relPattern.targetVar, relPattern.relVar, restOfQuery)
	}

	if strings.HasPrefix(strings.ToUpper(restOfQuery), "RETURN") {
		return e.buildJoinedResult(joinedRows, nodePattern.variable, relPattern.targetVar, relPattern.relVar, restOfQuery)
	}

	// No WITH or RETURN, just return count
	return &ExecuteResult{
		Columns: []string{"matched"},
		Rows:    [][]interface{}{{int64(len(joinedRows))}},
	}, nil
}

// parseOptionalRelPattern parses patterns like (a)-[r:TYPE]->(b:Label)
func (e *StorageExecutor) parseOptionalRelPattern(pattern string) optionalRelPattern {
	result := optionalRelPattern{
		direction:   "out",
		targetProps: make(map[string]interface{}),
	}
	pattern = strings.TrimSpace(pattern)

	// Check direction
	if strings.Contains(pattern, "<-") {
		result.direction = "in"
	} else if strings.Contains(pattern, "->") {
		result.direction = "out"
	} else if strings.Contains(pattern, "-") {
		result.direction = "both"
	}

	// Extract source variable
	if idx := strings.Index(pattern, "("); idx >= 0 {
		endIdx := strings.Index(pattern[idx:], ")")
		if endIdx > 0 {
			sourceStr := pattern[idx+1 : idx+endIdx]
			if colonIdx := strings.Index(sourceStr, ":"); colonIdx > 0 {
				result.sourceVar = strings.TrimSpace(sourceStr[:colonIdx])
			} else {
				result.sourceVar = strings.TrimSpace(sourceStr)
			}
		}
	}

	// Extract relationship type and variable
	if idx := strings.Index(pattern, "["); idx >= 0 {
		endIdx := strings.Index(pattern[idx:], "]")
		if endIdx > 0 {
			relStr := pattern[idx+1 : idx+endIdx]
			if colonIdx := strings.Index(relStr, ":"); colonIdx >= 0 {
				result.relVar = strings.TrimSpace(relStr[:colonIdx])
				result.relType = strings.TrimSpace(relStr[colonIdx+1:])
			} else {
				result.relVar = strings.TrimSpace(relStr)
			}
		}
	}

	// Extract target
	relEnd := strings.Index(pattern, "]")
	if relEnd > 0 {
		remaining := pattern[relEnd+1:]
		if idx := strings.Index(remaining, "("); idx >= 0 {
			endIdx := strings.Index(remaining[idx:], ")")
			if endIdx > 0 {
				targetStr := remaining[idx+1 : idx+endIdx]
				targetInfo := e.parseNodePattern("(" + targetStr + ")")
				if targetInfo.variable != "" {
					result.targetVar = targetInfo.variable
				}
				if len(targetInfo.labels) > 0 {
					result.targetLabel = targetInfo.labels[0]
				}
				if len(targetInfo.properties) > 0 {
					result.targetProps = targetInfo.properties
				}
			}
		}
	}

	return result
}

// findRelatedNodes finds nodes connected via the specified relationship pattern
func (e *StorageExecutor) findRelatedNodes(sourceNode *storage.Node, pattern optionalRelPattern) []optionalRelResult {
	var results []optionalRelResult
	var edges []*storage.Edge

	// Get edges based on direction
	switch pattern.direction {
	case "out":
		outEdges, err := e.storage.GetOutgoingEdges(sourceNode.ID)
		if err != nil {
			return results
		}
		edges = outEdges
	case "in":
		inEdges, err := e.storage.GetIncomingEdges(sourceNode.ID)
		if err != nil {
			return results
		}
		edges = inEdges
	case "both":
		outEdges, _ := e.storage.GetOutgoingEdges(sourceNode.ID)
		inEdges, _ := e.storage.GetIncomingEdges(sourceNode.ID)
		edges = append(outEdges, inEdges...)
	}

	for _, edge := range edges {
		// Check relationship type if specified
		if pattern.relType != "" && edge.Type != pattern.relType {
			continue
		}

		// Determine target node ID
		var targetNodeID storage.NodeID
		if edge.StartNode == sourceNode.ID {
			targetNodeID = edge.EndNode
		} else {
			targetNodeID = edge.StartNode
		}

		// Get the target node
		targetNode, err := e.storage.GetNode(targetNodeID)
		if err != nil || targetNode == nil {
			continue
		}

		// Check target label if specified
		if pattern.targetLabel != "" {
			hasLabel := false
			for _, label := range targetNode.Labels {
				if label == pattern.targetLabel {
					hasLabel = true
					break
				}
			}
			if !hasLabel {
				continue
			}
		}

		// Check target properties if specified
		if len(pattern.targetProps) > 0 {
			match := true
			for k, expected := range pattern.targetProps {
				actual, ok := targetNode.Properties[k]
				if !ok || !e.compareEqual(actual, expected) {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		results = append(results, optionalRelResult{node: targetNode, edge: edge})
	}

	return results
}

func (e *StorageExecutor) findOptionalRelatedNodes(sourceNode *storage.Node, patternText string, pattern optionalRelPattern) []optionalRelResult {
	if strings.Contains(patternText, "*") {
		traversal := e.parseTraversalPattern(patternText)
		if traversal == nil {
			return nil
		}
		paths := e.traverseFromNode(sourceNode, traversal)
		results := make([]optionalRelResult, 0, len(paths))
		seen := make(map[string]bool)
		for _, path := range paths {
			if len(path.Nodes) == 0 {
				continue
			}
			node := path.Nodes[len(path.Nodes)-1]
			var edge *storage.Edge
			if len(path.Relationships) > 0 {
				edge = path.Relationships[0]
			}
			key := string(node.ID)
			if edge != nil {
				key += ":" + string(edge.ID)
			}
			if !seen[key] {
				seen[key] = true
				results = append(results, optionalRelResult{node: node, edge: edge})
			}
		}
		return results
	}

	return e.findRelatedNodes(sourceNode, pattern)
}

// processWithAggregation handles WITH clauses with aggregation functions
// It finds the WITH clause that contains aggregations and processes them
// Also evaluates CASE WHEN expressions in WITH clauses
func buildJoinedEvaluationContext(row joinedRow, sourceVar, targetVar, relVar string) (map[string]*storage.Node, map[string]*storage.Edge) {
	nodeCtx := make(map[string]*storage.Node, 2)
	if sourceVar != "" {
		nodeCtx[sourceVar] = row.initialNode
	}
	if targetVar != "" {
		nodeCtx[targetVar] = row.relatedNode
	}
	relCtx := make(map[string]*storage.Edge, 1)
	if relVar != "" {
		relCtx[relVar] = row.relationship
	}
	return nodeCtx, relCtx
}

func stripTrailingReturnClauses(returnClause string) string {
	returnEnd := len(returnClause)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(returnClause, keyword); idx >= 0 && idx < returnEnd {
			returnEnd = idx
		}
	}
	return strings.TrimSpace(returnClause[:returnEnd])
}

func joinedValueKey(val interface{}) string {
	switch v := val.(type) {
	case *storage.Node:
		if v == nil {
			return "node:nil"
		}
		return "node:" + string(v.ID)
	case *storage.Edge:
		if v == nil {
			return "edge:nil"
		}
		return "edge:" + string(v.ID)
	default:
		return fmt.Sprintf("%#v", val)
	}
}

func (e *StorageExecutor) executeJoinedRowsWithOptionalMatch(rows []joinedRow, sourceVar, targetVar, relVar, query string) (*ExecuteResult, error) {
	withIdx := findKeywordIndex(query, "WITH")
	optMatchIdx := findKeywordIndex(query, "OPTIONAL MATCH")
	returnIdx := findKeywordIndex(query, "RETURN")
	if withIdx == -1 || optMatchIdx == -1 || returnIdx == -1 {
		return nil, fmt.Errorf("WITH, OPTIONAL MATCH, and RETURN clauses required")
	}

	withSection := strings.TrimSpace(query[withIdx+4 : optMatchIdx])
	postWithWhere := ""
	withClause := withSection
	if postWhereIdx := findKeywordIndex(withSection, "WHERE"); postWhereIdx > 0 {
		withClause = strings.TrimSpace(withSection[:postWhereIdx])
		postWithWhere = strings.TrimSpace(withSection[postWhereIdx+5:])
	}

	distinct := false
	if strings.HasPrefix(strings.ToUpper(withClause), "DISTINCT ") {
		distinct = true
		withClause = strings.TrimSpace(withClause[9:])
	}

	withItems := e.splitWithItems(withClause)
	type computedRow struct {
		values map[string]interface{}
	}
	computedRows := make([]computedRow, 0, len(rows))
	withAliases := make([]string, 0, len(withItems))

	for _, row := range rows {
		nodeCtx, relCtx := buildJoinedEvaluationContext(row, sourceVar, targetVar, relVar)
		values := make(map[string]interface{}, len(withItems))
		for _, item := range withItems {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			upperItem := strings.ToUpper(item)
			alias := item
			expr := item
			if asIdx := strings.Index(upperItem, " AS "); asIdx > 0 {
				expr = strings.TrimSpace(item[:asIdx])
				alias = strings.TrimSpace(item[asIdx+4:])
			}
			values[alias] = e.evaluateExpressionWithContext(expr, nodeCtx, relCtx)
			if len(computedRows) == 0 {
				withAliases = append(withAliases, alias)
			}
		}
		computedRows = append(computedRows, computedRow{values: values})
	}

	if distinct {
		seen := make(map[string]bool)
		unique := make([]computedRow, 0, len(computedRows))
		for _, row := range computedRows {
			parts := make([]string, 0, len(withAliases))
			for _, alias := range withAliases {
				parts = append(parts, alias+"="+joinedValueKey(row.values[alias]))
			}
			key := strings.Join(parts, "|")
			if !seen[key] {
				seen[key] = true
				unique = append(unique, row)
			}
		}
		computedRows = unique
	}

	if postWithWhere != "" {
		filtered := make([]computedRow, 0, len(computedRows))
		for _, row := range computedRows {
			if e.evaluateWithWhereCondition(postWithWhere, row.values) {
				filtered = append(filtered, row)
			}
		}
		computedRows = filtered
	}

	optMatchPattern := strings.TrimSpace(query[optMatchIdx+14 : returnIdx])
	optMatchWhereClause := ""
	if optMatchWhereIdx := findKeywordIndex(optMatchPattern, "WHERE"); optMatchWhereIdx > 0 {
		optMatchWhereClause = strings.TrimSpace(optMatchPattern[optMatchWhereIdx+5:])
		optMatchPattern = strings.TrimSpace(optMatchPattern[:optMatchWhereIdx])
	}
	relPattern := e.parseOptionalRelPattern(optMatchPattern)

	type optionalRow struct {
		computedValues map[string]interface{}
		relatedNode    *storage.Node
		relationship   *storage.Edge
	}
	joinedOptionalRows := make([]optionalRow, 0, len(computedRows))
	for _, row := range computedRows {
		sourceNode, _ := row.values[relPattern.sourceVar].(*storage.Node)
		if sourceNode == nil {
			joinedOptionalRows = append(joinedOptionalRows, optionalRow{computedValues: row.values})
			continue
		}
		relatedNodes := e.findOptionalRelatedNodes(sourceNode, optMatchPattern, relPattern)
		if len(relatedNodes) == 0 {
			joinedOptionalRows = append(joinedOptionalRows, optionalRow{computedValues: row.values})
			continue
		}
		addedAny := false
		for _, related := range relatedNodes {
			if optMatchWhereClause != "" && related.node != nil && !e.nodeMatchesWhereClause(related.node, optMatchWhereClause, relPattern.targetVar) {
				continue
			}
			joinedOptionalRows = append(joinedOptionalRows, optionalRow{computedValues: row.values, relatedNode: related.node, relationship: related.edge})
			addedAny = true
		}
		if !addedAny {
			joinedOptionalRows = append(joinedOptionalRows, optionalRow{computedValues: row.values})
		}
	}

	returnClause := stripTrailingReturnClauses(strings.TrimSpace(query[returnIdx+6:]))
	returnItems := e.parseReturnItems(returnClause)
	result := &ExecuteResult{Columns: make([]string, len(returnItems)), Rows: make([][]interface{}, 0, len(joinedOptionalRows))}
	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	for _, row := range joinedOptionalRows {
		resultRow := make([]interface{}, len(returnItems))
		nodeMap := make(map[string]*storage.Node)
		edgeMap := make(map[string]*storage.Edge)
		for varName, val := range row.computedValues {
			if node, ok := val.(*storage.Node); ok {
				nodeMap[varName] = node
			}
			if edge, ok := val.(*storage.Edge); ok {
				edgeMap[varName] = edge
			}
		}
		if relPattern.targetVar != "" {
			nodeMap[relPattern.targetVar] = row.relatedNode
		}
		if relPattern.relVar != "" {
			edgeMap[relPattern.relVar] = row.relationship
		}
		for i, item := range returnItems {
			if val, ok := row.computedValues[item.expr]; ok {
				resultRow[i] = val
				continue
			}
			resultRow[i] = e.evaluateExpressionWithContext(item.expr, nodeMap, edgeMap)
		}
		result.Rows = append(result.Rows, resultRow)
	}

	orderByIdx := findKeywordIndex(query, "ORDER")
	if orderByIdx > 0 {
		orderStart := orderByIdx + 5
		for orderStart < len(query) && isWhitespace(query[orderStart]) {
			orderStart++
		}
		if orderStart+2 <= len(query) && strings.EqualFold(query[orderStart:orderStart+2], "BY") {
			orderStart += 2
		}
		orderPart := query[orderStart:]
		endIdx := len(orderPart)
		for _, kw := range []string{"SKIP", "LIMIT"} {
			if idx := findKeywordIndex(orderPart, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderExpr := strings.TrimSpace(orderPart[:endIdx])
		result.Rows = e.orderResultRows(result.Rows, result.Columns, orderExpr)
	}

	skipIdx := findKeywordIndex(query, "SKIP")
	skip := 0
	if skipIdx > 0 {
		skipPart := strings.TrimSpace(query[skipIdx+4:])
		skipPart = strings.Fields(skipPart)[0]
		if s, err := strconv.Atoi(skipPart); err == nil {
			skip = s
		}
	}

	limitIdx := findKeywordIndex(query, "LIMIT")
	limit := -1
	if limitIdx > 0 {
		limitPart := strings.TrimSpace(query[limitIdx+5:])
		limitPart = strings.Fields(limitPart)[0]
		if l, err := strconv.Atoi(limitPart); err == nil {
			limit = l
		}
	}

	if skip > 0 || limit >= 0 {
		startIdx := skip
		if startIdx > len(result.Rows) {
			startIdx = len(result.Rows)
		}
		endIdx := len(result.Rows)
		if limit >= 0 && startIdx+limit < endIdx {
			endIdx = startIdx + limit
		}
		result.Rows = result.Rows[startIdx:endIdx]
	}

	return result, nil
}

func (e *StorageExecutor) processWithAggregation(rows []joinedRow, sourceVar, targetVar, relVar, restOfQuery string) (*ExecuteResult, error) {
	// Find RETURN clause
	returnIdx := findKeywordIndex(restOfQuery, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("RETURN clause required after WITH")
	}

	// First, check for CASE WHEN expressions in the first WITH clause and evaluate them
	// This computes values like: WITH f, c, CASE WHEN c IS NOT NULL THEN 1 ELSE 0 END as hasChunk
	computedValues := make(map[int]map[string]interface{}) // row index -> computed values
	firstWithIdx := findKeywordIndex(restOfQuery, "WITH")
	if firstWithIdx >= 0 {
		// Find where first WITH ends (at next WITH, RETURN, or end)
		firstWithEnd := returnIdx
		nextWithIdx := findKeywordIndex(restOfQuery[firstWithIdx+4:], "WITH")
		if nextWithIdx > 0 {
			firstWithEnd = firstWithIdx + 4 + nextWithIdx
		}

		firstWithClause := strings.TrimSpace(restOfQuery[firstWithIdx+4 : firstWithEnd])
		withItems := e.splitWithItems(firstWithClause)

		// Check if any item is a CASE expression
		for _, item := range withItems {
			item = strings.TrimSpace(item)
			upperItem := strings.ToUpper(item)
			asIdx := strings.Index(upperItem, " AS ")
			if asIdx > 0 {
				expr := strings.TrimSpace(item[:asIdx])
				alias := strings.TrimSpace(item[asIdx+4:])

				if isCaseExpression(expr) {
					// Evaluate CASE for each row
					for rowIdx, r := range rows {
						if computedValues[rowIdx] == nil {
							computedValues[rowIdx] = make(map[string]interface{})
						}
						nodeMap, relMap := buildJoinedEvaluationContext(r, sourceVar, targetVar, relVar)
						computedValues[rowIdx][alias] = e.evaluateCaseExpression(expr, nodeMap, relMap)
					}
				}
			}
		}
	}

	// Find the WITH clause that contains the aggregations
	// This handles cases like: WITH f, c, CASE... WITH COUNT(f)... RETURN...
	// We need to find the WITH that has COUNT/SUM/COLLECT etc.
	aggregationWithStart := -1
	aggregationWithEnd := returnIdx

	// Look for WITH clauses between start and RETURN
	queryBeforeReturn := restOfQuery[:returnIdx]
	withIdx := 0
	for {
		nextWithIdx := findKeywordIndex(queryBeforeReturn[withIdx:], "WITH")
		if nextWithIdx == -1 {
			break
		}
		absWithIdx := withIdx + nextWithIdx
		// Check if this WITH clause contains aggregation functions
		nextClauseEnd := len(queryBeforeReturn)
		followingWithIdx := findKeywordIndex(queryBeforeReturn[absWithIdx+4:], "WITH")
		if followingWithIdx > 0 {
			nextClauseEnd = absWithIdx + 4 + followingWithIdx
		}
		withContent := queryBeforeReturn[absWithIdx:nextClauseEnd]
		upperWithContent := strings.ToUpper(withContent)
		if strings.Contains(upperWithContent, "COUNT(") ||
			strings.Contains(upperWithContent, "SUM(") ||
			strings.Contains(upperWithContent, "COLLECT(") {
			aggregationWithStart = absWithIdx
			aggregationWithEnd = nextClauseEnd
			break
		}
		withIdx = absWithIdx + 4
	}

	// Parse the aggregation items from the WITH clause that contains them
	var returnItems []returnItem
	if aggregationWithStart >= 0 {
		withClause := strings.TrimSpace(restOfQuery[aggregationWithStart+4 : aggregationWithEnd])
		returnItems = e.parseReturnItems(withClause)
	} else {
		// No aggregation WITH found, use RETURN clause items
		returnClause := stripTrailingReturnClauses(strings.TrimSpace(restOfQuery[returnIdx+6:]))
		returnItems = e.parseReturnItems(returnClause)
	}

	result := &ExecuteResult{
		Columns: make([]string, len(returnItems)),
		Rows:    [][]interface{}{},
	}

	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	row := make([]interface{}, len(returnItems))

	for i, item := range returnItems {
		upperExpr := strings.ToUpper(item.expr)

		switch {
		case strings.HasPrefix(upperExpr, "COUNT(DISTINCT "):
			inner := item.expr[15 : len(item.expr)-1]
			inner = strings.TrimSpace(inner)

			if strings.HasPrefix(strings.ToUpper(inner), strings.ToUpper(sourceVar)) {
				seen := make(map[storage.NodeID]bool)
				for _, r := range rows {
					if r.initialNode != nil {
						seen[r.initialNode.ID] = true
					}
				}
				row[i] = int64(len(seen))
			} else if strings.HasPrefix(strings.ToUpper(inner), strings.ToUpper(targetVar)) {
				seen := make(map[storage.NodeID]bool)
				for _, r := range rows {
					if r.relatedNode != nil {
						seen[r.relatedNode.ID] = true
					}
				}
				row[i] = int64(len(seen))
			} else {
				row[i] = int64(0)
			}

		case strings.HasPrefix(upperExpr, "COUNT("):
			inner := item.expr[6 : len(item.expr)-1]
			inner = strings.TrimSpace(inner)

			if inner == "*" {
				row[i] = int64(len(rows))
			} else if isCaseExpression(inner) {
				// COUNT(CASE WHEN condition THEN 1 END) - count only non-NULL results
				count := int64(0)
				for _, r := range rows {
					nodeMap, relMap := buildJoinedEvaluationContext(r, sourceVar, targetVar, relVar)
					result := e.evaluateCaseExpression(inner, nodeMap, relMap)
					// count() only counts non-NULL values
					if result != nil {
						count++
					}
				}
				row[i] = count
			} else if strings.HasPrefix(strings.ToUpper(inner), strings.ToUpper(sourceVar)) {
				count := int64(0)
				for _, r := range rows {
					if r.initialNode != nil {
						count++
					}
				}
				row[i] = count
			} else if strings.HasPrefix(strings.ToUpper(inner), strings.ToUpper(targetVar)) {
				count := int64(0)
				for _, r := range rows {
					if r.relatedNode != nil {
						count++
					}
				}
				row[i] = count
			} else {
				row[i] = int64(len(rows))
			}

		case strings.HasPrefix(upperExpr, "SUM("):
			// Handle arithmetic of sum terms: SUM(x) + SUM(y) - SUM(z)
			if (strings.Contains(upperExpr, "+") || strings.Contains(upperExpr, "-")) && strings.Contains(upperExpr, "SUM(") {
				total := float64(0)
				lastOp := byte('+')
				start := 0
				parenDepth := 0
				inSingleQuote := false
				inDoubleQuote := false

				evalTerm := func(term string) (float64, bool, error) {
					inner, _, ok := extractFuncArgsWithSuffix(strings.TrimSpace(term), "sum")
					if !ok {
						return 0, false, fmt.Errorf("unsupported SUM arithmetic term: %s", strings.TrimSpace(term))
					}
					sumVal := float64(0)
					hasNonNull := false
					for rowIdx, r := range rows {
						var val interface{}
						if cv, ok := computedValues[rowIdx]; ok {
							if computed, exists := cv[strings.TrimSpace(inner)]; exists {
								val = computed
							}
						}
						if val == nil {
							nodeCtx, relCtx := buildJoinedEvaluationContext(r, sourceVar, targetVar, relVar)
							val = e.evaluateExpressionWithContext(strings.TrimSpace(inner), nodeCtx, relCtx)
						}
						if val == nil {
							continue // SUM ignores NULLs
						}
						num, ok := toFloat64(val)
						if !ok {
							return 0, false, fmt.Errorf("SUM() requires numeric values, got %T in expression %q", val, strings.TrimSpace(inner))
						}
						hasNonNull = true
						sumVal += num
					}
					return sumVal, hasNonNull, nil
				}

				for idx := 0; idx < len(item.expr); idx++ {
					ch := item.expr[idx]
					if ch == '\'' && !inDoubleQuote {
						inSingleQuote = !inSingleQuote
					} else if ch == '"' && !inSingleQuote {
						inDoubleQuote = !inDoubleQuote
					}
					if inSingleQuote || inDoubleQuote {
						continue
					}
					if ch == '(' {
						parenDepth++
						continue
					}
					if ch == ')' && parenDepth > 0 {
						parenDepth--
						continue
					}
					if parenDepth == 0 && (ch == '+' || ch == '-') {
						term := strings.TrimSpace(item.expr[start:idx])
						if term != "" {
							termValue, _, err := evalTerm(term)
							if err != nil {
								return nil, err
							}
							if lastOp == '+' {
								total += termValue
							} else {
								total -= termValue
							}
						}
						lastOp = ch
						start = idx + 1
					}
				}
				lastTerm := strings.TrimSpace(item.expr[start:])
				if lastTerm != "" {
					termValue, _, err := evalTerm(lastTerm)
					if err != nil {
						return nil, err
					}
					if lastOp == '+' {
						total += termValue
					} else {
						total -= termValue
					}
				}
				row[i] = total
				break
			}

			inner := item.expr[4 : len(item.expr)-1]
			inner = strings.TrimSpace(inner)
			sumVal := float64(0)
			hasNonNull := false
			for rowIdx, r := range rows {
				var val interface{}

				// Prefer computed aliases from preceding WITH.
				if cv, ok := computedValues[rowIdx]; ok {
					if computed, exists := cv[inner]; exists {
						val = computed
					}
				}

				if val == nil {
					nodeCtx, relCtx := buildJoinedEvaluationContext(r, sourceVar, targetVar, relVar)
					val = e.evaluateExpressionWithContext(inner, nodeCtx, relCtx)
				}

				if val == nil {
					continue // SUM ignores NULLs
				}
				num, ok := toFloat64(val)
				if !ok {
					return nil, fmt.Errorf("SUM() requires numeric values, got %T in expression %q", val, inner)
				}
				hasNonNull = true
				sumVal += num
			}
			if hasNonNull {
				row[i] = sumVal
			} else {
				row[i] = nil
			}

		case strings.HasPrefix(upperExpr, "COLLECT(DISTINCT "):
			// COLLECT(DISTINCT expression) - may have suffix like [..10]
			inner, suffix, _ := extractFuncArgsWithSuffix(item.expr, "collect")
			// Skip "DISTINCT " prefix
			if strings.HasPrefix(strings.ToUpper(inner), "DISTINCT ") {
				inner = strings.TrimSpace(inner[9:])
			}
			seen := make(map[string]bool) // Use string key for map comparison
			var collected []interface{}

			// Check if inner is simple property access or general expression
			if strings.Contains(inner, ".") && !strings.HasPrefix(inner, "{") {
				parts := strings.SplitN(inner, ".", 2)
				varName := strings.TrimSpace(parts[0])
				propName := strings.TrimSpace(parts[1])

				for _, r := range rows {
					var node *storage.Node
					if strings.EqualFold(varName, sourceVar) {
						node = r.initialNode
					} else if strings.EqualFold(varName, targetVar) {
						node = r.relatedNode
					}
					if node != nil {
						if val, ok := node.Properties[propName]; ok {
							key := fmt.Sprintf("%v", val)
							if !seen[key] {
								seen[key] = true
								collected = append(collected, val)
							}
						}
					}
				}
			} else {
				// General expression (e.g., map literal): COLLECT(DISTINCT { key: value })
				for _, r := range rows {
					nodeCtx, relCtx := buildJoinedEvaluationContext(r, sourceVar, targetVar, relVar)
					val := e.evaluateExpressionWithContext(inner, nodeCtx, relCtx)
					if val != nil {
						key := fmt.Sprintf("%v", val)
						if !seen[key] {
							seen[key] = true
							collected = append(collected, val)
						}
					}
				}
			}
			// Apply suffix (e.g., [..10] for slicing) if present
			if suffix != "" {
				row[i] = e.applyArraySuffix(collected, suffix)
			} else {
				row[i] = collected
			}

		case strings.HasPrefix(upperExpr, "COLLECT("):
			// COLLECT(expression) - may have suffix like [..10]
			inner, suffix, _ := extractFuncArgsWithSuffix(item.expr, "collect")
			var collected []interface{}

			// Check if inner is simple property access or general expression
			if strings.Contains(inner, ".") && !strings.HasPrefix(inner, "{") {
				parts := strings.SplitN(inner, ".", 2)
				varName := strings.TrimSpace(parts[0])
				propName := strings.TrimSpace(parts[1])

				for _, r := range rows {
					var node *storage.Node
					if strings.EqualFold(varName, sourceVar) {
						node = r.initialNode
					} else if strings.EqualFold(varName, targetVar) {
						node = r.relatedNode
					}
					if node != nil {
						if val, ok := node.Properties[propName]; ok {
							collected = append(collected, val)
						}
					}
				}
			} else {
				// General expression (e.g., map literal): COLLECT({ key: value })
				for _, r := range rows {
					nodeCtx, relCtx := buildJoinedEvaluationContext(r, sourceVar, targetVar, relVar)
					val := e.evaluateExpressionWithContext(inner, nodeCtx, relCtx)
					if val != nil {
						collected = append(collected, val)
					}
				}
			}
			// Apply suffix (e.g., [..10] for slicing) if present
			if suffix != "" {
				row[i] = e.applyArraySuffix(collected, suffix)
			} else {
				row[i] = collected
			}

		default:
			if strings.Contains(item.expr, ".") {
				// Handle simple property access: seed.name, connected.property, etc.
				parts := strings.SplitN(item.expr, ".", 2)
				varName := strings.TrimSpace(parts[0])
				propName := strings.TrimSpace(parts[1])

				// Get value from first row (for aggregated queries, all rows have the same source node)
				for _, r := range rows {
					var node *storage.Node
					if strings.EqualFold(varName, sourceVar) {
						node = r.initialNode
					} else if strings.EqualFold(varName, targetVar) {
						node = r.relatedNode
					}
					if node != nil {
						row[i] = node.Properties[propName]
						break // Use first non-nil value
					}
				}
			} else {
				row[i] = nil
			}
		}
	}

	result.Rows = append(result.Rows, row)
	return result, nil
}

// buildJoinedResult builds a result from joined rows for simple RETURN
// If RETURN contains aggregation functions, delegates to processWithAggregation
func (e *StorageExecutor) buildJoinedResult(rows []joinedRow, sourceVar, targetVar, relVar, restOfQuery string) (*ExecuteResult, error) {
	returnIdx := findKeywordIndex(restOfQuery, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("RETURN clause required")
	}

	returnClause := stripTrailingReturnClauses(strings.TrimSpace(restOfQuery[returnIdx+6:]))
	returnItems := e.parseReturnItems(returnClause)

	// Check if any return item is an aggregation function
	hasAggregation := false
	for _, item := range returnItems {
		upperExpr := strings.ToUpper(item.expr)
		if strings.HasPrefix(upperExpr, "COUNT(") ||
			strings.HasPrefix(upperExpr, "SUM(") ||
			strings.HasPrefix(upperExpr, "AVG(") ||
			strings.HasPrefix(upperExpr, "MIN(") ||
			strings.HasPrefix(upperExpr, "MAX(") ||
			strings.HasPrefix(upperExpr, "COLLECT(") {
			hasAggregation = true
			break
		}
	}

	// If there's an aggregation, delegate to processWithAggregation
	if hasAggregation {
		return e.processWithAggregation(rows, sourceVar, targetVar, relVar, restOfQuery)
	}

	result := &ExecuteResult{
		Columns: make([]string, len(returnItems)),
		Rows:    make([][]interface{}, 0, len(rows)),
	}

	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	for _, joinedRow := range rows {
		row := make([]interface{}, len(returnItems))
		nodeCtx, relCtx := buildJoinedEvaluationContext(joinedRow, sourceVar, targetVar, relVar)
		for i, item := range returnItems {
			row[i] = e.evaluateExpressionWithContext(item.expr, nodeCtx, relCtx)
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// ========================================
// FOREACH Clause
// ========================================

// executeForeach handles FOREACH clause - iterate and perform updates
func (e *StorageExecutor) executeForeach(ctx context.Context, cypher string) (*ExecuteResult, error) {
	return e.executeForeachWithContext(ctx, cypher, make(map[string]*storage.Node), make(map[string]*storage.Edge))
}

// executeForeachWithContext executes a FOREACH clause with access to existing variable bindings.
//
// This is required for Neo4j-compatible patterns like:
//
//	OPTIONAL MATCH (a:TypeA {name: 'A1'})
//	FOREACH (x IN CASE WHEN a IS NOT NULL THEN [1] ELSE [] END | MERGE (e)-[:REL]->(a))
func (e *StorageExecutor) executeForeachWithContext(ctx context.Context, cypher string, nodeCtx map[string]*storage.Node, relCtx map[string]*storage.Edge) (*ExecuteResult, error) {
	foreachIdx := findKeywordIndex(cypher, "FOREACH")
	if foreachIdx == -1 {
		return nil, fmt.Errorf("FOREACH clause not found in query: %q", truncateQuery(cypher, 80))
	}

	parenStart := strings.Index(cypher[foreachIdx:], "(")
	if parenStart == -1 {
		return nil, fmt.Errorf("FOREACH requires parentheses (e.g., FOREACH (x IN list | SET ...))")
	}
	parenStart += foreachIdx

	depth := 1
	parenEnd := parenStart + 1
	for parenEnd < len(cypher) && depth > 0 {
		if cypher[parenEnd] == '(' {
			depth++
		} else if cypher[parenEnd] == ')' {
			depth--
		}
		parenEnd++
	}
	if depth != 0 {
		return nil, fmt.Errorf("FOREACH requires balanced parentheses")
	}

	inner := strings.TrimSpace(cypher[parenStart+1 : parenEnd-1])

	inIdx := strings.Index(strings.ToUpper(inner), " IN ")
	if inIdx == -1 {
		return nil, fmt.Errorf("FOREACH requires IN clause (e.g., FOREACH (x IN list | SET ...))")
	}

	variable := strings.TrimSpace(inner[:inIdx])
	remainder := strings.TrimSpace(inner[inIdx+4:])

	pipeIdx := strings.Index(remainder, "|")
	if pipeIdx == -1 {
		return nil, fmt.Errorf("FOREACH requires | separator")
	}

	listExpr := strings.TrimSpace(remainder[:pipeIdx])
	updateClause := strings.TrimSpace(remainder[pipeIdx+1:])

	list := e.evaluateExpressionWithContext(listExpr, nodeCtx, relCtx)

	var items []interface{}
	switch v := list.(type) {
	case []interface{}:
		items = v
	case nil:
		items = nil
	default:
		items = []interface{}{list}
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	for _, item := range items {
		substituted := strings.TrimSpace(e.replaceVariableInQuery(updateClause, variable, item))
		if substituted == "" {
			continue
		}

		upper := strings.ToUpper(substituted)
		var updateResult *ExecuteResult
		var err error

		switch {
		case strings.HasPrefix(upper, "MERGE"):
			updateResult, err = e.executeMergeWithContext(ctx, substituted, nodeCtx, relCtx)
		default:
			// Fallback: execute as standalone clause.
			// This supports simple CREATE/SET/REMOVE updates that don't depend on external bindings.
			updateResult, err = e.executeInternal(ctx, substituted, nil)
		}

		if err != nil {
			return nil, err
		}

		if updateResult != nil && updateResult.Stats != nil {
			result.Stats.NodesCreated += updateResult.Stats.NodesCreated
			result.Stats.PropertiesSet += updateResult.Stats.PropertiesSet
			result.Stats.RelationshipsCreated += updateResult.Stats.RelationshipsCreated
		}
	}

	// Support continuation after FOREACH, e.g.:
	// FOREACH (...) RETURN ...
	trailing := strings.TrimSpace(cypher[parenEnd:])
	if trailing != "" {
		after, err := e.executeInternal(ctx, trailing, nil)
		if err != nil {
			return nil, err
		}
		if after != nil {
			if after.Stats == nil {
				after.Stats = &QueryStats{}
			}
			after.Stats.NodesCreated += result.Stats.NodesCreated
			after.Stats.PropertiesSet += result.Stats.PropertiesSet
			after.Stats.RelationshipsCreated += result.Stats.RelationshipsCreated
			return after, nil
		}
	}

	return result, nil
}

// ========================================
// LOAD CSV Clause
// ========================================

// executeLoadCSV handles LOAD CSV clause
func (e *StorageExecutor) executeLoadCSV(ctx context.Context, cypher string) (*ExecuteResult, error) {
	return nil, fmt.Errorf("LOAD CSV is not supported in NornicDB embedded mode")
}

// ========================================
// Helper Functions
// ========================================

// replaceVariableInQuery replaces all occurrences of a variable with its value in a query.
func (e *StorageExecutor) replaceVariableInQuery(query string, variable string, value interface{}) string {
	result := query

	// Handle property access patterns first (variable.property)
	// For maps, replace variable.key with the actual value.
	if valueMap, ok := toStringAnyMap(value); ok {
		// Find all property access patterns
		for key, propVal := range valueMap {
			propValStr := e.valueToLiteral(propVal)
			pattern := variable + "." + key
			result = strings.ReplaceAll(result, pattern, propValStr)
			backtickedPattern := variable + ".`" + strings.ReplaceAll(key, "`", "``") + "`"
			result = strings.ReplaceAll(result, backtickedPattern, propValStr)
		}
		// Also handle standalone variable references (e.g. SET n = row).
		value = valueMap
	}

	// Convert to a Cypher literal so maps/lists remain valid expressions.
	valueStr := e.valueToLiteral(value)
	return replaceIdentifierOutsideQuotes(result, variable, valueStr)
}

// replaceVariableInMutationQuery substitutes UNWIND row variables in mutation queries.
// For map-shaped rows, standalone variable tokens inside WITH pipelines are collapsed to
// "{}" to avoid parser ambiguity from large inline map literals, while row.property tokens
// are still replaced with their concrete values.
func (e *StorageExecutor) replaceVariableInMutationQuery(query string, variable string, value interface{}) string {
	result := query
	valueStr := e.valueToLiteral(value)

	if valueMap, ok := toStringAnyMap(value); ok {
		for key, propVal := range valueMap {
			propValStr := e.valueToLiteral(propVal)
			pattern := variable + "." + key
			result = strings.ReplaceAll(result, pattern, propValStr)
			backtickedPattern := variable + ".`" + strings.ReplaceAll(key, "`", "``") + "`"
			result = strings.ReplaceAll(result, backtickedPattern, propValStr)
		}
		if findKeywordIndexInContext(query, "WITH") >= 0 {
			valueStr = "{}"
		}
	}

	return replaceIdentifierOutsideQuotes(result, variable, valueStr)
}

func replaceIdentifierOutsideQuotes(input string, ident string, replacement string) string {
	if ident == "" {
		return input
	}
	var b strings.Builder
	b.Grow(len(input) + 16)

	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(input); {
		ch := input[i]
		switch {
		case inSingle:
			b.WriteByte(ch)
			i++
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			b.WriteByte(ch)
			i++
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			b.WriteByte(ch)
			i++
			if ch == '`' {
				inBacktick = false
			}
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

		if !isIdentByte(ch) {
			b.WriteByte(ch)
			i++
			continue
		}
		start := i
		for i < len(input) && isIdentByte(input[i]) {
			i++
		}
		token := input[start:i]
		if token == ident && shouldReplaceIdentifierToken(input, start, i) {
			b.WriteString(replacement)
		} else {
			b.WriteString(token)
		}
	}
	return b.String()
}

func shouldReplaceIdentifierToken(input string, tokenStart int, tokenEnd int) bool {
	// Do not replace property tokens (n.name) or map keys ({name: ...}).
	prev := prevNonSpaceByte(input, tokenStart)
	if prev == '.' {
		return false
	}
	next := nextNonSpaceByte(input, tokenEnd)
	if next == ':' {
		return false
	}
	return true
}

func prevNonSpaceByte(s string, pos int) byte {
	for i := pos - 1; i >= 0; i-- {
		if !isASCIISpace(s[i]) {
			return s[i]
		}
	}
	return 0
}

func nextNonSpaceByte(s string, pos int) byte {
	for i := pos; i < len(s); i++ {
		if !isASCIISpace(s[i]) {
			return s[i]
		}
	}
	return 0
}

func toStringAnyMap(value interface{}) (map[string]interface{}, bool) {
	if m, ok := value.(map[string]interface{}); ok {
		return m, true
	}
	if m, ok := value.(map[interface{}]interface{}); ok {
		out := make(map[string]interface{}, len(m))
		for k, v := range m {
			ks, ok := k.(string)
			if !ok {
				return nil, false
			}
			out[ks] = v
		}
		return out, true
	}
	return nil, false
}
