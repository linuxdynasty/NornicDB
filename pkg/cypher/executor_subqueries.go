package cypher

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ===== CALL {} Subquery Support (Neo4j 4.0+) =====

// isCallSubquery detects if a query is a CALL {} subquery vs CALL procedure()
// CALL {} subqueries have "CALL" followed by optional whitespace and "{"
// CALL procedures have "CALL procedure.name()"
func isCallSubquery(cypher string) bool {
	// Use regex for flexible whitespace matching: CALL followed by optional whitespace and {
	return hasSubqueryPattern(cypher, callSubqueryRe)
}

// executeMatchWithCallProcedure handles MATCH ... CALL procedure() ... queries
// This allows procedure calls to use bound variables from the MATCH clause
// Example: MATCH (n:Node {id: 'n1'}) CALL db.index.vector.queryNodes('idx', 10, n.embedding) YIELD node, score
func (e *StorageExecutor) executeMatchWithCallProcedure(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters first
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	// Find CALL position
	callIdx := findKeywordIndex(cypher, "CALL")
	if callIdx == -1 {
		return nil, fmt.Errorf("CALL not found in query")
	}

	// Extract the MATCH part (before CALL)
	matchPart := strings.TrimSpace(cypher[:callIdx])
	matchIdx := findKeywordIndex(matchPart, "MATCH")
	if matchIdx == -1 {
		return nil, fmt.Errorf("MATCH not found before CALL")
	}

	// Extract the CALL part and everything after
	callPart := strings.TrimSpace(cypher[callIdx:])

	// Execute MATCH to get bound variables
	// We'll execute a modified MATCH query that returns all bound variables
	matchPattern := strings.TrimSpace(matchPart[matchIdx+5:]) // Skip "MATCH"

	// Parse WHERE clause if present
	whereIdx := findKeywordIndex(matchPattern, "WHERE")
	var whereClause string
	var patternOnly string
	if whereIdx > 0 {
		patternOnly = strings.TrimSpace(matchPattern[:whereIdx])
		whereClause = strings.TrimSpace(matchPattern[whereIdx+5:])
	} else {
		patternOnly = matchPattern
	}

	// Parse node pattern to get variable name
	nodePattern := e.parseNodePattern(patternOnly)
	if nodePattern.variable == "" {
		return nil, fmt.Errorf("could not parse node pattern: %s", patternOnly)
	}

	// Get matching nodes
	var nodes []*storage.Node
	var err error
	if len(nodePattern.labels) > 0 {
		nodes, err = e.storage.GetNodesByLabel(nodePattern.labels[0])
	} else {
		nodes, err = e.storage.AllNodes()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes: %w", err)
	}

	// Filter by properties
	if len(nodePattern.properties) > 0 {
		nodes = e.filterNodesByProperties(nodes, nodePattern.properties)
	}

	// Filter by WHERE clause
	if whereClause != "" {
		var filtered []*storage.Node
		for _, node := range nodes {
			if e.evaluateWhere(node, nodePattern.variable, whereClause) {
				filtered = append(filtered, node)
			}
		}
		nodes = filtered
	}

	// If no nodes match, return empty result
	if len(nodes) == 0 {
		// Determine result columns from YIELD clause or procedure type
		var columns []string
		yield := parseYieldClause(callPart)
		if yield != nil && len(yield.items) > 0 {
			// Extract column names from yield items (use alias if present, otherwise name)
			columns = make([]string, len(yield.items))
			for i, item := range yield.items {
				if item.alias != "" {
					columns[i] = item.alias
				} else {
					columns[i] = item.name
				}
			}
		} else {
			// Default columns for vector queries
			if strings.Contains(strings.ToUpper(callPart), "QUERYNODES") {
				columns = []string{"node", "score"}
			} else if strings.Contains(strings.ToUpper(callPart), "QUERYRELATIONSHIPS") {
				columns = []string{"relationship", "score"}
			} else {
				columns = []string{} // Empty if unknown
			}
		}
		return &ExecuteResult{
			Columns: columns,
			Rows:    [][]interface{}{},
		}, nil
	}

	// For each matching node, evaluate the CALL with bound variables
	var allResults []*ExecuteResult
	for _, node := range nodes {
		// Create variable context for this node
		nodeContext := map[string]*storage.Node{
			nodePattern.variable: node,
		}

		// Evaluate variable references in the CALL statement
		// Replace patterns like "n.embedding" with actual values
		evaluatedCall := e.substituteBoundVariablesInCall(callPart, nodeContext)

		// Execute the CALL with evaluated values
		result, err := e.executeCall(ctx, evaluatedCall)
		if err != nil {
			return nil, fmt.Errorf("failed to execute CALL for node %s: %w", node.ID, err)
		}
		if result != nil {
			allResults = append(allResults, result)
		}
	}

	// Combine results from all nodes
	if len(allResults) == 0 {
		// Determine result columns from YIELD clause or procedure type
		var columns []string
		yield := parseYieldClause(callPart)
		if yield != nil && len(yield.items) > 0 {
			// Extract column names from yield items (use alias if present, otherwise name)
			columns = make([]string, len(yield.items))
			for i, item := range yield.items {
				if item.alias != "" {
					columns[i] = item.alias
				} else {
					columns[i] = item.name
				}
			}
		} else {
			if strings.Contains(strings.ToUpper(callPart), "QUERYNODES") {
				columns = []string{"node", "score"}
			} else if strings.Contains(strings.ToUpper(callPart), "QUERYRELATIONSHIPS") {
				columns = []string{"relationship", "score"}
			} else {
				columns = []string{} // Empty if unknown
			}
		}
		return &ExecuteResult{
			Columns: columns,
			Rows:    [][]interface{}{},
		}, nil
	}

	// Merge all results
	combined := allResults[0]
	for i := 1; i < len(allResults); i++ {
		combined.Rows = append(combined.Rows, allResults[i].Rows...)
	}

	return combined, nil
}

// substituteBoundVariablesInCall replaces variable references in CALL statements with actual values
// Example: "CALL db.index.vector.queryNodes('idx', 10, n.embedding)" -> "CALL db.index.vector.queryNodes('idx', 10, [0.1, 0.2, ...])"
// This handles variable.property patterns like n.embedding, n.id, etc.
func (e *StorageExecutor) substituteBoundVariablesInCall(callPart string, nodeContext map[string]*storage.Node) string {
	result := callPart

	// Find all variable.property patterns in the CALL
	// Pattern: varName.propertyName (but not inside strings)
	// We need to be careful not to match patterns inside quoted strings
	varPattern := regexp.MustCompile(`(\w+)\.(\w+)`)
	matches := varPattern.FindAllStringSubmatchIndex(callPart, -1)

	// Process matches in reverse order to maintain indices
	for i := len(matches) - 1; i >= 0; i-- {
		match := matches[i]
		startIdx := match[0]
		endIdx := match[1]
		varName := callPart[match[2]:match[3]]
		propName := callPart[match[4]:match[5]]

		// Check if this match is inside a quoted string (skip if so)
		beforeMatch := callPart[:startIdx]
		singleQuotes := strings.Count(beforeMatch, "'") - strings.Count(beforeMatch, "\\'")
		doubleQuotes := strings.Count(beforeMatch, "\"") - strings.Count(beforeMatch, "\\\"")
		if singleQuotes%2 != 0 || doubleQuotes%2 != 0 {
			// Inside a quoted string - skip
			continue
		}

		// Check if this variable is in our context
		if node, exists := nodeContext[varName]; exists {
			// Evaluate the property access
			var value interface{}
			if propName == "embedding" {
				// Special handling for embedding - return the first chunk vector (always stored in ChunkEmbeddings)
				if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
					value = node.ChunkEmbeddings[0]
				} else if emb, ok := node.Properties["embedding"].([]float32); ok {
					value = emb
				} else if emb, ok := node.Properties["embedding"].([]float64); ok {
					// Convert []float64 to []float32
					emb32 := make([]float32, len(emb))
					for i, v := range emb {
						emb32[i] = float32(v)
					}
					value = emb32
				} else if emb, ok := node.Properties["embedding"].([]interface{}); ok {
					// Convert []interface{} to []float32
					emb32 := make([]float32, 0, len(emb))
					for _, item := range emb {
						switch v := item.(type) {
						case float32:
							emb32 = append(emb32, v)
						case float64:
							emb32 = append(emb32, float32(v))
						case int:
							emb32 = append(emb32, float32(v))
						case int64:
							emb32 = append(emb32, float32(v))
						}
					}
					value = emb32
				}
			} else {
				// Regular property access
				if val, ok := node.Properties[propName]; ok {
					value = val
				}
			}

			// Replace the variable.property with the actual value
			if value != nil {
				var replacement string
				switch v := value.(type) {
				case []float32:
					// Format as vector array
					parts := make([]string, len(v))
					for i, f := range v {
						parts[i] = fmt.Sprintf("%g", f)
					}
					replacement = "[" + strings.Join(parts, ", ") + "]"
				case []float64:
					// Format as vector array
					parts := make([]string, len(v))
					for i, f := range v {
						parts[i] = fmt.Sprintf("%g", f)
					}
					replacement = "[" + strings.Join(parts, ", ") + "]"
				case string:
					replacement = fmt.Sprintf("'%s'", v)
				case int, int64:
					replacement = fmt.Sprintf("%d", v)
				case float32, float64:
					replacement = fmt.Sprintf("%g", v)
				case bool:
					if v {
						replacement = "true"
					} else {
						replacement = "false"
					}
				default:
					// For complex types, try to convert to string representation
					replacement = fmt.Sprintf("%v", v)
				}
				// Replace from end to start to maintain indices
				result = result[:startIdx] + replacement + result[endIdx:]
			}
		}
	}

	return result
}

// executeMatchWithCallSubquery handles MATCH ... WHERE ... CALL { WITH var ... } ... RETURN queries
// This is a correlated subquery where the CALL {} references variables from the outer MATCH
func (e *StorageExecutor) executeMatchWithCallSubquery(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	// Find CALL position
	callIdx := findKeywordIndex(cypher, "CALL")
	if callIdx == -1 {
		return nil, fmt.Errorf("CALL not found in query")
	}

	// Extract the outer MATCH + WHERE part (before CALL)
	outerPart := strings.TrimSpace(cypher[:callIdx])

	// Parse the outer MATCH to get seed nodes
	// First, execute the outer query to get the seed nodes
	// We need to add a RETURN clause to get the variables
	matchIdx := findKeywordIndex(outerPart, "MATCH")
	if matchIdx == -1 {
		return nil, fmt.Errorf("MATCH not found before CALL")
	}

	// Extract the pattern and WHERE clause
	matchPart := strings.TrimSpace(outerPart[matchIdx+5:]) // Skip "MATCH"
	whereIdx := findKeywordIndex(matchPart, "WHERE")

	var nodePatternStr string
	var whereClause string
	if whereIdx > 0 {
		nodePatternStr = strings.TrimSpace(matchPart[:whereIdx])
		whereClause = strings.TrimSpace(matchPart[whereIdx+5:]) // Skip "WHERE"
	} else {
		nodePatternStr = matchPart
	}

	// Parse node pattern to get variable name
	nodePattern := e.parseNodePattern(nodePatternStr)
	if nodePattern.variable == "" {
		return nil, fmt.Errorf("could not parse node pattern: %s", nodePatternStr)
	}

	// Get matching nodes
	var seedNodes []*storage.Node
	var err error
	if len(nodePattern.labels) > 0 {
		seedNodes, err = e.storage.GetNodesByLabel(nodePattern.labels[0])
	} else {
		seedNodes, err = e.storage.AllNodes()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get seed nodes: %w", err)
	}

	// Filter by properties
	if len(nodePattern.properties) > 0 {
		seedNodes = e.filterNodesByProperties(seedNodes, nodePattern.properties)
	}

	// Filter by WHERE clause
	if whereClause != "" {
		seedNodes = e.filterNodesByWhereClause(seedNodes, whereClause, nodePattern.variable)
	}

	if len(seedNodes) == 0 {
		// No seed nodes found - return empty result
		return &ExecuteResult{
			Columns: []string{nodePattern.variable, "neighbors"},
			Rows:    [][]interface{}{},
		}, nil
	}

	// Parse the CALL {} subquery and what comes after
	callPart := strings.TrimSpace(cypher[callIdx:])
	subqueryBody, afterCall, _, _ := e.parseCallSubquery(callPart)
	if subqueryBody == "" {
		return nil, fmt.Errorf("invalid CALL {} subquery: empty body")
	}

	// Check if subquery starts with "WITH <variable>" - this imports outer context
	upperBody := strings.ToUpper(strings.TrimSpace(subqueryBody))
	if !strings.HasPrefix(upperBody, "WITH ") {
		// No WITH clause - execute as standalone subquery for each seed
		return e.executeCallSubquery(ctx, callPart)
	}

	// Find where the WITH clause ends (at MATCH or RETURN)
	withEndIdx := findKeywordIndex(subqueryBody, "MATCH")
	if withEndIdx == -1 {
		withEndIdx = findKeywordIndex(subqueryBody, "RETURN")
	}
	if withEndIdx == -1 {
		withEndIdx = len(subqueryBody)
	}

	// Execute the subquery for each seed node
	var combinedResult *ExecuteResult

	// Use a unique parameter name to avoid collision with user-provided parameters
	seedIDParamName := "__internal_seed_id"

	for _, seedNode := range seedNodes {
		seedID := string(seedNode.ID)

		// Transform the inner query to bind the seed variable properly
		// Original: "WITH seed MATCH path = (seed)-[r*1..2]-(connected) RETURN seed, collect(...)"
		// We need to replace the WITH clause with an explicit seed binding
		// SECURITY: Use parameterized query to prevent Cypher injection

		// Extract the rest after WITH clause (starts with MATCH or RETURN)
		restOfSubquery := strings.TrimSpace(subqueryBody[withEndIdx:])

		// Create parameters map with the seed ID (safe from injection)
		subqueryParams := map[string]interface{}{
			seedIDParamName: seedID,
		}

		// If the rest starts with MATCH, we need to handle the path pattern
		// Replace "MATCH path = (seed)" with "MATCH path = (seed) WHERE id(seed) = $param"
		if strings.HasPrefix(strings.ToUpper(restOfSubquery), "MATCH") {
			// Find the existing WHERE or RETURN to know where to inject our filter
			matchPart := restOfSubquery[5:] // Skip "MATCH"
			returnIdx := findKeywordIndex(matchPart, "RETURN")

			if returnIdx > 0 {
				patternPart := strings.TrimSpace(matchPart[:returnIdx])
				returnPart := matchPart[returnIdx:]

				// Check if there's already a WHERE clause in patternPart
				whereIdx := findKeywordNotInBrackets(strings.ToUpper(patternPart), " WHERE ")
				var substitutedBody string
				seedFilter := "id(" + nodePattern.variable + ") = $" + seedIDParamName

				if whereIdx > 0 {
					// There's already a WHERE clause - append with AND
					beforeWhere := patternPart[:whereIdx]
					afterWhere := patternPart[whereIdx+7:] // Skip " WHERE "
					substitutedBody = "MATCH " + beforeWhere + " WHERE " + seedFilter + " AND " + afterWhere + " " + returnPart
				} else {
					// No existing WHERE - add one
					substitutedBody = "MATCH " + patternPart + " WHERE " + seedFilter + " " + returnPart
				}

				// Execute the substituted subquery with parameters
				innerResult, err := e.executeInternal(ctx, substitutedBody, subqueryParams)
				if err != nil {
					// Log but continue with other seeds
					continue
				}

				if combinedResult == nil {
					combinedResult = &ExecuteResult{
						Columns: innerResult.Columns,
						Rows:    make([][]interface{}, 0),
					}
				}

				// Add rows from this seed's result
				combinedResult.Rows = append(combinedResult.Rows, innerResult.Rows...)
				continue
			}
		}

		// Fallback: try the WITH chaining approach (parameterized - safe from injection)
		substitutedBody := "MATCH (" + nodePattern.variable + ") WHERE id(" + nodePattern.variable + ") = $" + seedIDParamName + " WITH " + nodePattern.variable + " " + restOfSubquery

		// Execute the substituted subquery with parameters
		innerResult, err := e.executeInternal(ctx, substitutedBody, subqueryParams)
		if err != nil {
			// Log but continue with other seeds
			continue
		}

		if combinedResult == nil {
			combinedResult = &ExecuteResult{
				Columns: innerResult.Columns,
				Rows:    make([][]interface{}, 0),
			}
		}

		// Add rows from this seed's result, injecting the seed node for variables that reference it
		for _, row := range innerResult.Rows {
			newRow := make([]interface{}, len(row))
			copy(newRow, row)

			// Find columns that match the seed variable and inject the seed node
			// The variable name from outer MATCH should match a column in the inner RETURN
			seedVarLower := strings.ToLower(nodePattern.variable)
			for colIdx, colName := range innerResult.Columns {
				colNameLower := strings.ToLower(strings.TrimSpace(colName))
				if colNameLower == seedVarLower && newRow[colIdx] == nil {
					// Inject the seed node as a map representation
					newRow[colIdx] = seedNode
				}
			}
			combinedResult.Rows = append(combinedResult.Rows, newRow)
		}

		// If inner query returned 0 rows but we have a seed, create a row with just the seed
		if len(innerResult.Rows) == 0 {
			// Create a row with the seed node and empty neighbors
			emptyRow := make([]interface{}, len(innerResult.Columns))
			for colIdx, colName := range innerResult.Columns {
				if strings.EqualFold(colName, nodePattern.variable) {
					emptyRow[colIdx] = seedNode
				}
			}
			combinedResult.Rows = append(combinedResult.Rows, emptyRow)
		}
	}

	if combinedResult == nil {
		combinedResult = &ExecuteResult{
			Columns: []string{},
			Rows:    [][]interface{}{},
		}
	}

	// If there's something after CALL { }, process it (e.g., RETURN)
	if afterCall != "" {
		return e.processAfterCallSubquery(ctx, combinedResult, afterCall)
	}

	return combinedResult, nil
}

// executeCallSubquery executes a CALL {} subquery
// Syntax: CALL { <subquery> } [IN TRANSACTIONS [OF n ROWS]]
// The subquery can contain MATCH, CREATE, RETURN, UNION, etc.
func (e *StorageExecutor) executeCallSubquery(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	// Extract the subquery body from CALL { ... }
	subqueryBody, afterCall, inTransactions, batchSize := e.parseCallSubquery(cypher)
	if subqueryBody == "" {
		return nil, fmt.Errorf("invalid CALL {} subquery: empty body (expected CALL { <query> })")
	}

	// Execute the inner subquery
	var innerResult *ExecuteResult
	var err error

	if inTransactions {
		// Execute in batches (for large data operations)
		innerResult, err = e.executeCallInTransactions(ctx, subqueryBody, batchSize)
	} else {
		// Check if subquery contains UNION - route to executeUnion if so
		// This must be checked before calling Execute, as Execute routes based on first keyword
		if findKeywordIndex(subqueryBody, "UNION ALL") >= 0 {
			innerResult, err = e.executeUnion(ctx, subqueryBody, true)
		} else if findKeywordIndex(subqueryBody, "UNION") >= 0 {
			innerResult, err = e.executeUnion(ctx, subqueryBody, false)
		} else {
			// Execute as single query
			innerResult, err = e.executeInternal(ctx, subqueryBody, nil)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("CALL subquery error: %w", err)
	}

	// If there's something after CALL { }, process it (e.g., RETURN)
	if afterCall != "" {
		return e.processAfterCallSubquery(ctx, innerResult, afterCall)
	}

	return innerResult, nil
}

// parseCallSubquery extracts the body from CALL { ... } and any trailing clauses
// Returns: body, afterCall, inTransactions bool, batchSize int
func (e *StorageExecutor) parseCallSubquery(cypher string) (body, afterCall string, inTransactions bool, batchSize int) {
	batchSize = 1000 // Default batch size

	trimmed := strings.TrimSpace(cypher)

	// Find the opening brace
	braceStart := strings.Index(trimmed, "{")
	if braceStart == -1 {
		return "", "", false, batchSize
	}

	// Find matching closing brace
	depth := 0
	braceEnd := -1
	for i := braceStart; i < len(trimmed); i++ {
		if trimmed[i] == '{' {
			depth++
		} else if trimmed[i] == '}' {
			depth--
			if depth == 0 {
				braceEnd = i
				break
			}
		}
	}

	if braceEnd == -1 {
		return "", "", false, batchSize
	}

	// Extract body (between braces)
	body = strings.TrimSpace(trimmed[braceStart+1 : braceEnd])

	// Get what's after the closing brace
	afterCall = strings.TrimSpace(trimmed[braceEnd+1:])

	// Check for IN TRANSACTIONS
	upperAfter := strings.ToUpper(afterCall)
	if strings.HasPrefix(upperAfter, "IN TRANSACTIONS") {
		inTransactions = true
		afterTx := strings.TrimSpace(afterCall[15:])
		upperAfterTx := strings.ToUpper(afterTx)

		// Check for OF n ROWS
		if strings.HasPrefix(upperAfterTx, "OF ") {
			// Parse batch size
			ofPart := afterTx[3:]
			// Find ROWS keyword
			rowsIdx := strings.Index(strings.ToUpper(ofPart), " ROWS")
			if rowsIdx > 0 {
				sizeStr := strings.TrimSpace(ofPart[:rowsIdx])
				if size, err := strconv.Atoi(sizeStr); err == nil && size > 0 {
					batchSize = size
				}
				afterCall = strings.TrimSpace(ofPart[rowsIdx+5:])
			} else {
				afterCall = ""
			}
		} else {
			afterCall = afterTx
		}
	}

	return body, afterCall, inTransactions, batchSize
}

// executeCallInTransactions executes a CALL {} IN TRANSACTIONS query
// This batches operations for large datasets by processing results in separate transactions.
//
// The subquery is executed in batches where each batch is processed in its own transaction.
// This is useful for large imports/updates to avoid memory issues and provide transaction boundaries.
//
// Example:
//
//	CALL {
//	  MATCH (p:Person)
//	  SET p.processed = true
//	  RETURN p.name AS name
//	} IN TRANSACTIONS OF 2 ROWS
//
// This will process Person nodes in batches of 2, each batch in a separate transaction.
//
// Strategy:
//  1. First execute the subquery to determine the total number of rows (read-only)
//  2. If it contains write operations, process in batches by adding LIMIT/SKIP to the MATCH
//  3. Each batch is executed in its own transaction via executeWithImplicitTransaction
func (e *StorageExecutor) executeCallInTransactions(ctx context.Context, subquery string, batchSize int) (*ExecuteResult, error) {
	if batchSize <= 0 {
		batchSize = 1000 // Default batch size
	}

	// Check if the subquery contains write operations (CREATE, SET, DELETE, MERGE)
	upperSubquery := strings.ToUpper(subquery)
	hasWrites := strings.Contains(upperSubquery, "CREATE") ||
		strings.Contains(upperSubquery, "SET") ||
		strings.Contains(upperSubquery, "DELETE") ||
		strings.Contains(upperSubquery, "MERGE")

	if !hasWrites {
		// No write operations - execute once and return (no need for batching)
		result, err := e.executeInternal(ctx, subquery, nil)
		if err != nil {
			return nil, fmt.Errorf("subquery execution failed: %w", err)
		}
		return result, nil
	}

	// For write operations, we need to batch the execution
	// Strategy: Add LIMIT/SKIP to the MATCH part (before write operations) to process in batches
	// We'll execute the subquery multiple times, each time with different LIMIT/SKIP values
	// Each execution will be in its own transaction

	// First, try to get a row count estimate by executing a read-only version
	// This helps us determine how many batches we need
	readOnlyQuery := e.makeSubqueryReadOnly(subquery)
	var totalRows int
	var resultColumns []string

	if readOnlyQuery != "" {
		// Execute read-only version to get row count (doesn't perform writes)
		readOnlyResult, err := e.executeInternal(ctx, readOnlyQuery, nil)
		if err == nil && readOnlyResult != nil {
			totalRows = len(readOnlyResult.Rows)
			resultColumns = readOnlyResult.Columns
		}
	}

	// If we couldn't get a row count, we'll need to process until we get no more results
	// This is less efficient but handles edge cases
	useIterativeBatching := totalRows == 0

	// Guard: write queries without a safely batchable MATCH row source (e.g. bare CREATE
	// or UNWIND-driven writes) cannot reliably make forward progress with our SKIP/LIMIT
	// pagination rewrite and may loop indefinitely. Execute once instead.
	if useIterativeBatching {
		hasBatchableSource := strings.Contains(upperSubquery, "MATCH ")
		if !hasBatchableSource {
			singleResult, err := e.executeWithImplicitTransaction(ctx, subquery, strings.ToUpper(subquery))
			if err != nil {
				return nil, fmt.Errorf("batch 1 failed: %w", err)
			}
			return singleResult, nil
		}
	}

	// Combined result
	combinedResult := &ExecuteResult{
		Columns: resultColumns,
		Rows:    make([][]interface{}, 0),
		Stats:   &QueryStats{},
	}

	if useIterativeBatching {
		// Iterative batching: process batches until we get no results
		batchNum := 0
		for {
			skip := batchNum * batchSize
			limit := batchSize

			// Create a modified subquery with LIMIT and SKIP to process this batch
			modifiedSubquery := e.addLimitSkipToSubquery(subquery, limit, skip)

			// Execute this batch in its own transaction
			batchResult, err := e.executeWithImplicitTransaction(ctx, modifiedSubquery, strings.ToUpper(modifiedSubquery))
			if err != nil {
				// On error, stop processing and return error
				return nil, fmt.Errorf("batch %d failed: %w", batchNum+1, err)
			}

			// If no results, we're done
			if batchResult == nil || len(batchResult.Rows) == 0 {
				break
			}

			// Set columns from first batch if not set
			if len(combinedResult.Columns) == 0 && len(batchResult.Columns) > 0 {
				combinedResult.Columns = batchResult.Columns
			}

			// Accumulate results
			combinedResult.Rows = append(combinedResult.Rows, batchResult.Rows...)
			if batchResult.Stats != nil {
				combinedResult.Stats.NodesCreated += batchResult.Stats.NodesCreated
				combinedResult.Stats.NodesDeleted += batchResult.Stats.NodesDeleted
				combinedResult.Stats.RelationshipsCreated += batchResult.Stats.RelationshipsCreated
				combinedResult.Stats.RelationshipsDeleted += batchResult.Stats.RelationshipsDeleted
				combinedResult.Stats.PropertiesSet += batchResult.Stats.PropertiesSet
				combinedResult.Stats.LabelsAdded += batchResult.Stats.LabelsAdded
			}

			// If we got fewer rows than the batch size, we're done
			if len(batchResult.Rows) < batchSize {
				break
			}

			batchNum++
		}
	} else {
		// Known row count: process exact number of batches
		// Calculate number of batches
		numBatches := (totalRows + batchSize - 1) / batchSize

		// Process each batch in a separate transaction
		for batchNum := 0; batchNum < numBatches; batchNum++ {
			skip := batchNum * batchSize
			limit := batchSize

			// Create a modified subquery with LIMIT and SKIP to process this batch
			modifiedSubquery := e.addLimitSkipToSubquery(subquery, limit, skip)

			// Execute this batch in its own transaction
			batchResult, err := e.executeWithImplicitTransaction(ctx, modifiedSubquery, strings.ToUpper(modifiedSubquery))
			if err != nil {
				// On error, stop processing and return error
				return nil, fmt.Errorf("batch %d/%d failed: %w", batchNum+1, numBatches, err)
			}

			// Set columns from first batch if not set
			if len(combinedResult.Columns) == 0 && batchResult != nil && len(batchResult.Columns) > 0 {
				combinedResult.Columns = batchResult.Columns
			}

			// Accumulate results
			if batchResult != nil {
				combinedResult.Rows = append(combinedResult.Rows, batchResult.Rows...)
				if batchResult.Stats != nil {
					combinedResult.Stats.NodesCreated += batchResult.Stats.NodesCreated
					combinedResult.Stats.NodesDeleted += batchResult.Stats.NodesDeleted
					combinedResult.Stats.RelationshipsCreated += batchResult.Stats.RelationshipsCreated
					combinedResult.Stats.RelationshipsDeleted += batchResult.Stats.RelationshipsDeleted
					combinedResult.Stats.PropertiesSet += batchResult.Stats.PropertiesSet
					combinedResult.Stats.LabelsAdded += batchResult.Stats.LabelsAdded
				}
			}
		}
	}

	return combinedResult, nil
}

// makeSubqueryReadOnly converts a subquery with writes to a read-only version for row counting.
// This is used to determine how many batches we need before executing the actual writes.
// Returns empty string if conversion is not possible.
func (e *StorageExecutor) makeSubqueryReadOnly(subquery string) string {
	upper := strings.ToUpper(subquery)

	// Simple strategy: Replace write operations with RETURN of matched entities
	// This works for common patterns like "MATCH ... SET ... RETURN"

	// Check for MATCH ... SET ... RETURN pattern
	matchIdx := strings.Index(upper, "MATCH")
	setIdx := strings.Index(upper, "SET")
	returnIdx := strings.Index(upper, "RETURN")

	if matchIdx >= 0 && setIdx > matchIdx && returnIdx > setIdx {
		// Extract MATCH and RETURN parts, skip SET
		matchPart := strings.TrimSpace(subquery[matchIdx:setIdx])
		returnPart := strings.TrimSpace(subquery[returnIdx:])
		return matchPart + " " + returnPart
	}

	// Check for MATCH ... CREATE ... RETURN pattern
	createIdx := strings.Index(upper, "CREATE")
	if matchIdx >= 0 && createIdx > matchIdx && returnIdx > createIdx {
		// Extract MATCH and RETURN parts, skip CREATE
		matchPart := strings.TrimSpace(subquery[matchIdx:createIdx])
		returnPart := strings.TrimSpace(subquery[returnIdx:])
		return matchPart + " " + returnPart
	}

	// If we can't convert, return empty string (caller will use iterative batching)
	return ""
}

// addLimitSkipToSubquery adds LIMIT and SKIP clauses to a subquery for batching.
// For queries with MATCH followed by write operations (SET, CREATE, DELETE, MERGE),
// it adds LIMIT/SKIP after the MATCH clause to limit how many rows are processed.
// For other patterns, it adds LIMIT/SKIP before RETURN.
//
// This ensures that batching limits the number of matched rows processed, not just
// the number of returned rows.
func (e *StorageExecutor) addLimitSkipToSubquery(subquery string, limit, skip int) string {
	upper := strings.ToUpper(subquery)

	// Check for MATCH ... SET/CREATE/DELETE/MERGE ... RETURN pattern
	// For these, we want to add LIMIT/SKIP after MATCH to limit how many rows are processed
	matchIdx := strings.Index(upper, "MATCH")
	if matchIdx >= 0 {
		// Find the first operation after MATCH (SET, CREATE, DELETE, MERGE, or RETURN)
		remaining := upper[matchIdx+5:] // Skip "MATCH"
		setIdx := strings.Index(remaining, "SET")
		createIdx := strings.Index(remaining, "CREATE")
		deleteIdx := strings.Index(remaining, "DELETE")
		mergeIdx := strings.Index(remaining, "MERGE")
		returnIdx := strings.Index(remaining, "RETURN")

		// Find the earliest operation after MATCH
		firstOpIdx := -1
		var firstOpName string
		if setIdx >= 0 && (firstOpIdx == -1 || setIdx < firstOpIdx) {
			firstOpIdx = setIdx
			firstOpName = "SET"
		}
		if createIdx >= 0 && (firstOpIdx == -1 || createIdx < firstOpIdx) {
			firstOpIdx = createIdx
			firstOpName = "CREATE"
		}
		if deleteIdx >= 0 && (firstOpIdx == -1 || deleteIdx < firstOpIdx) {
			firstOpIdx = deleteIdx
			firstOpName = "DELETE"
		}
		if mergeIdx >= 0 && (firstOpIdx == -1 || mergeIdx < firstOpIdx) {
			firstOpIdx = mergeIdx
			firstOpName = "MERGE"
		}
		if returnIdx >= 0 && (firstOpIdx == -1 || returnIdx < firstOpIdx) {
			firstOpIdx = returnIdx
			firstOpName = "RETURN"
		}

		if firstOpIdx > 0 {
			// We need to find where the MATCH clause ends
			// The MATCH clause can include WHERE, so we need to find the end of the pattern
			matchEnd := matchIdx + 5 + firstOpIdx // End of MATCH pattern, start of first operation

			// Check if there's a WHERE clause between MATCH and the first operation
			whereIdx := strings.Index(upper[matchIdx+5:matchIdx+5+firstOpIdx], "WHERE")
			if whereIdx >= 0 {
				// Find end of WHERE clause (before first operation)
				whereEnd := strings.Index(upper[matchIdx+5+whereIdx:matchIdx+5+firstOpIdx], " "+firstOpName)
				if whereEnd > 0 {
					matchEnd = matchIdx + 5 + whereIdx + 5 + whereEnd // After WHERE clause
				}
			}

			// Extract the MATCH part
			matchPart := strings.TrimSpace(subquery[:matchEnd])
			afterOp := subquery[matchEnd:]

			// Extract variable name from MATCH pattern (e.g., "MATCH (s:Source)" -> "s")
			varNames := e.extractVariableNamesFromPattern(matchPart[5:]) // Skip "MATCH"
			varName := "n"                                               // Default fallback
			if len(varNames) > 0 {
				varName = varNames[0]
			}

			// Use WITH clause to apply LIMIT/SKIP (Cypher doesn't allow LIMIT directly after MATCH)
			// Format: MATCH ... WITH var SKIP n LIMIT m CREATE/SET...
			if skip > 0 {
				return matchPart + fmt.Sprintf(" WITH %s SKIP %d LIMIT %d ", varName, skip, limit) + afterOp
			}
			return matchPart + fmt.Sprintf(" WITH %s LIMIT %d ", varName, limit) + afterOp
		}
	}

	// Fallback: Add LIMIT/SKIP before RETURN (or at end if no RETURN)
	returnIdx := strings.LastIndex(upper, "RETURN")
	if returnIdx == -1 {
		// No RETURN clause - append LIMIT/SKIP at the end
		if skip > 0 {
			return subquery + fmt.Sprintf(" SKIP %d LIMIT %d", skip, limit)
		}
		return subquery + fmt.Sprintf(" LIMIT %d", limit)
	}

	// Find where the RETURN clause starts in the original query
	returnPart := subquery[returnIdx:]

	// Check if LIMIT or SKIP already exists
	if strings.Contains(strings.ToUpper(returnPart), "LIMIT") || strings.Contains(strings.ToUpper(returnPart), "SKIP") {
		// LIMIT/SKIP already present - append (may cause issues but handles common cases)
		if skip > 0 {
			return subquery + fmt.Sprintf(" SKIP %d LIMIT %d", skip, limit)
		}
		return subquery + fmt.Sprintf(" LIMIT %d", limit)
	}

	// Insert SKIP and LIMIT before RETURN
	beforeReturn := strings.TrimSpace(subquery[:returnIdx])
	returnClause := subquery[returnIdx:]

	if skip > 0 {
		return beforeReturn + fmt.Sprintf(" SKIP %d LIMIT %d ", skip, limit) + returnClause
	}
	return beforeReturn + fmt.Sprintf(" LIMIT %d ", limit) + returnClause
}

// processAfterCallSubquery handles clauses after CALL { } like RETURN
func (e *StorageExecutor) processAfterCallSubquery(ctx context.Context, innerResult *ExecuteResult, afterCall string) (*ExecuteResult, error) {
	upperAfter := strings.ToUpper(afterCall)

	// Handle RETURN clause
	if strings.HasPrefix(upperAfter, "RETURN ") {
		return e.processCallSubqueryReturn(innerResult, afterCall)
	}

	// Handle ORDER BY (without RETURN means use inner result's columns)
	if strings.HasPrefix(upperAfter, "ORDER BY ") {
		result := e.applyOrderByToResult(innerResult, afterCall)
		// Check for LIMIT/SKIP after ORDER BY
		return e.applyResultModifiers(result, afterCall)
	}

	// Unsupported clause after CALL {}
	firstWord := strings.Split(upperAfter, " ")[0]
	return nil, fmt.Errorf("unsupported clause after CALL {}: %s (supported: RETURN, ORDER BY, SKIP, LIMIT)", firstWord)
}

// processCallSubqueryReturn processes the RETURN clause after CALL {}
func (e *StorageExecutor) processCallSubqueryReturn(innerResult *ExecuteResult, afterCall string) (*ExecuteResult, error) {
	// Parse RETURN expressions
	returnIdx := findKeywordIndex(afterCall, "RETURN")
	if returnIdx == -1 {
		return innerResult, nil
	}

	returnClause := strings.TrimSpace(afterCall[returnIdx+6:])

	// Check for ORDER BY, LIMIT, SKIP
	orderByIdx := findKeywordIndex(returnClause, "ORDER BY")
	limitIdx := findKeywordIndex(returnClause, "LIMIT")
	skipIdx := findKeywordIndex(returnClause, "SKIP")

	// Find the earliest modifier
	modifierIdx := len(returnClause)
	if orderByIdx != -1 && orderByIdx < modifierIdx {
		modifierIdx = orderByIdx
	}
	if limitIdx != -1 && limitIdx < modifierIdx {
		modifierIdx = limitIdx
	}
	if skipIdx != -1 && skipIdx < modifierIdx {
		modifierIdx = skipIdx
	}

	returnExprs := strings.TrimSpace(returnClause[:modifierIdx])
	modifierClause := ""
	if modifierIdx < len(returnClause) {
		modifierClause = returnClause[modifierIdx:]
	}

	// Parse return expressions
	parts := splitReturnExpressions(returnExprs)

	// Build column mapping from inner result
	colMap := make(map[string]int)
	for i, col := range innerResult.Columns {
		colMap[col] = i
	}

	// Check if RETURN clause has aggregation functions
	hasAggregation := false
	for _, part := range parts {
		if containsAggregateFunc(part) {
			hasAggregation = true
			break
		}
	}

	if hasAggregation {
		// Handle aggregation - aggregate all rows into one
		newColumns := make([]string, len(parts))
		resultRow := make([]interface{}, len(parts))

		for i, part := range parts {
			part = strings.TrimSpace(part)

			// Check for alias
			alias := part
			expr := part
			upperPart := strings.ToUpper(part)
			if asIdx := strings.Index(upperPart, " AS "); asIdx != -1 {
				alias = strings.TrimSpace(part[asIdx+4:])
				expr = strings.TrimSpace(part[:asIdx])
			}

			newColumns[i] = alias

			if containsAggregateFunc(expr) {
				// Handle aggregation functions
				inner := extractFuncInner(expr)

				if isAggregateFuncName(expr, "collect") {
					// Handle COLLECT (with or without DISTINCT)
					upperInner := strings.ToUpper(inner)
					isDistinct := strings.HasPrefix(upperInner, "DISTINCT ")
					collectExpr := inner
					if isDistinct {
						collectExpr = strings.TrimSpace(inner[9:])
					}

					seen := make(map[string]bool)
					var collected []interface{}
					for _, row := range innerResult.Rows {
						// Build a values map from the row
						values := make(map[string]interface{})
						for j, col := range innerResult.Columns {
							if j < len(row) {
								values[col] = row[j]
							}
						}
						val := e.evaluateExpressionFromValues(collectExpr, values)
						if isDistinct {
							key := fmt.Sprintf("%v", val)
							if !seen[key] {
								seen[key] = true
								collected = append(collected, val)
							}
						} else {
							collected = append(collected, val)
						}
					}
					resultRow[i] = collected
				} else if isAggregateFuncName(expr, "count") {
					if inner == "*" {
						resultRow[i] = int64(len(innerResult.Rows))
					} else {
						count := int64(0)
						for _, row := range innerResult.Rows {
							if idx, ok := colMap[inner]; ok && idx < len(row) && row[idx] != nil {
								count++
							}
						}
						resultRow[i] = count
					}
				} else if isAggregateFuncName(expr, "sum") {
					sum := float64(0)
					for _, row := range innerResult.Rows {
						if idx, ok := colMap[inner]; ok && idx < len(row) {
							if num, ok := toFloat64(row[idx]); ok {
								sum += num
							}
						}
					}
					resultRow[i] = sum
				} else if isAggregateFuncName(expr, "avg") {
					sum := float64(0)
					count := 0
					for _, row := range innerResult.Rows {
						if idx, ok := colMap[inner]; ok && idx < len(row) {
							if num, ok := toFloat64(row[idx]); ok {
								sum += num
								count++
							}
						}
					}
					if count > 0 {
						resultRow[i] = sum / float64(count)
					}
				} else if isAggregateFuncName(expr, "min") {
					var minVal interface{}
					for _, row := range innerResult.Rows {
						if idx, ok := colMap[inner]; ok && idx < len(row) {
							val := row[idx]
							if val != nil && (minVal == nil || e.compareOrderValues(val, minVal) < 0) {
								minVal = val
							}
						}
					}
					resultRow[i] = minVal
				} else if isAggregateFuncName(expr, "max") {
					var maxVal interface{}
					for _, row := range innerResult.Rows {
						if idx, ok := colMap[inner]; ok && idx < len(row) {
							val := row[idx]
							if val != nil && (maxVal == nil || e.compareOrderValues(val, maxVal) > 0) {
								maxVal = val
							}
						}
					}
					resultRow[i] = maxVal
				}
			} else {
				// Non-aggregated column - use value from first row
				if len(innerResult.Rows) > 0 {
					if idx, ok := colMap[expr]; ok && idx < len(innerResult.Rows[0]) {
						resultRow[i] = innerResult.Rows[0][idx]
					}
				}
			}
		}

		result := &ExecuteResult{
			Columns: newColumns,
			Rows:    [][]interface{}{resultRow},
			Stats:   innerResult.Stats,
		}

		// Apply modifiers (ORDER BY, LIMIT, SKIP)
		if modifierClause != "" {
			return e.applyResultModifiers(result, modifierClause)
		}
		return result, nil
	}

	// No aggregation - Project columns
	newColumns := make([]string, 0, len(parts))
	colIndices := make([]int, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Check for alias
		alias := part
		expr := part
		upperPart := strings.ToUpper(part)
		if asIdx := strings.Index(upperPart, " AS "); asIdx != -1 {
			alias = strings.TrimSpace(part[asIdx+4:])
			expr = strings.TrimSpace(part[:asIdx])
		}

		newColumns = append(newColumns, alias)

		// Find column index
		if idx, ok := colMap[expr]; ok {
			colIndices = append(colIndices, idx)
		} else {
			// Not found in inner result, append -1 (will be nil)
			colIndices = append(colIndices, -1)
		}
	}

	// Project rows
	newRows := make([][]interface{}, 0, len(innerResult.Rows))
	for _, row := range innerResult.Rows {
		newRow := make([]interface{}, len(colIndices))
		for i, idx := range colIndices {
			if idx >= 0 && idx < len(row) {
				newRow[i] = row[idx]
			} else {
				newRow[i] = nil
			}
		}
		newRows = append(newRows, newRow)
	}

	result := &ExecuteResult{
		Columns: newColumns,
		Rows:    newRows,
		Stats:   innerResult.Stats,
	}

	// Apply modifiers (ORDER BY, LIMIT, SKIP)
	if modifierClause != "" {
		return e.applyResultModifiers(result, modifierClause)
	}

	return result, nil
}

// applyResultModifiers applies ORDER BY, LIMIT, SKIP to a result
func (e *StorageExecutor) applyResultModifiers(result *ExecuteResult, modifiers string) (*ExecuteResult, error) {
	// Apply ORDER BY
	if orderByIdx := findKeywordIndex(modifiers, "ORDER BY"); orderByIdx != -1 {
		result = e.applyOrderByToResult(result, modifiers[orderByIdx:])
	}

	// Apply SKIP
	if skipIdx := findKeywordIndex(modifiers, "SKIP"); skipIdx != -1 {
		skipPart := strings.TrimSpace(modifiers[skipIdx+4:])
		// Find next keyword
		nextKw := len(skipPart)
		for _, kw := range []string{" LIMIT", " ORDER"} {
			if idx := strings.Index(strings.ToUpper(skipPart), kw); idx != -1 && idx < nextKw {
				nextKw = idx
			}
		}
		skipStr := strings.TrimSpace(skipPart[:nextKw])
		if skip, err := strconv.Atoi(skipStr); err == nil && skip > 0 {
			if skip < len(result.Rows) {
				result.Rows = result.Rows[skip:]
			} else {
				result.Rows = [][]interface{}{}
			}
		}
	}

	// Apply LIMIT
	if limitIdx := findKeywordIndex(modifiers, "LIMIT"); limitIdx != -1 {
		limitPart := strings.TrimSpace(modifiers[limitIdx+5:])
		// Find next keyword
		nextKw := len(limitPart)
		for _, kw := range []string{" SKIP", " ORDER"} {
			if idx := strings.Index(strings.ToUpper(limitPart), kw); idx != -1 && idx < nextKw {
				nextKw = idx
			}
		}
		limitStr := strings.TrimSpace(limitPart[:nextKw])
		if limit, err := strconv.Atoi(limitStr); err == nil && limit >= 0 {
			if limit < len(result.Rows) {
				result.Rows = result.Rows[:limit]
			}
		}
	}

	return result, nil
}

// applyOrderByToResult applies ORDER BY to a result set
func (e *StorageExecutor) applyOrderByToResult(result *ExecuteResult, orderByClause string) *ExecuteResult {
	// Parse ORDER BY column [DESC|ASC]
	clause := strings.TrimSpace(orderByClause)
	if idx := findKeywordIndex(clause, "ORDER BY"); idx != -1 {
		clause = strings.TrimSpace(clause[idx+8:])
	}

	// Find end of ORDER BY (before LIMIT, SKIP)
	endIdx := len(clause)
	for _, kw := range []string{" LIMIT", " SKIP"} {
		if idx := strings.Index(strings.ToUpper(clause), kw); idx != -1 && idx < endIdx {
			endIdx = idx
		}
	}
	clause = strings.TrimSpace(clause[:endIdx])

	// Parse column and direction
	parts := strings.Fields(clause)
	if len(parts) == 0 {
		return result
	}

	colName := parts[0]
	descending := false
	if len(parts) > 1 && strings.ToUpper(parts[1]) == "DESC" {
		descending = true
	}

	// Find column index
	colIdx := -1
	for i, col := range result.Columns {
		if col == colName {
			colIdx = i
			break
		}
	}

	if colIdx == -1 {
		return result
	}

	// Sort rows
	sort.SliceStable(result.Rows, func(i, j int) bool {
		vi := result.Rows[i][colIdx]
		vj := result.Rows[j][colIdx]
		cmp := compareValuesForSort(vi, vj)
		if descending {
			return cmp > 0
		}
		return cmp < 0
	})

	return result
}

// compareValuesForSort compares two values for sorting, returns -1, 0, or 1
func compareValuesForSort(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison
	switch va := a.(type) {
	case int:
		if vb, ok := b.(int); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case float64:
		if vb, ok := b.(float64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	}

	// Fallback to string comparison
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
}
