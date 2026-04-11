// Shortest path Cypher syntax support for NornicDB.
// Implements shortestPath() and allShortestPaths() functions.
//
// Syntax:
//   MATCH p = shortestPath((start)-[*]-(end)) RETURN p
//   MATCH p = allShortestPaths((start)-[*]-(end)) RETURN p

package cypher

import (
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// parseShortestPathQuery parses queries with shortestPath() or allShortestPaths()
func (e *StorageExecutor) parseShortestPathQuery(cypher string) (*ShortestPathQuery, error) {
	query := &ShortestPathQuery{
		maxHops:        10, // Default max depth
		originalCypher: cypher,
	}

	funcName, pattern, funcIdx, ok := extractShortestPathCall(cypher)
	if !ok {
		return nil, fmt.Errorf("not a shortest path query")
	}
	query.findAll = strings.EqualFold(funcName, "allShortestPaths")
	if pattern == "" {
		return nil, fmt.Errorf("invalid shortestPath syntax")
	}
	match := e.parseTraversalPattern(pattern)
	if match == nil {
		return nil, fmt.Errorf("invalid path pattern: %s", pattern)
	}
	query.startNode = match.StartNode
	query.endNode = match.EndNode
	query.relTypes = match.Relationship.Types
	query.direction = match.Relationship.Direction
	if match.Relationship.MaxHops > 0 {
		query.maxHops = match.Relationship.MaxHops
	}
	query.pathVariable = extractShortestPathPathVariable(cypher, funcIdx)

	// Extract WHERE clause if present
	whereIdx := findKeywordIndexInContext(cypher, "WHERE")
	returnIdx := findKeywordIndexInContext(cypher, "RETURN")
	if whereIdx > 0 && whereIdx < returnIdx {
		query.whereClause = strings.TrimSpace(cypher[whereIdx+5 : returnIdx])
	}

	// Extract RETURN clause
	if returnIdx > 0 {
		query.returnClause = strings.TrimSpace(cypher[returnIdx+6:])
	}

	// Resolve variable bindings from the preceding MATCH clause, if present.
	e.resolveShortestPathVariables(query, match.StartNode.variable, match.EndNode.variable, extractPreviousMatchClause(cypher, funcIdx))

	return query, nil
}

// resolveShortestPathVariables resolves variable references from the MATCH clause
func (e *StorageExecutor) resolveShortestPathVariables(query *ShortestPathQuery, startVar, endVar, matchClause string) {
	if strings.TrimSpace(matchClause) == "" {
		return
	}

	// Parse node patterns from the MATCH clause
	// Pattern: (var:Label {props}), (var2:Label2 {props2})
	nodePatterns := e.splitNodePatterns(matchClause)

	varBindings := make(map[string]nodePatternInfo)
	for _, np := range nodePatterns {
		info := e.parseNodePattern(np)
		if info.variable != "" {
			varBindings[info.variable] = info
		}
	}

	// Check if startVar is a variable reference (no labels/props in shortestPath pattern)
	if len(query.startNode.labels) == 0 && len(query.startNode.properties) == 0 {
		// It's a variable reference - look it up
		if binding, ok := varBindings[startVar]; ok {
			// Find the actual node
			query.startVarBinding = e.findNodeByPattern(binding)
		}
	}

	// Check if endVar is a variable reference
	if len(query.endNode.labels) == 0 && len(query.endNode.properties) == 0 {
		// It's a variable reference - look it up
		if binding, ok := varBindings[endVar]; ok {
			// Find the actual node
			query.endVarBinding = e.findNodeByPattern(binding)
		}
	}
}

func extractShortestPathCall(cypher string) (string, string, int, bool) {
	upperCypher := strings.ToUpper(cypher)
	for _, funcName := range []string{"allShortestPaths", "shortestPath"} {
		upperFunc := strings.ToUpper(funcName)
		searchStart := 0
		for searchStart < len(cypher) {
			idx := strings.Index(upperCypher[searchStart:], upperFunc)
			if idx < 0 {
				break
			}
			idx += searchStart
			if idx > 0 && isWordChar(cypher[idx-1]) {
				searchStart = idx + 1
				continue
			}
			openParen := idx + len(funcName)
			for openParen < len(cypher) && (cypher[openParen] == ' ' || cypher[openParen] == '\t' || cypher[openParen] == '\n' || cypher[openParen] == '\r') {
				openParen++
			}
			if openParen >= len(cypher) || cypher[openParen] != '(' {
				searchStart = idx + 1
				continue
			}
			closeParen := findMatchingParen(cypher, openParen)
			if closeParen < 0 {
				return funcName, "", idx, false
			}
			return funcName, strings.TrimSpace(cypher[openParen+1 : closeParen]), idx, true
		}
	}
	return "", "", -1, false
}

func extractShortestPathPathVariable(cypher string, funcIdx int) string {
	matchIdx := lastKeywordIndexBefore(cypher, "MATCH", funcIdx)
	if matchIdx < 0 {
		return ""
	}
	clause := strings.TrimSpace(cypher[matchIdx+len("MATCH") : funcIdx])
	eqIdx := strings.LastIndex(clause, "=")
	if eqIdx <= 0 {
		return ""
	}
	left := strings.TrimSpace(clause[:eqIdx])
	if !isValidIdentifier(left) {
		return ""
	}
	return left
}

func extractPreviousMatchClause(cypher string, beforeIdx int) string {
	currentMatchIdx := lastKeywordIndexBefore(cypher, "MATCH", beforeIdx)
	if currentMatchIdx < 0 {
		return ""
	}
	previousMatchIdx := lastKeywordIndexBefore(cypher, "MATCH", currentMatchIdx)
	if previousMatchIdx < 0 {
		return ""
	}
	return strings.TrimSpace(cypher[previousMatchIdx+len("MATCH") : currentMatchIdx])
}

// findNodeByPattern finds a node matching the given pattern
func (e *StorageExecutor) findNodeByPattern(pattern nodePatternInfo) *storage.Node {
	var candidates []*storage.Node

	if len(pattern.labels) > 0 {
		candidates, _ = e.storage.GetNodesByLabel(pattern.labels[0])
	} else {
		candidates, _ = e.storage.AllNodes()
	}

	for _, node := range candidates {
		if e.nodeMatchesProps(node, pattern.properties) {
			return node
		}
	}

	return nil
}

// ShortestPathQuery represents a parsed shortest path query
type ShortestPathQuery struct {
	pathVariable    string
	startNode       nodePatternInfo
	endNode         nodePatternInfo
	startVarBinding *storage.Node // Resolved node from MATCH clause (if variable reference)
	endVarBinding   *storage.Node // Resolved node from MATCH clause (if variable reference)
	relTypes        []string
	direction       string
	maxHops         int
	findAll         bool // true for allShortestPaths, false for shortestPath
	whereClause     string
	returnClause    string
	originalCypher  string // Full original query for MATCH clause parsing
}

// executeShortestPathQuery executes a shortestPath or allShortestPaths query
func (e *StorageExecutor) executeShortestPathQuery(query *ShortestPathQuery) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Resolve start and end nodes from variable bindings (from MATCH clause)
	// If startNode/endNode has a variable reference but no labels/props, it references
	// a node from the preceding MATCH clause. We need to find those nodes.

	var startNodes []*storage.Node
	var endNodes []*storage.Node

	// Check if we have concrete node patterns or just variable references
	startHasPattern := len(query.startNode.labels) > 0 || len(query.startNode.properties) > 0
	endHasPattern := len(query.endNode.labels) > 0 || len(query.endNode.properties) > 0

	if !startHasPattern && query.startVarBinding != nil {
		// Variable reference - use the resolved node from MATCH clause
		startNodes = []*storage.Node{query.startVarBinding}
	} else if len(query.startNode.labels) > 0 {
		startNodes, _ = e.storage.GetNodesByLabel(query.startNode.labels[0])
		// Filter by properties
		if len(query.startNode.properties) > 0 {
			var filtered []*storage.Node
			for _, n := range startNodes {
				if e.nodeMatchesProps(n, query.startNode.properties) {
					filtered = append(filtered, n)
				}
			}
			startNodes = filtered
		}
	} else {
		startNodes, _ = e.storage.AllNodes()
	}

	if !endHasPattern && query.endVarBinding != nil {
		// Variable reference - use the resolved node from MATCH clause
		endNodes = []*storage.Node{query.endVarBinding}
	} else if len(query.endNode.labels) > 0 {
		endNodes, _ = e.storage.GetNodesByLabel(query.endNode.labels[0])
		// Filter by properties
		if len(query.endNode.properties) > 0 {
			var filtered []*storage.Node
			for _, n := range endNodes {
				if e.nodeMatchesProps(n, query.endNode.properties) {
					filtered = append(filtered, n)
				}
			}
			endNodes = filtered
		}
	} else {
		endNodes, _ = e.storage.AllNodes()
	}

	// Find paths between all start/end node pairs
	var allPaths []PathResult
	for _, start := range startNodes {
		for _, end := range endNodes {
			if start.ID == end.ID {
				continue // Skip same node
			}

			if query.findAll {
				paths := e.allShortestPaths(start, end, query.relTypes, query.direction, query.maxHops)
				allPaths = append(allPaths, paths...)
			} else {
				path := e.shortestPath(start, end, query.relTypes, query.direction, query.maxHops)
				if path != nil {
					allPaths = append(allPaths, *path)
				}
			}
		}
	}

	// Build result
	if query.returnClause != "" {
		// Parse return items
		returnItems := e.parseReturnItems(query.returnClause)

		for _, item := range returnItems {
			if item.alias != "" {
				result.Columns = append(result.Columns, item.alias)
			} else {
				result.Columns = append(result.Columns, item.expr)
			}
		}

		// Build rows from paths
		for _, path := range allPaths {
			row := make([]interface{}, len(returnItems))

			for i, item := range returnItems {
				// Handle path variable and path functions
				exprLower := strings.ToLower(item.expr)
				pathVarLower := strings.ToLower(query.pathVariable)

				// Check if expression is the path variable directly, or a path function like length(p), nodes(p), relationships(p)
				isPathExpr := item.expr == query.pathVariable ||
					strings.HasPrefix(item.expr, query.pathVariable+".") ||
					strings.Contains(exprLower, "length("+pathVarLower+")") ||
					strings.Contains(exprLower, "nodes("+pathVarLower+")") ||
					strings.Contains(exprLower, "relationships("+pathVarLower+")")

				if isPathExpr {
					row[i] = e.pathToValue(path, item.expr, query.pathVariable)
				} else {
					// Try to evaluate as expression
					row[i] = e.evaluatePathExpression(item.expr, path, query)
				}
			}

			result.Rows = append(result.Rows, row)
		}
	} else {
		// Return paths directly
		result.Columns = []string{query.pathVariable}
		for _, path := range allPaths {
			result.Rows = append(result.Rows, []interface{}{e.pathToMap(path)})
		}
	}

	return result, nil
}

// pathToValue converts a path to the requested value
func (e *StorageExecutor) pathToValue(path PathResult, expr, pathVar string) interface{} {
	if expr == pathVar {
		// Return full path
		return e.pathToMap(path)
	}

	// Handle path functions: length(p), nodes(p), relationships(p)
	if matchFuncStart(expr, "length") {
		return int64(path.Length)
	}

	if matchFuncStart(expr, "nodes") {
		nodes := make([]interface{}, len(path.Nodes))
		for i, n := range path.Nodes {
			nodes[i] = n
		}
		return nodes
	}

	if matchFuncStart(expr, "relationships") {
		rels := make([]interface{}, len(path.Relationships))
		for i, r := range path.Relationships {
			rels[i] = e.edgeToMap(r)
		}
		return rels
	}

	return nil
}

// pathToMap converts a PathResult to a map representation
func (e *StorageExecutor) pathToMap(path PathResult) map[string]interface{} {
	nodes := make([]interface{}, len(path.Nodes))
	for i, n := range path.Nodes {
		nodes[i] = n
	}

	rels := make([]interface{}, len(path.Relationships))
	for i, r := range path.Relationships {
		rels[i] = e.edgeToMap(r)
	}

	return map[string]interface{}{
		"_pathResult":   path,
		"nodes":         nodes,
		"relationships": rels,
		"length":        int64(path.Length),
	}
}

// evaluatePathExpression evaluates an expression in the context of a path
func (e *StorageExecutor) evaluatePathExpression(expr string, path PathResult, query *ShortestPathQuery) interface{} {
	// Build context from path
	nodes := make(map[string]*storage.Node)
	rels := make(map[string]*storage.Edge)

	if query.startNode.variable != "" && len(path.Nodes) > 0 {
		nodes[query.startNode.variable] = path.Nodes[0]
	}
	if query.endNode.variable != "" && len(path.Nodes) > 1 {
		nodes[query.endNode.variable] = path.Nodes[len(path.Nodes)-1]
	}

	return e.evaluateExpressionWithContext(expr, nodes, rels)
}

// isShortestPathQuery checks if a query uses shortestPath or allShortestPaths
func isShortestPathQuery(cypher string) bool {
	upper := strings.ToUpper(cypher)
	return strings.Contains(upper, "SHORTESTPATH") || strings.Contains(upper, "ALLSHORTESTPATHS")
}
