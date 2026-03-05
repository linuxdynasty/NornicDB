// MERGE clause implementation for NornicDB.
// This file contains MERGE execution, compound queries, and context-aware operations.

package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) executeMerge(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	// Extract the main MERGE pattern - use word boundary detection
	mergeIdx := findKeywordIndex(cypher, "MERGE")
	if mergeIdx == -1 {
		return nil, fmt.Errorf("MERGE clause not found in query: %q", truncateQuery(cypher, 80))
	}

	// Find ON CREATE SET, ON MATCH SET, standalone SET, and RETURN clauses
	// Use word boundary detection to avoid matching substrings
	onCreateIdx := findKeywordIndex(cypher, "ON CREATE SET")
	onMatchIdx := findKeywordIndex(cypher, "ON MATCH SET")
	returnIdx := findKeywordIndex(cypher, "RETURN")
	withIdx := findKeywordIndex(cypher, "WITH")

	// Find standalone SET clause (after ON CREATE SET / ON MATCH SET)
	// Must handle SET preceded by space, tab, or newline
	setIdx := -1
	searchStart := 0
	if onCreateIdx > 0 {
		searchStart = onCreateIdx + 13 // After "ON CREATE SET"
	}
	if onMatchIdx > 0 && onMatchIdx > searchStart {
		searchStart = onMatchIdx + 12 // After "ON MATCH SET"
	}

	// Helper function to find SET with any whitespace before it
	findStandaloneSet := func(s string, start int) int {
		upperS := strings.ToUpper(s)
		for i := start; i <= len(upperS)-3; i++ {
			if strings.HasPrefix(upperS[i:], "SET") {
				// Check for whitespace before SET
				if i > 0 {
					prevChar := upperS[i-1]
					if prevChar != ' ' && prevChar != '\n' && prevChar != '\t' && prevChar != '\r' {
						continue // Not a word boundary
					}
				}
				// Check for whitespace/end after SET
				endPos := i + 3
				if endPos < len(upperS) {
					nextChar := upperS[endPos]
					if nextChar != ' ' && nextChar != '\n' && nextChar != '\t' && nextChar != '\r' {
						continue // Not a word boundary
					}
				}
				// Make sure this isn't part of ON CREATE SET or ON MATCH SET
				if i >= 10 && strings.HasPrefix(upperS[i-10:], "ON CREATE ") {
					continue
				}
				if i >= 9 && strings.HasPrefix(upperS[i-9:], "ON MATCH ") {
					continue
				}
				return i
			}
		}
		return -1
	}

	if searchStart > 0 {
		setIdx = findStandaloneSet(cypher, searchStart)
	} else {
		setIdx = findStandaloneSet(cypher, 0)
	}

	// Determine where the MERGE pattern ends
	patternEnd := len(cypher)
	for _, idx := range []int{onCreateIdx, onMatchIdx, setIdx, returnIdx} {
		if idx > 0 && idx < patternEnd {
			patternEnd = idx
		}
	}

	// Extract MERGE pattern (e.g., "(n:Label {prop: value})")
	mergePattern := strings.TrimSpace(cypher[mergeIdx+5 : patternEnd])

	// Parse the pattern to extract labels and properties for matching
	// Note: Parameters ($param) should already be substituted by substituteParams()
	varName, labels, matchProps, err := e.parseMergePattern(mergePattern)

	// If pattern contains unsubstituted params (like $path), handle gracefully
	if strings.Contains(mergePattern, "$") {
		// Extract what we can from the pattern
		varName = e.extractVarName(mergePattern)
		labels = e.extractLabels(mergePattern)
		matchProps = make(map[string]interface{})
		err = nil // Continue with partial info
	}

	if err != nil || (len(labels) == 0 && len(matchProps) == 0) {
		// If we truly can't parse, create a basic node
		node := &storage.Node{
			ID:         storage.NodeID(e.generateID()),
			Labels:     labels,
			Properties: matchProps,
		}
		actualID, err := store.CreateNode(node)
		if err != nil {
			return nil, fmt.Errorf("failed to create node in MERGE: %w", err)
		}
		node.ID = actualID
		e.notifyNodeMutated(string(node.ID))
		result.Stats.NodesCreated = 1

		if varName == "" {
			varName = "n"
		}
		result.Columns = []string{varName}
		result.Rows = append(result.Rows, []interface{}{node})
		return result, nil
	}

	// Try to find existing node
	var existingNode *storage.Node
	if len(labels) > 0 && len(matchProps) > 0 {
		// Search for node with matching label and properties
		nodes, _ := store.GetNodesByLabel(labels[0])
		for _, n := range nodes {
			matches := true
			for key, val := range matchProps {
				if nodeVal, ok := n.Properties[key]; !ok || nodeVal != val {
					matches = false
					break
				}
			}
			if matches {
				existingNode = n
				break
			}
		}
	}

	var node *storage.Node
	if existingNode != nil {
		// Node exists - apply ON MATCH SET if present
		node = existingNode
		if onMatchIdx > 0 {
			setEnd := len(cypher)
			for _, idx := range []int{onCreateIdx, returnIdx} {
				if idx > onMatchIdx && idx < setEnd {
					setEnd = idx
				}
			}
			setClause := strings.TrimSpace(cypher[onMatchIdx+13 : setEnd])
			e.applySetToNode(node, varName, setClause)
			store.UpdateNode(node)
			e.notifyNodeMutated(string(node.ID))
		}
	} else {
		// Node doesn't exist - create it
		node = &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("node-%d", e.idCounter())),
			Labels:     labels,
			Properties: matchProps,
		}
		actualID, err := store.CreateNode(node)
		if err != nil {
			return nil, fmt.Errorf("failed to create node in MERGE: %w", err)
		}
		node.ID = actualID
		e.notifyNodeMutated(string(node.ID))
		result.Stats.NodesCreated = 1

		// Apply ON CREATE SET if present
		if onCreateIdx > 0 {
			setEnd := len(cypher)
			// Stop at: standalone SET, ON MATCH SET, WITH, or RETURN
			for _, idx := range []int{setIdx, onMatchIdx, withIdx, returnIdx} {
				if idx > onCreateIdx && idx < setEnd {
					setEnd = idx
				}
			}
			setClause := strings.TrimSpace(cypher[onCreateIdx+13 : setEnd])
			e.applySetToNode(node, varName, setClause)
		}
	}

	// Apply standalone SET clause (runs for both create and match)
	if setIdx > 0 {
		setEnd := len(cypher)
		for _, idx := range []int{withIdx, returnIdx} {
			if idx > setIdx && idx < setEnd {
				setEnd = idx
			}
		}
		setClause := strings.TrimSpace(cypher[setIdx+3 : setEnd]) // +3 to skip "SET"
		e.applySetToNode(node, varName, setClause)
	}

	// Persist updates
	if existingNode != nil || setIdx > 0 || onCreateIdx > 0 {
		store.UpdateNode(node)
		e.notifyNodeMutated(string(node.ID))
	}

	// Handle RETURN clause
	if returnIdx > 0 {
		returnClause := strings.TrimSpace(cypher[returnIdx+6:])
		columns, values := e.parseReturnClause(returnClause, varName, node)
		result.Columns = columns
		if len(values) > 0 {
			result.Rows = append(result.Rows, values)
		}
	}

	return result, nil
}

// executeCompoundMatchMerge handles MATCH ... MERGE ... queries where MERGE references matched nodes.
// This is the Neo4j pattern: MATCH (a) ... MERGE (b) ... SET b.prop = a.prop, etc.
func (e *StorageExecutor) executeCompoundMatchMerge(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Use word boundary detection to avoid matching substrings
	matchIdx := findKeywordIndex(cypher, "MATCH")
	mergeIdx := findKeywordIndex(cypher, "MERGE")

	// If MERGE appears at the start, find the second one (after MATCH)
	if mergeIdx <= matchIdx && mergeIdx != -1 {
		// Search for MERGE after MATCH
		afterMatch := cypher[matchIdx+5:]
		secondMergeIdx := findKeywordIndex(afterMatch, "MERGE")
		if secondMergeIdx != -1 {
			mergeIdx = matchIdx + 5 + secondMergeIdx
		}
	}

	if matchIdx == -1 || mergeIdx == -1 {
		return nil, fmt.Errorf("invalid MATCH ... MERGE query")
	}

	// Extract MATCH clause
	matchClause := strings.TrimSpace(cypher[matchIdx:mergeIdx])
	mergeClause := strings.TrimSpace(cypher[mergeIdx:])

	// Execute MATCH to get context
	matchedNodes, matchedRels, err := e.executeMatchForContext(ctx, matchClause)
	if err != nil {
		return nil, fmt.Errorf("failed to execute MATCH: %v", err)
	}

	// If no matches found and not OPTIONAL MATCH, return empty
	if len(matchedNodes) == 0 && findKeywordIndex(cypher, "OPTIONAL MATCH") == -1 {
		return result, nil
	}

	// For each set of matched nodes, execute the MERGE with context
	for _, nodeContext := range matchedNodes {
		mergeResult, err := e.executeMergeWithContext(ctx, mergeClause, nodeContext, matchedRels)
		if err != nil {
			return nil, err
		}

		// Combine results
		if mergeResult.Stats != nil {
			result.Stats.NodesCreated += mergeResult.Stats.NodesCreated
			result.Stats.RelationshipsCreated += mergeResult.Stats.RelationshipsCreated
			result.Stats.PropertiesSet += mergeResult.Stats.PropertiesSet
		}

		// Add rows from merge result
		if len(mergeResult.Columns) > 0 && len(result.Columns) == 0 {
			result.Columns = mergeResult.Columns
		}
		result.Rows = append(result.Rows, mergeResult.Rows...)
	}

	// If no matched nodes but had OPTIONAL MATCH, still try to execute MERGE
	if len(matchedNodes) == 0 {
		mergeResult, err := e.executeMergeWithContext(ctx, mergeClause, make(map[string]*storage.Node), make(map[string]*storage.Edge))
		if err != nil {
			return nil, err
		}
		result = mergeResult
	}

	return result, nil
}

// executeMatchForContext executes a MATCH clause and returns matched nodes by variable name.
// Handles both simple node patterns like (a:Label), (b:Label2) and relationship patterns
// like (a)<-[:REL]-(b)-[:REL]->(c).
func (e *StorageExecutor) executeMatchForContext(ctx context.Context, matchClause string) ([]map[string]*storage.Node, map[string]*storage.Edge, error) {
	relMatches := make(map[string]*storage.Edge)
	store := e.getStorage(ctx)

	upper := strings.ToUpper(matchClause)

	// Find WHERE clause if present
	whereIdx := strings.Index(upper, " WHERE ")
	var patternPart string

	if whereIdx > 0 {
		patternPart = matchClause[5:whereIdx]
	} else {
		patternPart = matchClause[5:]
	}

	patternPart = strings.TrimSpace(patternPart)

	// Check if this is a relationship pattern (contains ->, <-, or ]-)
	// If so, we need to use proper path matching, not cartesian product
	hasRelationship := strings.Contains(patternPart, "->") ||
		strings.Contains(patternPart, "<-") ||
		strings.Contains(patternPart, "]-")

	if hasRelationship {
		// Use executeMatch to properly find paths, then extract variable bindings
		return e.executeMatchForContextWithRelationships(ctx, matchClause, patternPart)
	}

	// Simple node patterns only - use cartesian product approach
	// Split multiple node patterns: (a:Label), (b:Label2)
	nodePatterns := e.splitNodePatterns(patternPart)

	// If no patterns found, try parsing as single pattern
	if len(nodePatterns) == 0 {
		nodePatterns = []string{patternPart}
	}

	// For each node pattern, find matching nodes
	patternMatches := make([]struct {
		variable string
		nodes    []*storage.Node
	}, len(nodePatterns))

	for i, np := range nodePatterns {
		nodeInfo := e.parseNodePattern(np)

		var candidates []*storage.Node
		if len(nodeInfo.labels) > 0 {
			candidates, _ = store.GetNodesByLabel(nodeInfo.labels[0])
		} else {
			candidates = store.GetAllNodes()
		}

		// Filter by properties
		var filtered []*storage.Node
		for _, node := range candidates {
			if e.nodeMatchesProps(node, nodeInfo.properties) {
				filtered = append(filtered, node)
			}
		}

		patternMatches[i] = struct {
			variable string
			nodes    []*storage.Node
		}{
			variable: nodeInfo.variable,
			nodes:    filtered,
		}
	}

	// Build cartesian product of all pattern matches
	allMatches := e.buildCartesianProduct(patternMatches)

	// Apply WHERE clause to each combination
	if whereIdx > 0 {
		wherePart := matchClause[whereIdx+7:]
		var filtered []map[string]*storage.Node
		for _, nodeMap := range allMatches {
			matches := true
			for varName, node := range nodeMap {
				if !e.evaluateWhere(node, varName, wherePart) {
					// Check if WHERE references this variable (property access, function call, or direct reference)
					lowerWhere := strings.ToLower(wherePart)
					refsVar := strings.Contains(wherePart, varName+".") ||
						strings.Contains(wherePart, varName+" ") ||
						strings.Contains(lowerWhere, "id("+varName+")") ||
						strings.Contains(lowerWhere, "elementid("+varName+")")
					if refsVar {
						matches = false
						break
					}
				}
			}
			if matches {
				filtered = append(filtered, nodeMap)
			}
		}
		allMatches = filtered
	}

	return allMatches, relMatches, nil
}

// executeMatchForContextWithRelationships handles MATCH patterns that include relationships.
// It executes the MATCH query and extracts variable bindings from the results.
func (e *StorageExecutor) executeMatchForContextWithRelationships(ctx context.Context, matchClause, patternPart string) ([]map[string]*storage.Node, map[string]*storage.Edge, error) {
	relMatches := make(map[string]*storage.Edge)
	store := e.getStorage(ctx)

	// Extract all variable names from the pattern
	varNames := e.extractVariableNamesFromPattern(patternPart)
	if len(varNames) == 0 {
		return nil, relMatches, nil
	}

	// Build a synthetic RETURN clause to get all node variables
	// Filter to only include node variables (not relationship variables)
	nodeVarNames := make([]string, 0)
	for _, v := range varNames {
		// Skip relationship variables (they appear after [ and before ])
		// Node variables appear after ( and before )
		nodeVarNames = append(nodeVarNames, v)
	}

	if len(nodeVarNames) == 0 {
		return nil, relMatches, nil
	}

	// Build RETURN clause with all variables
	returnClause := "RETURN " + strings.Join(nodeVarNames, ", ")
	fullQuery := matchClause + " " + returnClause

	// Execute the match
	result, err := e.executeMatch(ctx, fullQuery)
	if err != nil {
		return nil, relMatches, err
	}

	// Convert results to node context maps
	var allMatches []map[string]*storage.Node

	for _, row := range result.Rows {
		nodeMap := make(map[string]*storage.Node)
		for i, col := range result.Columns {
			if i >= len(row) {
				continue
			}

			// Get the node from storage based on the returned value
			val := row[i]
			if val == nil {
				continue
			}

			// The returned value might be a map or a node representation
			var node *storage.Node

			switch v := val.(type) {
			case map[string]interface{}:
				// It's a map representation - find the actual node
				// Look for an ID property or _id
				if id, ok := v["_id"]; ok {
					if nodeID, ok := id.(string); ok {
						node, _ = store.GetNode(storage.NodeID(nodeID))
					}
				} else if id, ok := v["id"]; ok {
					if nodeID, ok := id.(string); ok {
						node, _ = store.GetNode(storage.NodeID(nodeID))
					}
				}
				// If we still don't have a node, try to find by properties
				if node == nil {
					// Try to find by matching properties
					node = e.findNodeByProperties(v)
				}
			case *storage.Node:
				node = v
			case storage.Node:
				node = &v
			}

			if node != nil {
				nodeMap[col] = node
			}
		}

		if len(nodeMap) > 0 {
			allMatches = append(allMatches, nodeMap)
		}
	}

	return allMatches, relMatches, nil
}

// extractVariableNamesFromPattern extracts variable names from a Cypher pattern.
// e.g., "(p:Person)<-[:REL]-(poc:POC)-[:BELONGS_TO]->(a:Area)" returns ["p", "poc", "a"]
func (e *StorageExecutor) extractVariableNamesFromPattern(pattern string) []string {
	var varNames []string
	seen := make(map[string]bool)

	// Find all node patterns (...)
	inParen := false
	inBracket := false
	var current strings.Builder

	for _, c := range pattern {
		switch c {
		case '(':
			inParen = true
			current.Reset()
		case ')':
			if inParen {
				nodeContent := current.String()
				// Extract variable name (before : or end)
				varName := strings.Split(nodeContent, ":")[0]
				varName = strings.TrimSpace(varName)
				// Remove any property part
				if idx := strings.Index(varName, "{"); idx > 0 {
					varName = strings.TrimSpace(varName[:idx])
				}
				if varName != "" && !seen[varName] {
					varNames = append(varNames, varName)
					seen[varName] = true
				}
			}
			inParen = false
		case '[':
			inBracket = true
		case ']':
			inBracket = false
		default:
			if inParen && !inBracket {
				current.WriteRune(c)
			}
		}
	}

	return varNames
}

// findNodeByProperties finds a node by matching its properties.
func (e *StorageExecutor) findNodeByProperties(props map[string]interface{}) *storage.Node {
	// Get all nodes and try to match
	allNodes := e.storage.GetAllNodes()
	for _, node := range allNodes {
		// Check if name property matches (common identifier)
		if name, ok := props["name"]; ok {
			if nodeName, ok := node.Properties["name"]; ok && nodeName == name {
				return node
			}
		}
		// Check if all provided properties match
		matches := true
		for k, v := range props {
			if k == "_labels" || k == "_id" {
				continue
			}
			if nodeVal, ok := node.Properties[k]; !ok || nodeVal != v {
				matches = false
				break
			}
		}
		if matches && len(props) > 0 {
			return node
		}
	}
	return nil
}

// buildCartesianProduct creates all combinations of node matches
func (e *StorageExecutor) buildCartesianProduct(patternMatches []struct {
	variable string
	nodes    []*storage.Node
}) []map[string]*storage.Node {
	if len(patternMatches) == 0 {
		return nil
	}

	// Start with first pattern's nodes
	var result []map[string]*storage.Node
	for _, node := range patternMatches[0].nodes {
		result = append(result, map[string]*storage.Node{
			patternMatches[0].variable: node,
		})
	}

	// For each subsequent pattern, expand the combinations
	for i := 1; i < len(patternMatches); i++ {
		pm := patternMatches[i]
		var expanded []map[string]*storage.Node

		for _, existing := range result {
			for _, node := range pm.nodes {
				// Copy existing map and add new variable
				newMap := make(map[string]*storage.Node)
				for k, v := range existing {
					newMap[k] = v
				}
				newMap[pm.variable] = node
				expanded = append(expanded, newMap)
			}
		}

		result = expanded
	}

	return result
}

// executeMergeWithContext executes a MERGE clause with context from a prior MATCH.
func (e *StorageExecutor) executeMergeWithContext(ctx context.Context, cypher string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	// Find clauses - use word boundary detection
	mergeIdx := findKeywordIndex(cypher, "MERGE")
	if mergeIdx == -1 {
		mergeIdx = 0 // Already stripped
	}

	onCreateIdx := findKeywordIndex(cypher, "ON CREATE SET")
	onMatchIdx := findKeywordIndex(cypher, "ON MATCH SET")
	// Use quote-aware search for RETURN and WITH since text content may contain these keywords
	returnIdx := findKeywordIndexInContext(cypher, "RETURN")
	withIdx := findKeywordIndexInContext(cypher, "WITH")

	// Find standalone SET (not ON CREATE/MATCH SET)
	// Must handle SET preceded by space, tab, or newline
	setIdx := -1
	searchStart := 0
	if onCreateIdx > 0 {
		searchStart = onCreateIdx + 13
	}
	if onMatchIdx > 0 && onMatchIdx > searchStart {
		searchStart = onMatchIdx + 12
	}

	// Helper function to find SET with any whitespace before it (reuse from executeMerge)
	findStandaloneSetInContext := func(s string, start int) int {
		upperS := strings.ToUpper(s)
		for i := start; i <= len(upperS)-3; i++ {
			if strings.HasPrefix(upperS[i:], "SET") {
				// Check for whitespace before SET
				if i > 0 {
					prevChar := upperS[i-1]
					if prevChar != ' ' && prevChar != '\n' && prevChar != '\t' && prevChar != '\r' {
						continue // Not a word boundary
					}
				}
				// Check for whitespace/end after SET
				endPos := i + 3
				if endPos < len(upperS) {
					nextChar := upperS[endPos]
					if nextChar != ' ' && nextChar != '\n' && nextChar != '\t' && nextChar != '\r' {
						continue // Not a word boundary
					}
				}
				// Make sure this isn't part of ON CREATE SET or ON MATCH SET
				if i >= 10 && strings.HasPrefix(upperS[i-10:], "ON CREATE ") {
					continue
				}
				if i >= 9 && strings.HasPrefix(upperS[i-9:], "ON MATCH ") {
					continue
				}
				return i
			}
		}
		return -1
	}

	if searchStart > 0 {
		setIdx = findStandaloneSetInContext(cypher, searchStart)
	} else {
		setIdx = findStandaloneSetInContext(cypher, 0)
	}

	// Find MERGE pattern end
	patternEnd := len(cypher)
	for _, idx := range []int{onCreateIdx, onMatchIdx, setIdx, returnIdx, withIdx} {
		if idx > 0 && idx < patternEnd {
			patternEnd = idx
		}
	}

	// Handle second MERGE in compound query (handle any whitespace before MERGE)
	// Use quote-aware search since text content may contain "MERGE" keyword
	secondMergeIdx := findKeywordIndexInContext(cypher[mergeIdx+5:], "MERGE")
	if secondMergeIdx > 0 {
		// There's a second MERGE clause - this is for relationships
		// Handle the first MERGE, then process second
		firstMergeEnd := mergeIdx + 5 + secondMergeIdx
		if firstMergeEnd < patternEnd {
			patternEnd = firstMergeEnd
		}
	}

	// Extract and parse MERGE pattern
	mergePattern := strings.TrimSpace(cypher[mergeIdx+5 : patternEnd])

	// Check if this is a relationship pattern: (a)-[r:TYPE]->(b)
	if strings.Contains(mergePattern, "->") || strings.Contains(mergePattern, "<-") || strings.Contains(mergePattern, "]-") {
		// Relationship MERGE - need to create relationship between nodes
		return e.executeMergeRelationshipWithContext(ctx, cypher, mergePattern, nodeContext, relContext)
	}

	// Parse node pattern
	varName, labels, matchProps, err := e.parseMergePattern(mergePattern)
	if err != nil || varName == "" {
		varName = e.extractVarName(mergePattern)
		labels = e.extractLabels(mergePattern)
		matchProps = make(map[string]interface{})
	}

	// Try to find existing node
	var existingNode *storage.Node
	if len(labels) > 0 && len(matchProps) > 0 {
		nodes, _ := store.GetNodesByLabel(labels[0])
		for _, n := range nodes {
			matches := true
			for key, val := range matchProps {
				if nodeVal, ok := n.Properties[key]; !ok || !e.compareEqual(nodeVal, val) {
					matches = false
					break
				}
			}
			if matches {
				existingNode = n
				break
			}
		}
	}

	var node *storage.Node
	if existingNode != nil {
		node = existingNode
		if onMatchIdx > 0 {
			setEnd := len(cypher)
			for _, idx := range []int{onCreateIdx, returnIdx, withIdx, setIdx} {
				if idx > onMatchIdx && idx < setEnd {
					setEnd = idx
				}
			}
			setClause := strings.TrimSpace(cypher[onMatchIdx+13 : setEnd])
			e.applySetToNodeWithContext(node, varName, setClause, nodeContext, relContext)
			store.UpdateNode(node)
			e.notifyNodeMutated(string(node.ID))
		}
	} else {
		node = &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("node-%d", e.idCounter())),
			Labels:     labels,
			Properties: matchProps,
		}
		actualID, err := store.CreateNode(node)
		if err != nil {
			return nil, fmt.Errorf("failed to create node in MERGE: %w", err)
		}
		node.ID = actualID
		e.notifyNodeMutated(string(node.ID))
		result.Stats.NodesCreated = 1

		if onCreateIdx > 0 {
			setEnd := len(cypher)
			for _, idx := range []int{setIdx, onMatchIdx, withIdx, returnIdx} {
				if idx > onCreateIdx && idx < setEnd {
					setEnd = idx
				}
			}
			setClause := strings.TrimSpace(cypher[onCreateIdx+13 : setEnd])
			e.applySetToNodeWithContext(node, varName, setClause, nodeContext, relContext)
		}
	}

	// Apply standalone SET
	if setIdx > 0 {
		setEnd := len(cypher)
		// Also check for second MERGE - SET clause ends there too
		secondMergeAbsIdx := -1
		if secondMergeIdx > 0 {
			secondMergeAbsIdx = mergeIdx + 5 + secondMergeIdx
		}
		for _, idx := range []int{withIdx, returnIdx, secondMergeAbsIdx} {
			if idx > setIdx && idx < setEnd {
				setEnd = idx
			}
		}
		setClause := strings.TrimSpace(cypher[setIdx+3 : setEnd])
		e.applySetToNodeWithContext(node, varName, setClause, nodeContext, relContext)
	}

	// Save updates
	store.UpdateNode(node)
	e.notifyNodeMutated(string(node.ID))

	// Add this node to context for subsequent MERGEs
	nodeContext[varName] = node

	// Handle second MERGE (usually relationship creation)
	if secondMergeIdx > 0 {
		secondMergePart := strings.TrimSpace(cypher[mergeIdx+5+secondMergeIdx+1:])
		_, err := e.executeMergeWithContext(ctx, secondMergePart, nodeContext, relContext)
		if err != nil {
			return nil, err
		}
	}

	// Handle RETURN clause
	if returnIdx > 0 {
		returnClause := strings.TrimSpace(cypher[returnIdx+6:])
		columns, values := e.parseReturnClauseWithContext(returnClause, nodeContext, relContext)
		result.Columns = columns
		if len(values) > 0 {
			result.Rows = append(result.Rows, values)
		}
	}

	return result, nil
}

// executeMergeRelationshipWithContext handles MERGE for relationship patterns.
func (e *StorageExecutor) executeMergeRelationshipWithContext(ctx context.Context, cypher string, pattern string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) (*ExecuteResult, error) {
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	store := e.getStorage(ctx)

	// Use word boundary detection
	returnIdx := findKeywordIndex(cypher, "RETURN")

	// Parse relationship pattern: (a)-[r:TYPE {props}]->(b)
	// Extract start node, relationship, end node

	// Find the relationship part
	relStart := strings.Index(pattern, "[")
	relEnd := strings.Index(pattern, "]")

	if relStart == -1 || relEnd == -1 {
		return result, nil // Not a valid relationship pattern
	}

	// Get start and end node variables
	startPart := strings.TrimSpace(pattern[:relStart])
	endPart := strings.TrimSpace(pattern[relEnd+1:])
	relPart := pattern[relStart+1 : relEnd]

	// Remove direction markers and parens
	startPart = strings.Trim(startPart, "()-")
	endPart = strings.Trim(endPart, "()<>-")

	// Extract start/end variable names
	startVar := strings.Split(startPart, ":")[0]
	endVar := strings.Split(endPart, ":")[0]

	// Parse relationship type and variable
	relVar := ""
	relType := ""
	relProps := make(map[string]interface{})

	relPart = strings.TrimSpace(relPart)
	propsStart := strings.Index(relPart, "{")
	if propsStart > 0 {
		propsEnd := strings.LastIndex(relPart, "}")
		if propsEnd > propsStart {
			relProps = e.parseProperties(relPart[propsStart : propsEnd+1])
		}
		relPart = relPart[:propsStart]
	}

	relParts := strings.Split(relPart, ":")
	if len(relParts) > 0 {
		relVar = strings.TrimSpace(relParts[0])
	}
	if len(relParts) > 1 {
		relType = strings.TrimSpace(relParts[1])
	}

	// Get start and end nodes from context
	startNode := nodeContext[startVar]
	endNode := nodeContext[endVar]

	if startNode == nil || endNode == nil {
		// Nodes not in context - can't create relationship
		return result, nil
	}

	// Check if relationship exists
	existingEdge := store.GetEdgeBetween(startNode.ID, endNode.ID, relType)

	var edge *storage.Edge
	if existingEdge != nil {
		edge = existingEdge
	} else {
		// Create new relationship
		edge = &storage.Edge{
			ID:         storage.EdgeID(e.generateID()),
			Type:       relType,
			StartNode:  startNode.ID,
			EndNode:    endNode.ID,
			Properties: relProps,
		}
		err := store.CreateEdge(edge)
		if err != nil {
			// If already exists error, ignore it (MERGE semantics)
			if err == storage.ErrAlreadyExists {
				// Try to find the existing edge again
				existingEdge = store.GetEdgeBetween(startNode.ID, endNode.ID, relType)
				if existingEdge != nil {
					edge = existingEdge
				}
			} else {
				return nil, fmt.Errorf("failed to create relationship: %w", err)
			}
		} else {
			result.Stats.RelationshipsCreated = 1
		}
	}

	// Store in context
	if relVar != "" {
		relContext[relVar] = edge
	}

	// Handle RETURN
	if returnIdx > 0 {
		returnClause := strings.TrimSpace(cypher[returnIdx+6:])
		columns, values := e.parseReturnClauseWithContext(returnClause, nodeContext, relContext)
		result.Columns = columns
		if len(values) > 0 {
			result.Rows = append(result.Rows, values)
		}
	}

	return result, nil
}

// applySetToNodeWithContext applies SET clauses with access to matched context.
func (e *StorageExecutor) applySetToNodeWithContext(node *storage.Node, varName string, setClause string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) {
	// Add current node to context for self-references
	fullContext := make(map[string]*storage.Node)
	for k, v := range nodeContext {
		fullContext[k] = v
	}
	fullContext[varName] = node

	// Split SET clause into individual assignments
	assignments := e.splitSetAssignments(setClause)

	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if !strings.HasPrefix(assignment, varName+".") {
			continue
		}

		eqIdx := strings.Index(assignment, "=")
		if eqIdx <= 0 {
			continue
		}

		propName := strings.TrimSpace(assignment[len(varName)+1 : eqIdx])
		propValue := strings.TrimSpace(assignment[eqIdx+1:])

		// Evaluate expression with full context
		setNodeProperty(node, propName, e.evaluateSetExpressionWithContext(propValue, fullContext, relContext))
	}
}

// evaluateSetExpressionWithContext evaluates SET clause expressions with context.
func (e *StorageExecutor) evaluateSetExpressionWithContext(expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) interface{} {
	return e.evaluateExpressionWithContext(expr, nodes, rels)
}

// parseReturnClauseWithContext parses RETURN with context from MATCH.
func (e *StorageExecutor) parseReturnClauseWithContext(returnClause string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) ([]string, []interface{}) {
	// Handle RETURN *
	if strings.TrimSpace(returnClause) == "*" {
		var columns []string
		var values []interface{}
		for name, node := range nodes {
			columns = append(columns, name)
			values = append(values, node)
		}
		return columns, values
	}

	var columns []string
	var values []interface{}

	parts := e.splitReturnExpressions(returnClause)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		var expr, alias string
		asIdx := strings.LastIndex(strings.ToUpper(part), " AS ")
		if asIdx > 0 {
			expr = strings.TrimSpace(part[:asIdx])
			alias = strings.TrimSpace(part[asIdx+4:])
		} else {
			expr = part
			alias = e.expressionToAlias(expr)
		}

		value := e.evaluateExpressionWithContext(expr, nodes, rels)
		columns = append(columns, alias)
		values = append(values, value)
	}

	return columns, values
}

// parseReturnClause parses RETURN expressions and evaluates them against a node.
// Supports: n.prop, n.prop AS alias, id(n), *, literal values
func (e *StorageExecutor) parseReturnClause(returnClause string, varName string, node *storage.Node) ([]string, []interface{}) {
	// Handle RETURN *
	if strings.TrimSpace(returnClause) == "*" {
		return []string{varName}, []interface{}{node}
	}

	var columns []string
	var values []interface{}

	// Split by comma, but be careful with nested expressions
	parts := e.splitReturnExpressions(returnClause)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Check for AS alias
		var expr, alias string
		asIdx := strings.LastIndex(strings.ToUpper(part), " AS ")
		if asIdx > 0 {
			expr = strings.TrimSpace(part[:asIdx])
			alias = strings.TrimSpace(part[asIdx+4:])
		} else {
			expr = part
			// Generate alias from expression
			alias = e.expressionToAlias(expr)
		}

		// Evaluate expression
		value := e.evaluateExpression(expr, varName, node)
		columns = append(columns, alias)
		values = append(values, value)
	}

	return columns, values
}

// splitReturnExpressions splits RETURN clause by commas, respecting parentheses.
func (e *StorageExecutor) splitReturnExpressions(clause string) []string {
	var result []string
	var current strings.Builder
	depth := 0

	for _, ch := range clause {
		switch ch {
		case '(':
			depth++
			current.WriteRune(ch)
		case ')':
			depth--
			current.WriteRune(ch)
		case ',':
			if depth == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// expressionToAlias converts an expression to a column alias.
func (e *StorageExecutor) expressionToAlias(expr string) string {
	expr = strings.TrimSpace(expr)

	// Function call: id(n) -> id(n)
	if strings.Contains(expr, "(") {
		return expr
	}

	// Property access: n.prop -> prop
	if dotIdx := strings.LastIndex(expr, "."); dotIdx > 0 {
		return expr[dotIdx+1:]
	}

	return expr
}

// executeMergeWithChain handles MERGE ... WITH ... MATCH ... MERGE chain patterns.
// This is the pattern used in import scripts:
//
//	MERGE (e:Entry {key: $key})
//	ON CREATE SET e.value = $value
//	WITH e
//	MATCH (c:Category {name: $category})
//	MERGE (e)-[:IN_CATEGORY]->(c)
//	WITH e
//	MATCH (t:Team {name: $team})
//	MERGE (e)-[:MANAGED_BY]->(t)
//	RETURN e.key
//
// In Neo4j Cypher, if any MATCH in the chain fails to find a node,
// the query returns 0 rows (the chain is broken). The MERGE still executes
// for nodes found before the break.
func (e *StorageExecutor) executeMergeWithChain(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Split the query into segments at each WITH clause
	// Each segment is: [initial MERGE] or [MATCH ... MERGE relationship]
	segments := e.splitMergeChainSegments(cypher)
	if len(segments) == 0 {
		return nil, fmt.Errorf("invalid MERGE...WITH chain: no segments found")
	}

	// Context to track bound variables (node variable -> *storage.Node)
	nodeContext := make(map[string]*storage.Node)
	relContext := make(map[string]*storage.Edge)

	// Track if chain is broken (a MATCH returned 0 rows)
	chainBroken := false

	// Process each segment
	for i, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}

		upperSeg := strings.ToUpper(segment)

		if i == 0 {
			// First segment: MERGE (node) [ON CREATE SET ...] [ON MATCH SET ...]
			// Execute the initial MERGE to create/find the node
			mergedNode, varName, err := e.executeMergeNodeSegment(ctx, segment)
			if err != nil {
				return nil, fmt.Errorf("initial MERGE failed: %w", err)
			}
			if mergedNode != nil && varName != "" {
				nodeContext[varName] = mergedNode
				result.Stats.NodesCreated++ // May be 0 if node existed
			}
		} else if strings.HasPrefix(upperSeg, "RETURN") {
			// RETURN segment: build final result
			if chainBroken {
				// Chain broken - return 0 rows
				returnClause := strings.TrimSpace(segment[6:])
				items := e.parseReturnItems(returnClause)
				for _, item := range items {
					if item.alias != "" {
						result.Columns = append(result.Columns, item.alias)
					} else {
						result.Columns = append(result.Columns, item.expr)
					}
				}
				// No rows - chain was broken
				return result, nil
			}

			// Build result from context
			returnClause := strings.TrimSpace(segment[6:])
			items := e.parseReturnItems(returnClause)

			row := make([]interface{}, len(items))
			for i, item := range items {
				if item.alias != "" {
					result.Columns = append(result.Columns, item.alias)
				} else {
					result.Columns = append(result.Columns, item.expr)
				}
				row[i] = e.evaluateExpressionWithContext(item.expr, nodeContext, relContext)
			}
			result.Rows = append(result.Rows, row)
		} else {
			// Segment after WITH: starts with a WITH projection (e.g., "e") followed by one or more clauses.
			// Example:
			//   WITH e
			//   OPTIONAL MATCH (a:TypeA {name: 'A1'})
			//   FOREACH (...)
			//
			// We apply WITH semantics by filtering context to only passed variables, then execute clauses in order.

			segmentNodeCtx := nodeContext
			segmentRelCtx := relContext

			remaining, newNodeCtx, newRelCtx := e.applyWithProjection(segment, segmentNodeCtx, segmentRelCtx)
			segmentNodeCtx = newNodeCtx
			segmentRelCtx = newRelCtx

			clauses := splitMergeChainClauseBlock(remaining)
			for _, clause := range clauses {
				if strings.TrimSpace(clause) == "" {
					continue
				}
				upperClause := strings.ToUpper(strings.TrimSpace(clause))

				// If chain is broken, we must still allow the final RETURN segment to produce 0 rows
				// (handled above), but all intermediate updates/clauses are skipped.
				if chainBroken {
					continue
				}

				switch {
				case strings.HasPrefix(upperClause, "OPTIONAL MATCH"):
					matchedNode, matchVarName, err := e.executeMatchSegment(ctx, clause, segmentNodeCtx)
					if err != nil {
						// OPTIONAL MATCH errors still break execution (Neo4j would error)
						return nil, err
					}
					if matchVarName != "" {
						segmentNodeCtx[matchVarName] = matchedNode // may be nil
					}
				case strings.HasPrefix(upperClause, "MATCH"):
					matchedNode, matchVarName, err := e.executeMatchSegment(ctx, clause, segmentNodeCtx)
					if err != nil {
						chainBroken = true
						continue
					}
					if matchedNode == nil {
						chainBroken = true
						continue
					}
					if matchVarName != "" {
						segmentNodeCtx[matchVarName] = matchedNode
					}

					// Check for MERGE relationship in this clause (MATCH ... MERGE ...)
					mergeIdx := findKeywordIndex(clause, "MERGE")
					if mergeIdx > 0 {
						mergePart := strings.TrimSpace(clause[mergeIdx+5:])
						if strings.Contains(mergePart, "-[") || strings.Contains(mergePart, "]-") {
							err := e.executeMergeRelSegment(ctx, mergePart, segmentNodeCtx)
							if err == nil {
								result.Stats.RelationshipsCreated++
							}
						}
					}
				case strings.HasPrefix(upperClause, "FOREACH"):
					_, err := e.executeForeachWithContext(ctx, clause, segmentNodeCtx, segmentRelCtx)
					if err != nil {
						return nil, err
					}
				}
			}

			// Persist segment context back to main context for subsequent segments.
			nodeContext = segmentNodeCtx
			relContext = segmentRelCtx
		}
	}

	return result, nil
}

// applyWithProjection applies WITH semantics to a MERGE chain segment.
//
// The input segment is the text between "WITH" and the next "WITH"/"RETURN",
// i.e. it starts with a projection list (e.g., "e") followed by the next clause.
// It returns the remaining clause block plus filtered contexts.
func (e *StorageExecutor) applyWithProjection(segment string, nodeCtx map[string]*storage.Node, relCtx map[string]*storage.Edge) (remaining string, newNodeCtx map[string]*storage.Node, newRelCtx map[string]*storage.Edge) {
	segment = strings.TrimSpace(segment)
	if segment == "" {
		return "", nodeCtx, relCtx
	}

	keywords := []string{"OPTIONAL MATCH", "MATCH", "FOREACH", "RETURN"}
	nextClausePos := -1
	for _, kw := range keywords {
		if idx := findKeywordIndex(segment, kw); idx >= 0 {
			if nextClausePos == -1 || idx < nextClausePos {
				nextClausePos = idx
			}
		}
	}
	if nextClausePos == -1 {
		// No further clause keywords - treat entire segment as projection list.
		nextClausePos = len(segment)
	}

	withPart := strings.TrimSpace(segment[:nextClausePos])
	remaining = strings.TrimSpace(segment[nextClausePos:])

	// WITH * keeps everything.
	if strings.TrimSpace(withPart) == "*" {
		return remaining, nodeCtx, relCtx
	}

	items := e.parseReturnItems(withPart)
	if len(items) == 0 {
		// If we can't parse, avoid dropping context.
		return remaining, nodeCtx, relCtx
	}

	newNodeCtx = make(map[string]*storage.Node)
	newRelCtx = make(map[string]*storage.Edge)
	for _, item := range items {
		name := strings.TrimSpace(item.expr)
		if item.alias != "" {
			name = strings.TrimSpace(item.alias)
		}
		if name == "" {
			continue
		}
		if n, ok := nodeCtx[name]; ok {
			newNodeCtx[name] = n
		}
		if r, ok := relCtx[name]; ok {
			newRelCtx[name] = r
		}
	}

	return remaining, newNodeCtx, newRelCtx
}

func splitMergeChainClauseBlock(block string) []string {
	block = strings.TrimSpace(block)
	if block == "" {
		return nil
	}

	keywords := []string{"OPTIONAL MATCH", "MATCH", "FOREACH", "RETURN"}

	// Find the first clause start at top level.
	start := -1
	for _, kw := range keywords {
		if idx := findKeywordIndex(block, kw); idx >= 0 {
			if start == -1 || idx < start {
				start = idx
			}
		}
	}
	if start == -1 {
		return []string{block}
	}
	if start > 0 {
		block = strings.TrimSpace(block[start:])
	}

	var clauses []string
	pos := 0
	for pos < len(block) {
		// Identify which keyword starts here.
		var currentKw string
		for _, kw := range keywords {
			if findKeywordIndex(block[pos:], kw) == 0 {
				currentKw = kw
				break
			}
		}
		if currentKw == "" {
			// Skip unknown leading content until the next recognized keyword.
			next := -1
			for _, kw := range keywords {
				if idx := findKeywordIndex(block[pos:], kw); idx > 0 {
					if next == -1 || idx < next {
						next = idx
					}
				}
			}
			if next == -1 {
				clauses = append(clauses, strings.TrimSpace(block[pos:]))
				break
			}
			pos += next
			continue
		}

		// Find the next clause start.
		searchFrom := pos + len(currentKw)
		nextStart := -1
		for _, kw := range keywords {
			if idx := findKeywordIndex(block[searchFrom:], kw); idx >= 0 {
				abs := searchFrom + idx
				if abs > pos && (nextStart == -1 || abs < nextStart) {
					nextStart = abs
				}
			}
		}

		if nextStart == -1 {
			clauses = append(clauses, strings.TrimSpace(block[pos:]))
			break
		}

		clauses = append(clauses, strings.TrimSpace(block[pos:nextStart]))
		pos = nextStart
	}

	return clauses
}

// splitMergeChainSegments splits a MERGE...WITH...MATCH chain into segments.
// Returns segments like: ["MERGE (e:Entry...) ON CREATE SET...", "MATCH (c:Cat...) MERGE (e)-[:REL]->(c)", "RETURN..."]
func (e *StorageExecutor) splitMergeChainSegments(cypher string) []string {
	var segments []string

	// Find all WITH positions
	var withPositions []int
	searchPos := 0
	for {
		idx := findKeywordIndex(cypher[searchPos:], "WITH")
		if idx == -1 {
			break
		}
		// Check it's not "STARTS WITH" or "ENDS WITH"
		actualPos := searchPos + idx
		if actualPos > 6 {
			before := strings.ToUpper(cypher[actualPos-6 : actualPos])
			if strings.HasSuffix(strings.TrimSpace(before), "STARTS") || strings.HasSuffix(strings.TrimSpace(before), "ENDS") {
				searchPos = actualPos + 4
				continue
			}
		}
		withPositions = append(withPositions, actualPos)
		searchPos = actualPos + 4
	}

	// Find RETURN position
	returnIdx := findKeywordIndex(cypher, "RETURN")

	if len(withPositions) == 0 {
		// No WITH clauses - return whole query
		return []string{cypher}
	}

	// First segment: from start to first WITH
	segments = append(segments, strings.TrimSpace(cypher[:withPositions[0]]))

	// Middle segments: between WITH clauses
	for i := 0; i < len(withPositions); i++ {
		// Skip the WITH keyword and find the content after it
		startPos := withPositions[i] + 4 // Skip "WITH"

		// Find where this segment ends
		var endPos int
		if i+1 < len(withPositions) {
			endPos = withPositions[i+1]
		} else if returnIdx > startPos {
			endPos = returnIdx
		} else {
			endPos = len(cypher)
		}

		// Preserve everything after WITH so we can apply WITH semantics and execute
		// OPTIONAL MATCH/FOREACH patterns inside the segment.
		segmentContent := strings.TrimSpace(cypher[startPos:endPos])
		if segmentContent != "" {
			segments = append(segments, segmentContent)
		}
	}

	// Add RETURN segment if present
	if returnIdx > 0 {
		segments = append(segments, strings.TrimSpace(cypher[returnIdx:]))
	}

	return segments
}

// executeMergeNodeSegment executes the initial MERGE (node) part and returns the node and variable name.
func (e *StorageExecutor) executeMergeNodeSegment(ctx context.Context, segment string) (*storage.Node, string, error) {
	store := e.getStorage(ctx)
	// Parse: MERGE (varName:Label {props}) [ON CREATE SET ...] [ON MATCH SET ...]
	mergeIdx := findKeywordIndex(segment, "MERGE")
	if mergeIdx == -1 {
		return nil, "", fmt.Errorf("MERGE not found in segment")
	}

	// Find the pattern end (ON CREATE, ON MATCH, or end of segment)
	patternEnd := len(segment)
	for _, keyword := range []string{"ON CREATE", "ON MATCH"} {
		idx := findKeywordIndex(segment, keyword)
		if idx > 0 && idx < patternEnd {
			patternEnd = idx
		}
	}

	pattern := strings.TrimSpace(segment[mergeIdx+5 : patternEnd])

	// Parse the pattern
	varName, labels, props, err := e.parseMergePattern(pattern)
	if err != nil {
		return nil, "", err
	}

	// Try to find existing node
	var existingNode *storage.Node
	if len(labels) > 0 && len(props) > 0 {
		nodes, _ := store.GetNodesByLabel(labels[0])
		for _, n := range nodes {
			matches := true
			for key, val := range props {
				if nodeVal, ok := n.Properties[key]; !ok || fmt.Sprintf("%v", nodeVal) != fmt.Sprintf("%v", val) {
					matches = false
					break
				}
			}
			if matches {
				existingNode = n
				break
			}
		}
	}

	var node *storage.Node
	if existingNode != nil {
		node = existingNode
		// Apply ON MATCH SET if present
		onMatchIdx := findKeywordIndex(segment, "ON MATCH SET")
		if onMatchIdx > 0 {
			setEnd := len(segment)
			onCreateIdx := findKeywordIndex(segment, "ON CREATE SET")
			if onCreateIdx > onMatchIdx {
				setEnd = onCreateIdx
			}
			setClause := strings.TrimSpace(segment[onMatchIdx+12 : setEnd])
			e.applySetToNode(node, varName, setClause)
			store.UpdateNode(node)
			e.notifyNodeMutated(string(node.ID))
		}
	} else {
		// Create new node
		node = &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("node-%d", e.idCounter())),
			Labels:     labels,
			Properties: props,
		}
		actualID, err := store.CreateNode(node)
		if err != nil {
			return nil, "", fmt.Errorf("failed to create node: %w", err)
		}
		node.ID = actualID
		e.notifyNodeMutated(string(node.ID))

		// Apply ON CREATE SET if present
		onCreateIdx := findKeywordIndex(segment, "ON CREATE SET")
		if onCreateIdx > 0 {
			setEnd := len(segment)
			onMatchIdx := findKeywordIndex(segment, "ON MATCH SET")
			if onMatchIdx > onCreateIdx {
				setEnd = onMatchIdx
			}
			setClause := strings.TrimSpace(segment[onCreateIdx+13 : setEnd])
			e.applySetToNode(node, varName, setClause)
			store.UpdateNode(node)
			e.notifyNodeMutated(string(node.ID))
		}
	}

	return node, varName, nil
}

// executeMatchSegment executes a MATCH segment and returns the matched node.
func (e *StorageExecutor) executeMatchSegment(ctx context.Context, segment string, nodeContext map[string]*storage.Node) (*storage.Node, string, error) {
	store := e.getStorage(ctx)
	// Parse: MATCH (varName:Label {props}) [MERGE ...]
	matchIdx := findKeywordIndex(segment, "MATCH")
	if matchIdx == -1 {
		return nil, "", fmt.Errorf("MATCH not found in segment")
	}

	// Find the pattern end (MERGE or end of segment)
	patternEnd := len(segment)
	mergeIdx := findKeywordIndex(segment, "MERGE")
	if mergeIdx > 0 {
		patternEnd = mergeIdx
	}

	pattern := strings.TrimSpace(segment[matchIdx+5 : patternEnd])

	// Parse the node pattern
	nodePattern := e.parseNodePattern(pattern)
	if nodePattern.variable == "" && len(nodePattern.labels) == 0 {
		return nil, "", fmt.Errorf("could not parse node pattern: %s", pattern)
	}

	// Check if variable is already bound
	if boundNode, exists := nodeContext[nodePattern.variable]; exists {
		return boundNode, nodePattern.variable, nil
	}

	// Find matching node
	var nodes []*storage.Node
	var err error
	if len(nodePattern.labels) > 0 {
		nodes, err = store.GetNodesByLabel(nodePattern.labels[0])
	} else {
		nodes, err = store.AllNodes()
	}
	if err != nil {
		return nil, "", err
	}

	// Filter by properties
	for _, n := range nodes {
		matches := true
		for key, val := range nodePattern.properties {
			if nodeVal, ok := n.Properties[key]; !ok || fmt.Sprintf("%v", nodeVal) != fmt.Sprintf("%v", val) {
				matches = false
				break
			}
		}
		if matches {
			return n, nodePattern.variable, nil
		}
	}

	// No match found
	return nil, nodePattern.variable, nil
}

// executeMergeRelSegment executes a MERGE relationship segment like (e)-[:REL]->(c)
func (e *StorageExecutor) executeMergeRelSegment(ctx context.Context, pattern string, nodeContext map[string]*storage.Node) error {
	store := e.getStorage(ctx)
	// Parse relationship pattern: (startVar)-[:TYPE]->(endVar) or (startVar)-[:TYPE {props}]->(endVar)
	pattern = strings.TrimSpace(pattern)

	// Extract start node variable
	startParen := strings.Index(pattern, "(")
	if startParen == -1 {
		return fmt.Errorf("invalid relationship pattern: missing start node in %q", pattern)
	}

	endStartParen := strings.Index(pattern[startParen+1:], ")")
	if endStartParen == -1 {
		return fmt.Errorf("invalid relationship pattern: missing start node closing paren in %q", pattern)
	}
	startVar := strings.TrimSpace(pattern[startParen+1 : startParen+1+endStartParen])

	// Find the relationship part -[...]->
	relStart := strings.Index(pattern, "-[")
	relEnd := strings.Index(pattern, "]->")
	if relEnd == -1 {
		relEnd = strings.Index(pattern, "]-")
	}
	if relStart == -1 || relEnd == -1 {
		return fmt.Errorf("invalid relationship pattern: missing relationship brackets (expected -[type]-> or -[type]-) in %q", pattern)
	}

	relContent := pattern[relStart+2 : relEnd]

	// Parse relationship type and properties
	var relType string
	relProps := make(map[string]interface{})

	if colonIdx := strings.Index(relContent, ":"); colonIdx >= 0 {
		afterColon := relContent[colonIdx+1:]
		if braceIdx := strings.Index(afterColon, "{"); braceIdx > 0 {
			relType = strings.TrimSpace(afterColon[:braceIdx])
			// Parse properties (simplified)
		} else {
			relType = strings.TrimSpace(afterColon)
		}
	}

	// Extract end node variable
	// Find the last (var) pattern
	lastParenStart := strings.LastIndex(pattern, "(")
	lastParenEnd := strings.LastIndex(pattern, ")")
	if lastParenStart == -1 || lastParenEnd == -1 || lastParenEnd < lastParenStart {
		return fmt.Errorf("invalid relationship pattern: missing end node in %q", pattern)
	}
	endVar := strings.TrimSpace(pattern[lastParenStart+1 : lastParenEnd])

	// Look up nodes in context
	startNode, startExists := nodeContext[startVar]
	endNode, endExists := nodeContext[endVar]

	if !startExists {
		return fmt.Errorf("start node variable '%s' not in context (available: %v)", startVar, getKeys(nodeContext))
	}
	if !endExists {
		return fmt.Errorf("end node variable '%s' not in context (available: %v)", endVar, getKeys(nodeContext))
	}

	// Check if relationship already exists
	edges, _ := store.GetOutgoingEdges(startNode.ID)
	for _, edge := range edges {
		if edge.Type == relType && edge.EndNode == endNode.ID {
			// Relationship already exists
			return nil
		}
	}

	// Create the relationship
	edge := &storage.Edge{
		ID:         storage.EdgeID(fmt.Sprintf("edge-%d", e.idCounter())),
		Type:       relType,
		StartNode:  startNode.ID,
		EndNode:    endNode.ID,
		Properties: relProps,
	}

	return store.CreateEdge(edge)
}

// executeMultipleMerges handles queries with multiple MERGE statements without WITH:
//
//	MERGE (e:Entry {key: 'x'})
//	MERGE (f:Category {name: 'y'})
//	MERGE (e)-[:REL]->(f)
//	RETURN e.key, f.name
//
// Each MERGE is executed in sequence, building a context of bound variables.
// Relationship MERGEs use variables from previous node MERGEs.
func (e *StorageExecutor) executeMultipleMerges(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	// Context to track bound variables
	nodeContext := make(map[string]*storage.Node)
	relContext := make(map[string]*storage.Edge)

	// Split into MERGE segments
	segments := e.splitMultipleMerges(cypher)

	// Process each MERGE segment
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		upperSeg := strings.ToUpper(segment)

		if strings.HasPrefix(upperSeg, "MERGE") {
			mergeContent := strings.TrimSpace(segment[5:])

			// Check if this is a relationship MERGE
			if strings.Contains(mergeContent, "-[") || strings.Contains(mergeContent, "]-") {
				// Relationship MERGE
				err := e.executeMergeRelSegment(ctx, mergeContent, nodeContext)
				if err != nil {
					return nil, fmt.Errorf("relationship MERGE failed: %w", err)
				}
				result.Stats.RelationshipsCreated++
			} else {
				// Node MERGE
				node, varName, err := e.executeMergeNodeSegment(ctx, segment)
				if err != nil {
					return nil, fmt.Errorf("node MERGE failed: %w", err)
				}
				if node != nil && varName != "" {
					nodeContext[varName] = node
				}
			}
		} else if strings.HasPrefix(upperSeg, "RETURN") {
			// Build result from context
			returnClause := strings.TrimSpace(segment[6:])
			items := e.parseReturnItems(returnClause)

			row := make([]interface{}, len(items))
			for i, item := range items {
				if item.alias != "" {
					result.Columns = append(result.Columns, item.alias)
				} else {
					result.Columns = append(result.Columns, item.expr)
				}
				row[i] = e.evaluateExpressionWithContext(item.expr, nodeContext, relContext)
			}
			result.Rows = append(result.Rows, row)
		}
	}

	return result, nil
}

// splitMultipleMerges splits a query into MERGE and RETURN segments.
func (e *StorageExecutor) splitMultipleMerges(cypher string) []string {
	var segments []string

	// Find all MERGE positions
	var mergePositions []int
	searchPos := 0
	for {
		idx := findKeywordIndex(cypher[searchPos:], "MERGE")
		if idx == -1 {
			break
		}
		mergePositions = append(mergePositions, searchPos+idx)
		searchPos = searchPos + idx + 5
	}

	// Find RETURN position
	returnIdx := findKeywordIndex(cypher, "RETURN")

	// Build segments
	for i, pos := range mergePositions {
		var endPos int
		if i+1 < len(mergePositions) {
			endPos = mergePositions[i+1]
		} else if returnIdx > pos {
			endPos = returnIdx
		} else {
			endPos = len(cypher)
		}
		segments = append(segments, strings.TrimSpace(cypher[pos:endPos]))
	}

	// Add RETURN segment
	if returnIdx > 0 {
		segments = append(segments, strings.TrimSpace(cypher[returnIdx:]))
	}

	return segments
}

// parseMergePattern parses a MERGE pattern like "(n:Label {prop: value})"
