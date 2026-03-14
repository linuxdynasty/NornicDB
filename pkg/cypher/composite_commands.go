// Package cypher provides composite database command execution.
package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func parseCypherValueToken(tok string) (string, error) {
	tok = strings.TrimSpace(tok)
	if tok == "" {
		return "", nil
	}
	if len(tok) >= 2 {
		if (tok[0] == '\'' && tok[len(tok)-1] == '\'') || (tok[0] == '"' && tok[len(tok)-1] == '"') {
			return tok[1 : len(tok)-1], nil
		}
	}
	if strings.HasPrefix(tok, "`") {
		return unquoteBacktickIdentifier(tok)
	}
	return tok, nil
}

func parseConstituentFromTokens(tokens []string, idx *int) (map[string]interface{}, error) {
	if *idx >= len(tokens) || !strings.EqualFold(tokens[*idx], "ALIAS") {
		return nil, fmt.Errorf("invalid constituent syntax: ALIAS expected")
	}
	*idx = *idx + 1

	if *idx >= len(tokens) {
		return nil, fmt.Errorf("invalid constituent syntax: alias name cannot be empty")
	}
	aliasName, err := parseCypherValueToken(tokens[*idx])
	if err != nil {
		return nil, fmt.Errorf("invalid constituent syntax: alias name: %w", err)
	}
	*idx = *idx + 1
	if strings.TrimSpace(aliasName) == "" {
		return nil, fmt.Errorf("invalid constituent syntax: alias name cannot be empty")
	}

	if *idx+1 >= len(tokens) || !strings.EqualFold(tokens[*idx], "FOR") || !strings.EqualFold(tokens[*idx+1], "DATABASE") {
		return nil, fmt.Errorf("invalid constituent syntax: FOR DATABASE expected")
	}
	*idx += 2

	if *idx >= len(tokens) {
		return nil, fmt.Errorf("invalid constituent syntax: database name cannot be empty")
	}
	constituentDbName, err := parseCypherValueToken(tokens[*idx])
	if err != nil {
		return nil, fmt.Errorf("invalid constituent syntax: database name: %w", err)
	}
	*idx = *idx + 1
	if strings.TrimSpace(constituentDbName) == "" {
		return nil, fmt.Errorf("invalid constituent syntax: database name cannot be empty")
	}

	ref := map[string]interface{}{
		"alias":         aliasName,
		"database_name": constituentDbName,
		"type":          "local",
		"access_mode":   "read_write",
	}
	hasUserPassword := false
	hasOIDCForwarding := false

	for *idx < len(tokens) {
		if strings.EqualFold(tokens[*idx], "ALIAS") {
			break
		}

		switch {
		case strings.EqualFold(tokens[*idx], "AT"):
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, fmt.Errorf("invalid constituent syntax: remote URI expected after AT")
			}
			uri, err := parseCypherValueToken(tokens[*idx])
			if err != nil {
				return nil, fmt.Errorf("invalid constituent syntax: remote URI: %w", err)
			}
			*idx = *idx + 1
			if strings.TrimSpace(uri) == "" {
				return nil, fmt.Errorf("invalid constituent syntax: remote URI cannot be empty")
			}
			ref["uri"] = uri
			ref["type"] = "remote"

		case strings.EqualFold(tokens[*idx], "USER"):
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, fmt.Errorf("invalid constituent syntax: user cannot be empty")
			}
			user, err := parseCypherValueToken(tokens[*idx])
			if err != nil {
				return nil, fmt.Errorf("invalid constituent syntax: user: %w", err)
			}
			*idx = *idx + 1
			if strings.TrimSpace(user) == "" {
				return nil, fmt.Errorf("invalid constituent syntax: user cannot be empty")
			}
			ref["user"] = user
			hasUserPassword = true

		case strings.EqualFold(tokens[*idx], "PASSWORD"):
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, fmt.Errorf("invalid constituent syntax: password cannot be empty")
			}
			password, err := parseCypherValueToken(tokens[*idx])
			if err != nil {
				return nil, fmt.Errorf("invalid constituent syntax: password: %w", err)
			}
			*idx = *idx + 1
			if strings.TrimSpace(password) == "" {
				return nil, fmt.Errorf("invalid constituent syntax: password cannot be empty")
			}
			ref["password"] = password
			hasUserPassword = true

		case strings.EqualFold(tokens[*idx], "OIDC"):
			*idx = *idx + 1
			if *idx+1 >= len(tokens) || !strings.EqualFold(tokens[*idx], "CREDENTIAL") || !strings.EqualFold(tokens[*idx+1], "FORWARDING") {
				return nil, fmt.Errorf("invalid constituent syntax: OIDC CREDENTIAL FORWARDING expected")
			}
			*idx += 2
			hasOIDCForwarding = true

		case strings.EqualFold(tokens[*idx], "SECRET"):
			*idx = *idx + 1
			if *idx >= len(tokens) || !strings.EqualFold(tokens[*idx], "REF") {
				return nil, fmt.Errorf("invalid constituent syntax: SECRET REF expected")
			}
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, fmt.Errorf("invalid constituent syntax: secret ref cannot be empty")
			}
			secretRef, err := parseCypherValueToken(tokens[*idx])
			if err != nil {
				return nil, fmt.Errorf("invalid constituent syntax: secret ref: %w", err)
			}
			*idx = *idx + 1
			if strings.TrimSpace(secretRef) == "" {
				return nil, fmt.Errorf("invalid constituent syntax: secret ref cannot be empty")
			}
			ref["secret_ref"] = secretRef

		case strings.EqualFold(tokens[*idx], "TYPE"):
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, fmt.Errorf("invalid constituent syntax: type cannot be empty")
			}
			typeVal, err := parseCypherValueToken(tokens[*idx])
			if err != nil {
				return nil, fmt.Errorf("invalid constituent syntax: type: %w", err)
			}
			*idx = *idx + 1
			typeVal = strings.ToLower(strings.TrimSpace(typeVal))
			if typeVal != "local" && typeVal != "remote" {
				return nil, fmt.Errorf("invalid constituent syntax: type must be local or remote")
			}
			// Reject contradictory AT + TYPE local: AT implies remote.
			if existingType, ok := ref["type"]; ok && existingType == "remote" && typeVal == "local" {
				return nil, fmt.Errorf("invalid constituent syntax: TYPE local contradicts AT (remote URI already specified)")
			}
			ref["type"] = typeVal

		case strings.EqualFold(tokens[*idx], "ACCESS"):
			*idx = *idx + 1
			if *idx >= len(tokens) {
				return nil, fmt.Errorf("invalid constituent syntax: access mode cannot be empty")
			}
			accessVal, err := parseCypherValueToken(tokens[*idx])
			if err != nil {
				return nil, fmt.Errorf("invalid constituent syntax: access mode: %w", err)
			}
			*idx = *idx + 1
			accessVal = strings.ToLower(strings.TrimSpace(accessVal))
			switch accessVal {
			case "read", "write", "read_write":
				ref["access_mode"] = accessVal
			default:
				return nil, fmt.Errorf("invalid constituent syntax: access mode must be read, write, or read_write")
			}

		default:
			return nil, fmt.Errorf("invalid constituent syntax: unexpected token '%s'", tokens[*idx])
		}
	}

	if t, _ := ref["type"].(string); t == "remote" {
		switch {
		case hasOIDCForwarding && hasUserPassword:
			return nil, fmt.Errorf("invalid constituent syntax: cannot combine OIDC CREDENTIAL FORWARDING with USER/PASSWORD")
		case hasUserPassword:
			user, _ := ref["user"].(string)
			password, _ := ref["password"].(string)
			user = strings.TrimSpace(user)
			password = strings.TrimSpace(password)
			if user == "" || password == "" {
				return nil, fmt.Errorf("invalid constituent syntax: USER and PASSWORD must both be provided")
			}
			ref["auth_mode"] = "user_password"
		default:
			ref["auth_mode"] = "oidc_forwarding"
		}
	} else if hasUserPassword || hasOIDCForwarding {
		return nil, fmt.Errorf("invalid constituent syntax: USER/PASSWORD and OIDC CREDENTIAL FORWARDING require a remote constituent (AT '<url>' or TYPE remote)")
	}

	return ref, nil
}

// executeCreateCompositeDatabase handles CREATE COMPOSITE DATABASE command.
//
// Syntax:
//
//	CREATE COMPOSITE DATABASE name [IF NOT EXISTS]
//	  ALIAS alias1 FOR DATABASE db1
//	  ALIAS alias2 FOR DATABASE db2
//	  ...
//
// Example:
//
//	CREATE COMPOSITE DATABASE analytics
//	  ALIAS tenant_a FOR DATABASE tenant_a
//	  ALIAS tenant_b FOR DATABASE tenant_b
//
//	CREATE COMPOSITE DATABASE analytics IF NOT EXISTS
//	  ALIAS tenant_a FOR DATABASE tenant_a
func (e *StorageExecutor) executeCreateCompositeDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - CREATE COMPOSITE DATABASE requires multi-database support")
	}

	// Find "CREATE COMPOSITE DATABASE" keyword position
	createIdx := findMultiWordKeywordIndex(cypher, "CREATE", "COMPOSITE DATABASE")
	if createIdx == -1 {
		return nil, fmt.Errorf("invalid CREATE COMPOSITE DATABASE syntax")
	}

	// Skip "CREATE COMPOSITE DATABASE" and whitespace
	startPos := createIdx + len("CREATE")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "COMPOSITE DATABASE"
	if startPos+len("COMPOSITE DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE DATABASE")], "COMPOSITE DATABASE") {
		startPos += len("COMPOSITE DATABASE")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	} else {
		// Try with flexible whitespace
		if startPos+len("COMPOSITE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE")], "COMPOSITE") {
			startPos += len("COMPOSITE")
			for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
				startPos++
			}
			if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
				startPos += len("DATABASE")
				for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
					startPos++
				}
			}
		}
	}

	if startPos >= len(cypher) {
		return nil, fmt.Errorf("invalid CREATE COMPOSITE DATABASE syntax: database name expected")
	}

	// Extract composite database name (until newline or end)
	dbNameEnd := startPos
	for dbNameEnd < len(cypher) && !isWhitespace(cypher[dbNameEnd]) && cypher[dbNameEnd] != '\n' {
		dbNameEnd++
	}

	dbName := strings.TrimSpace(cypher[startPos:dbNameEnd])
	if dbName == "" {
		return nil, fmt.Errorf("invalid CREATE COMPOSITE DATABASE syntax: database name cannot be empty")
	}

	// Check for IF NOT EXISTS after database name.
	ifNotExists := false
	remaining := strings.TrimSpace(cypher[dbNameEnd:])
	upperRemaining := strings.ToUpper(remaining)
	if strings.HasPrefix(upperRemaining, "IF NOT EXISTS") {
		ifNotExists = true
		remaining = strings.TrimSpace(remaining[len("IF NOT EXISTS"):])
	}

	// If IF NOT EXISTS and database already exists, return success silently.
	if ifNotExists && e.dbManager != nil && e.dbManager.IsCompositeDatabase(dbName) {
		return &ExecuteResult{
			Columns: []string{"name"},
			Rows:    [][]interface{}{{dbName}},
		}, nil
	}
	// Also handle IF NOT EXISTS when a standard database with that name exists.
	if ifNotExists && e.dbManager != nil && e.dbManager.Exists(dbName) {
		return &ExecuteResult{
			Columns: []string{"name"},
			Rows:    [][]interface{}{{dbName}},
		}, nil
	}

	// Parse constituents (ALIAS ... FOR DATABASE ... [AT ...] [SECRET REF ...])
	constituents := []interface{}{}
	if remaining != "" {
		tokens, err := tokenize(remaining)
		if err != nil {
			return nil, fmt.Errorf("invalid CREATE COMPOSITE DATABASE syntax: %w", err)
		}
		idx := 0
		for idx < len(tokens) {
			ref, err := parseConstituentFromTokens(tokens, &idx)
			if err != nil {
				return nil, err
			}
			constituents = append(constituents, ref)
		}
	}

	if len(constituents) == 0 {
		return nil, fmt.Errorf("invalid CREATE COMPOSITE DATABASE syntax: at least one constituent required")
	}

	// Create composite database
	err := e.dbManager.CreateCompositeDatabase(dbName, constituents)
	if err != nil {
		return nil, fmt.Errorf("failed to create composite database '%s': %w", dbName, err)
	}

	return &ExecuteResult{
		Columns: []string{"name"},
		Rows:    [][]interface{}{{dbName}},
	}, nil
}

// executeDropCompositeDatabase handles DROP COMPOSITE DATABASE command.
func (e *StorageExecutor) executeDropCompositeDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - DROP COMPOSITE DATABASE requires multi-database support")
	}

	// Find "DROP COMPOSITE DATABASE" keyword position
	dropIdx := findMultiWordKeywordIndex(cypher, "DROP", "COMPOSITE DATABASE")
	if dropIdx == -1 {
		return nil, fmt.Errorf("invalid DROP COMPOSITE DATABASE syntax")
	}

	// Skip "DROP COMPOSITE DATABASE" and whitespace
	startPos := dropIdx + len("DROP")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "COMPOSITE DATABASE"
	if startPos+len("COMPOSITE DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE DATABASE")], "COMPOSITE DATABASE") {
		startPos += len("COMPOSITE DATABASE")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	} else {
		// Try with flexible whitespace
		if startPos+len("COMPOSITE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE")], "COMPOSITE") {
			startPos += len("COMPOSITE")
			for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
				startPos++
			}
			if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
				startPos += len("DATABASE")
				for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
					startPos++
				}
			}
		}
	}

	if startPos >= len(cypher) {
		return nil, fmt.Errorf("invalid DROP COMPOSITE DATABASE syntax: database name expected")
	}

	// Extract database name
	dbName := strings.TrimSpace(cypher[startPos:])
	dbName = strings.ReplaceAll(dbName, " ", "")
	dbName = strings.ReplaceAll(dbName, "\t", "")
	dbName = strings.ReplaceAll(dbName, "\n", "")
	dbName = strings.ReplaceAll(dbName, "\r", "")

	if dbName == "" {
		return nil, fmt.Errorf("invalid DROP COMPOSITE DATABASE syntax: database name cannot be empty")
	}

	// Drop composite database
	err := e.dbManager.DropCompositeDatabase(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to drop composite database '%s': %w", dbName, err)
	}

	return &ExecuteResult{
		Columns: []string{"name"},
		Rows:    [][]interface{}{{dbName}},
	}, nil
}

// executeShowCompositeDatabases handles SHOW COMPOSITE DATABASES command.
func (e *StorageExecutor) executeShowCompositeDatabases(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - SHOW COMPOSITE DATABASES requires multi-database support")
	}

	compositeDbs := e.dbManager.ListCompositeDatabases()

	rows := make([][]interface{}, len(compositeDbs))
	for i, db := range compositeDbs {
		rows[i] = []interface{}{db.Name(), db.Type(), db.Status()}
	}

	return &ExecuteResult{
		Columns: []string{"name", "type", "status"},
		Rows:    rows,
	}, nil
}

// executeShowConstituents handles SHOW CONSTITUENTS FOR COMPOSITE DATABASE command.
func (e *StorageExecutor) executeShowConstituents(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - SHOW CONSTITUENTS requires multi-database support")
	}

	// Find "SHOW CONSTITUENTS" keyword position
	showIdx := findMultiWordKeywordIndex(cypher, "SHOW", "CONSTITUENTS")
	if showIdx == -1 {
		return nil, fmt.Errorf("invalid SHOW CONSTITUENTS syntax")
	}

	// Check for "FOR COMPOSITE DATABASE"
	forIdx := findMultiWordKeywordIndex(cypher, "FOR", "COMPOSITE DATABASE")
	var compositeName string

	if forIdx >= 0 {
		// Extract composite database name
		startPos := forIdx + len("FOR")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
		// Skip "COMPOSITE DATABASE"
		if startPos+len("COMPOSITE DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE DATABASE")], "COMPOSITE DATABASE") {
			startPos += len("COMPOSITE DATABASE")
			for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
				startPos++
			}
		} else {
			// Try with flexible whitespace
			if startPos+len("COMPOSITE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE")], "COMPOSITE") {
				startPos += len("COMPOSITE")
				for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
					startPos++
				}
				if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
					startPos += len("DATABASE")
					for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
						startPos++
					}
				}
			}
		}

		compositeName = strings.TrimSpace(cypher[startPos:])
		compositeName = strings.ReplaceAll(compositeName, " ", "")
		compositeName = strings.ReplaceAll(compositeName, "\t", "")
		compositeName = strings.ReplaceAll(compositeName, "\n", "")
		compositeName = strings.ReplaceAll(compositeName, "\r", "")
	}

	if compositeName == "" {
		return nil, fmt.Errorf("invalid SHOW CONSTITUENTS syntax: FOR COMPOSITE DATABASE name expected")
	}

	// Get constituents
	constituents, err := e.dbManager.GetCompositeConstituents(compositeName)
	if err != nil {
		return nil, fmt.Errorf("failed to get constituents: %w", err)
	}

	rows := make([][]interface{}, len(constituents))
	for i, c := range constituents {
		// Handle ConstituentRef type
		if ref, ok := c.(multidb.ConstituentRef); ok {
			rows[i] = []interface{}{
				ref.Alias,
				ref.DatabaseName,
				ref.Type,
				ref.AccessMode,
				ref.URI,
				ref.SecretRef,
				ref.AuthMode,
				ref.User,
			}
		} else if m, ok := c.(map[string]interface{}); ok {
			// Fallback for map format (if returned as map)
			rows[i] = []interface{}{
				m["alias"],
				m["database_name"],
				m["type"],
				m["access_mode"],
				m["uri"],
				m["secret_ref"],
				m["auth_mode"],
				m["user"],
			}
		} else {
			// Unknown type - return empty row
			rows[i] = []interface{}{"", "", "", "", "", "", "", ""}
		}
	}

	return &ExecuteResult{
		Columns: []string{"alias", "database", "type", "access_mode", "uri", "secret_ref", "auth_mode", "user"},
		Rows:    rows,
	}, nil
}

// executeAlterCompositeDatabase handles ALTER COMPOSITE DATABASE command.
//
// Syntax:
//
//	ALTER COMPOSITE DATABASE name
//	  ADD ALIAS alias FOR DATABASE db
//	ALTER COMPOSITE DATABASE name
//	  DROP ALIAS alias
//
// Example:
//
//	ALTER COMPOSITE DATABASE analytics
//	  ADD ALIAS tenant_d FOR DATABASE tenant_d
//
//	ALTER COMPOSITE DATABASE analytics
//	  DROP ALIAS tenant_c
func (e *StorageExecutor) executeAlterCompositeDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - ALTER COMPOSITE DATABASE requires multi-database support")
	}

	// Find "ALTER COMPOSITE DATABASE" keyword position
	// First find "ALTER COMPOSITE", then check for "DATABASE"
	alterIdx := findMultiWordKeywordIndex(cypher, "ALTER", "COMPOSITE")
	if alterIdx == -1 {
		return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax")
	}

	// Check that "DATABASE" follows "COMPOSITE"
	afterComposite := alterIdx + len("ALTER")
	for afterComposite < len(cypher) && isWhitespace(cypher[afterComposite]) {
		afterComposite++
	}
	if afterComposite+len("COMPOSITE") <= len(cypher) && strings.EqualFold(cypher[afterComposite:afterComposite+len("COMPOSITE")], "COMPOSITE") {
		afterComposite += len("COMPOSITE")
		for afterComposite < len(cypher) && isWhitespace(cypher[afterComposite]) {
			afterComposite++
		}
		if afterComposite+len("DATABASE") > len(cypher) || !strings.EqualFold(cypher[afterComposite:afterComposite+len("DATABASE")], "DATABASE") {
			return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax: DATABASE expected after COMPOSITE")
		}
	} else {
		return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax")
	}

	// Skip "ALTER COMPOSITE DATABASE" and whitespace
	startPos := alterIdx + len("ALTER")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "COMPOSITE DATABASE"
	if startPos+len("COMPOSITE DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE DATABASE")], "COMPOSITE DATABASE") {
		startPos += len("COMPOSITE DATABASE")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	} else {
		// Try with flexible whitespace
		if startPos+len("COMPOSITE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("COMPOSITE")], "COMPOSITE") {
			startPos += len("COMPOSITE")
			for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
				startPos++
			}
			if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
				startPos += len("DATABASE")
				for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
					startPos++
				}
			}
		}
	}

	if startPos >= len(cypher) {
		return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax: database name expected")
	}

	// Extract composite database name (until newline or whitespace)
	dbNameEnd := startPos
	for dbNameEnd < len(cypher) && !isWhitespace(cypher[dbNameEnd]) && cypher[dbNameEnd] != '\n' {
		dbNameEnd++
	}

	dbName := strings.TrimSpace(cypher[startPos:dbNameEnd])
	if dbName == "" {
		return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax: database name cannot be empty")
	}

	// Check for ADD or DROP
	remaining := strings.TrimSpace(cypher[dbNameEnd:])
	upperRemaining := strings.ToUpper(remaining)

	if strings.HasPrefix(upperRemaining, "ADD ALIAS") {
		tokens, err := tokenize(remaining)
		if err != nil {
			return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax: %w", err)
		}
		if len(tokens) < 2 || !strings.EqualFold(tokens[0], "ADD") || !strings.EqualFold(tokens[1], "ALIAS") {
			return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax: ADD ALIAS expected")
		}
		idx := 1
		constituent, err := parseConstituentFromTokens(tokens, &idx)
		if err != nil {
			return nil, err
		}
		if idx != len(tokens) {
			return nil, fmt.Errorf("invalid ADD ALIAS syntax: unexpected token '%s'", tokens[idx])
		}
		aliasName, _ := constituent["alias"].(string)
		constituentDbName, _ := constituent["database_name"].(string)

		// Add constituent using the interface
		err = e.dbManager.AddConstituent(dbName, constituent)
		if err != nil {
			return nil, fmt.Errorf("failed to add constituent to composite database '%s': %w", dbName, err)
		}

		return &ExecuteResult{
			Columns: []string{"composite_database", "action", "alias", "database", "type", "uri", "secret_ref", "auth_mode", "user"},
			Rows: [][]interface{}{{
				dbName,
				"ADD",
				aliasName,
				constituentDbName,
				constituent["type"],
				constituent["uri"],
				constituent["secret_ref"],
				constituent["auth_mode"],
				constituent["user"],
			}},
		}, nil

	} else if strings.HasPrefix(upperRemaining, "DROP ALIAS") {
		// DROP ALIAS alias
		dropIdx := findMultiWordKeywordIndex(remaining, "DROP", "ALIAS")
		if dropIdx == -1 {
			return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax: DROP ALIAS expected")
		}

		// Skip "DROP ALIAS" and whitespace
		aliasStart := dropIdx + len("DROP")
		for aliasStart < len(remaining) && isWhitespace(remaining[aliasStart]) {
			aliasStart++
		}
		if aliasStart+len("ALIAS") <= len(remaining) && strings.EqualFold(remaining[aliasStart:aliasStart+len("ALIAS")], "ALIAS") {
			aliasStart += len("ALIAS")
			for aliasStart < len(remaining) && isWhitespace(remaining[aliasStart]) {
				aliasStart++
			}
		}

		// Extract alias name (until newline or end)
		aliasNameEnd := aliasStart
		for aliasNameEnd < len(remaining) && !isWhitespace(remaining[aliasNameEnd]) && remaining[aliasNameEnd] != '\n' {
			aliasNameEnd++
		}

		aliasName := strings.TrimSpace(remaining[aliasStart:aliasNameEnd])
		aliasName = strings.ReplaceAll(aliasName, " ", "")
		aliasName = strings.ReplaceAll(aliasName, "\t", "")
		aliasName = strings.ReplaceAll(aliasName, "\n", "")
		aliasName = strings.ReplaceAll(aliasName, "\r", "")

		if aliasName == "" {
			return nil, fmt.Errorf("invalid DROP ALIAS syntax: alias name cannot be empty")
		}

		// Remove constituent
		err := e.dbManager.RemoveConstituent(dbName, aliasName)
		if err != nil {
			return nil, fmt.Errorf("failed to remove constituent from composite database '%s': %w", dbName, err)
		}

		return &ExecuteResult{
			Columns: []string{"composite_database", "action", "alias"},
			Rows:    [][]interface{}{{dbName, "DROP", aliasName}},
		}, nil

	} else {
		return nil, fmt.Errorf("invalid ALTER COMPOSITE DATABASE syntax: ADD ALIAS or DROP ALIAS expected")
	}
}
