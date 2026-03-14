package cypher

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

var fabricCallUsePattern = regexp.MustCompile(`(?is)\bCALL\s*\{\s*USE\s+`)
var fabricUsePattern = regexp.MustCompile(`(?is)\bUSE\s+`)
var fabricDynamicGraphPattern = regexp.MustCompile(`(?is)\bgraph\.(byName|byElementId)\s*\(`)

func (e *StorageExecutor) shouldUseFabricPlanner(cypher string) bool {
	if e.dbManager == nil {
		return false
	}
	return fabricCallUsePattern.MatchString(cypher) ||
		fabricUsePattern.MatchString(cypher) ||
		fabricDynamicGraphPattern.MatchString(cypher)
}

func (e *StorageExecutor) executeViaFabric(ctx context.Context, cypher string, params map[string]interface{}) (*ExecuteResult, error) {
	tx := fabric.NewFabricTransaction(fmt.Sprintf("fab-%d", time.Now().UnixNano()))
	return e.executeViaFabricWithTx(ctx, cypher, params, tx, true)
}

func (e *StorageExecutor) executeViaFabricWithTx(ctx context.Context, cypher string, params map[string]interface{}, tx *fabric.FabricTransaction, autoCommit bool) (*ExecuteResult, error) {
	catalog, err := e.buildFabricCatalog()
	if err != nil {
		return nil, err
	}

	authToken := GetAuthTokenFromContext(ctx)
	localExec := fabric.NewLocalFragmentExecutor(&cypherFabricExecutor{base: e, authToken: authToken}, func(dbName string) (storage.Engine, error) {
		if e.dbManager != nil {
			engineIface, err := e.dbManager.GetStorageForUse(dbName, authToken)
			if err == nil {
				if engine, ok := engineIface.(storage.Engine); ok {
					return engine, nil
				}
				return nil, fmt.Errorf("storage engine has unexpected type for '%s'", dbName)
			}
		}
		scoped, _, err := e.scopedExecutorForUse(dbName, authToken)
		if err != nil {
			return nil, err
		}
		return scoped.storage, nil
	})
	remoteExec := fabric.NewRemoteFragmentExecutor()
	defer func() { _ = remoteExec.Close() }()

	planner := fabric.NewFabricPlanner(catalog)
	sessionDB := e.currentDatabaseName()
	if dbFromCtx := GetUseDatabaseFromContext(ctx); strings.TrimSpace(dbFromCtx) != "" {
		sessionDB = dbFromCtx
	}
	gateway := fabric.NewQueryGateway(planner, fabric.NewFabricExecutor(catalog, localExec, remoteExec))
	stream, err := gateway.Execute(ctx, tx, cypher, sessionDB, params, authToken)
	if err != nil {
		// In explicit transactions (autoCommit=false), preserve transaction lifecycle
		// for client-issued COMMIT/ROLLBACK. In autocommit mode, rollback immediately.
		if autoCommit {
			_ = tx.Rollback(func(_ *fabric.SubTransaction) error { return nil })
		}
		return nil, err
	}
	if autoCommit {
		if err := tx.Commit(func(_ *fabric.SubTransaction) error { return nil }, func(_ *fabric.SubTransaction) error { return nil }); err != nil {
			return nil, err
		}
	}
	if stream == nil {
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}
	return &ExecuteResult{Columns: stream.Columns, Rows: stream.Rows}, nil
}

func (e *StorageExecutor) currentDatabaseName() string {
	if ns, ok := e.storage.(interface{ Namespace() string }); ok {
		if name := strings.TrimSpace(ns.Namespace()); name != "" {
			return name
		}
	}
	return "nornic"
}

func (e *StorageExecutor) buildFabricCatalog() (*fabric.Catalog, error) {
	catalog := fabric.NewCatalog()
	for _, db := range e.dbManager.ListDatabases() {
		dbName := strings.TrimSpace(db.Name())
		if dbName == "" {
			continue
		}
		catalog.Register(dbName, &fabric.LocationLocal{DBName: dbName})
		for alias := range e.dbManager.ListAliases(dbName) {
			alias = strings.TrimSpace(alias)
			if alias != "" {
				catalog.Register(alias, &fabric.LocationLocal{DBName: dbName})
			}
		}

		if db.Type() != "composite" {
			continue
		}
		constituents, err := e.dbManager.GetCompositeConstituents(dbName)
		if err != nil {
			return nil, fmt.Errorf("failed to get constituents for '%s': %w", dbName, err)
		}
		for _, raw := range constituents {
			ref, ok := toConstituentRef(raw)
			if !ok || strings.TrimSpace(ref.Alias) == "" {
				continue
			}
			qualified := dbName + "." + ref.Alias
			if strings.EqualFold(strings.TrimSpace(ref.Type), "remote") {
				catalog.Register(qualified, &fabric.LocationRemote{
					DBName:   ref.DatabaseName,
					URI:      ref.URI,
					AuthMode: strings.TrimSpace(ref.AuthMode),
					User:     ref.User,
					Password: ref.Password,
				})
				continue
			}
			catalog.Register(qualified, &fabric.LocationLocal{DBName: ref.DatabaseName})
		}
	}
	return catalog, nil
}

func toConstituentRef(raw interface{}) (multidb.ConstituentRef, bool) {
	if ref, ok := raw.(multidb.ConstituentRef); ok {
		return ref, true
	}
	m, ok := raw.(map[string]interface{})
	if !ok {
		return multidb.ConstituentRef{}, false
	}
	return multidb.ConstituentRef{
		Alias:        mapString(m, "alias"),
		DatabaseName: mapString(m, "database_name"),
		Type:         mapString(m, "type"),
		AccessMode:   mapString(m, "access_mode"),
		URI:          mapString(m, "uri"),
		SecretRef:    mapString(m, "secret_ref"),
		AuthMode:     mapString(m, "auth_mode"),
		User:         mapString(m, "user"),
		Password:     mapString(m, "password"),
	}, true
}

func mapString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

type cypherFabricExecutor struct {
	base      *StorageExecutor
	authToken string
}

func (c *cypherFabricExecutor) ExecuteQuery(ctx context.Context, engine storage.Engine, query string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	exec := c.base.cloneForStorage(engine)
	ctx = WithAuthToken(ctx, c.authToken)
	result, err := exec.executeInternal(ctx, query, params)
	if err != nil {
		return nil, nil, err
	}
	if result == nil {
		return []string{}, [][]interface{}{}, nil
	}
	return result.Columns, result.Rows, nil
}
