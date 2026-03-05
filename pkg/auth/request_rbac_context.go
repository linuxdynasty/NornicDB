// Package auth provides request-scoped RBAC context helpers.
// When the server mounts authenticated endpoints (Bifrost, GraphQL), it can
// attach principal roles, DatabaseAccessMode, and ResolvedAccess resolver to
// the request context so handlers and resolvers can enforce per-database access.
package auth

import "context"

type requestRBACKey string

var (
	requestRBACKeyPrincipalRoles         = requestRBACKey("principal_roles")
	requestRBACKeyDatabaseAccessMode     = requestRBACKey("database_access_mode")
	requestRBACKeyResolvedAccessResolver = requestRBACKey("resolved_access_resolver")
)

// WithRequestPrincipalRoles attaches the principal's role names to the context.
func WithRequestPrincipalRoles(ctx context.Context, roles []string) context.Context {
	return context.WithValue(ctx, requestRBACKeyPrincipalRoles, roles)
}

// WithRequestDatabaseAccessMode attaches the principal's per-database access mode to the context.
func WithRequestDatabaseAccessMode(ctx context.Context, mode DatabaseAccessMode) context.Context {
	return context.WithValue(ctx, requestRBACKeyDatabaseAccessMode, mode)
}

// WithRequestResolvedAccessResolver attaches a resolver (dbName -> ResolvedAccess) to the context.
func WithRequestResolvedAccessResolver(ctx context.Context, fn func(string) ResolvedAccess) context.Context {
	return context.WithValue(ctx, requestRBACKeyResolvedAccessResolver, fn)
}

// RequestPrincipalRolesFromContext returns the principal's roles from the request context, or nil.
func RequestPrincipalRolesFromContext(ctx context.Context) []string {
	v, _ := ctx.Value(requestRBACKeyPrincipalRoles).([]string)
	return v
}

// RequestDatabaseAccessModeFromContext returns the principal's DatabaseAccessMode from context, or nil.
func RequestDatabaseAccessModeFromContext(ctx context.Context) DatabaseAccessMode {
	v, _ := ctx.Value(requestRBACKeyDatabaseAccessMode).(DatabaseAccessMode)
	return v
}

// RequestResolvedAccessResolverFromContext returns the ResolvedAccess resolver from context, or nil.
func RequestResolvedAccessResolverFromContext(ctx context.Context) func(string) ResolvedAccess {
	v, _ := ctx.Value(requestRBACKeyResolvedAccessResolver).(func(string) ResolvedAccess)
	return v
}
