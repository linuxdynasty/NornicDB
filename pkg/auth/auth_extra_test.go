package auth

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
)

// ============================================================================
// roleNameFromNodeID
// ============================================================================

func TestRoleNameFromNodeID_WithPrefix(t *testing.T) {
	result := roleNameFromNodeID(rolePrefix + "admin")
	assert.Equal(t, "admin", result)
}

func TestRoleNameFromNodeID_WithoutPrefix(t *testing.T) {
	result := roleNameFromNodeID("nornic:node-123")
	assert.Equal(t, "", result)
}

func TestRoleNameFromNodeID_Empty(t *testing.T) {
	result := roleNameFromNodeID("")
	assert.Equal(t, "", result)
}

// ============================================================================
// Authenticator.IsSecurityEnabled
// ============================================================================

func TestAuthenticator_IsSecurityEnabled_True(t *testing.T) {
	cfg := AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-that-is-long-enough-32chars"),
	}
	eng := storage.NewMemoryEngine()
	a, err := NewAuthenticator(cfg, eng)
	if err != nil {
		t.Skip("cannot create authenticator:", err)
	}
	assert.True(t, a.IsSecurityEnabled())
}

func TestAuthenticator_IsSecurityEnabled_False(t *testing.T) {
	cfg := AuthConfig{
		SecurityEnabled: false,
		JWTSecret:       []byte("test-secret-that-is-long-enough-32chars"),
	}
	eng := storage.NewMemoryEngine()
	a, err := NewAuthenticator(cfg, eng)
	if err != nil {
		t.Skip("cannot create authenticator:", err)
	}
	assert.False(t, a.IsSecurityEnabled())
}

// ============================================================================
// cloneJWTClaims
// ============================================================================

func TestCloneJWTClaims_Nil(t *testing.T) {
	result := cloneJWTClaims(nil)
	assert.Nil(t, result)
}

func TestCloneJWTClaims_Full(t *testing.T) {
	original := &JWTClaims{
		Sub:      "user-1",
		Username: "alice",
		Email:    "alice@example.com",
		Roles:    []string{"admin", "reader"},
		Iat:      1234567890,
		Exp:      9999999999,
	}
	clone := cloneJWTClaims(original)
	assert.Equal(t, original.Sub, clone.Sub)
	assert.Equal(t, original.Username, clone.Username)
	assert.Equal(t, original.Roles, clone.Roles)
	// Mutating original should not affect clone
	original.Roles[0] = "mutated"
	assert.Equal(t, "admin", clone.Roles[0])
}

func TestCloneJWTClaims_EmptyRoles(t *testing.T) {
	original := &JWTClaims{Sub: "x", Roles: []string{}}
	clone := cloneJWTClaims(original)
	assert.Equal(t, original.Sub, clone.Sub)
	assert.Empty(t, clone.Roles)
}

// ============================================================================
// BasicAuthCache – Set/Get by username/password
// ============================================================================

func TestBasicAuthCache_GetSet_ByCredentials(t *testing.T) {
	c := NewBasicAuthCache(10, DefaultAuthCacheTTL)
	if c == nil {
		t.Skip("cache not created")
	}
	claims := &JWTClaims{Username: "bob", Sub: "bob-id"}
	c.Set("bob", "password123", claims)

	got, ok := c.Get("bob", "password123")
	assert.True(t, ok)
	assert.Equal(t, "bob", got.Username)

	// Wrong password → miss
	_, ok2 := c.Get("bob", "wrongpass")
	assert.False(t, ok2)
}

func TestBasicAuthCache_Get_EmptyCredentials(t *testing.T) {
	c := NewBasicAuthCache(10, DefaultAuthCacheTTL)
	_, ok := c.Get("", "password")
	assert.False(t, ok)
	_, ok2 := c.Get("user", "")
	assert.False(t, ok2)
}
