package auth

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBasicAuthCache_GetFromHeader_NilCache(t *testing.T) {
	var c *BasicAuthCache
	v, ok := c.GetFromHeader("Basic dXNlcjpwYXNz")
	assert.Nil(t, v)
	assert.False(t, ok)
}

func TestBasicAuthCache_GetFromHeader_EmptyHeader(t *testing.T) {
	c := NewBasicAuthCache(10, time.Minute)
	v, ok := c.GetFromHeader("")
	assert.Nil(t, v)
	assert.False(t, ok)
}

func TestBasicAuthCache_SetFromHeader_RoundTrip(t *testing.T) {
	c := NewBasicAuthCache(10, time.Minute)

	claims := &JWTClaims{Username: "alice", Sub: "alice-id"}
	c.SetFromHeader("Basic YWxpY2U6cGFzcw==", claims)

	got, ok := c.GetFromHeader("Basic YWxpY2U6cGFzcw==")
	assert.True(t, ok)
	assert.Equal(t, "alice", got.Username)
}

func TestBasicAuthCache_SetFromHeader_NilClaims(t *testing.T) {
	c := NewBasicAuthCache(10, time.Minute)
	// Should not panic
	c.SetFromHeader("Basic header", nil)
	_, ok := c.GetFromHeader("Basic header")
	assert.False(t, ok)
}

func TestBasicAuthCache_SetFromHeader_NilCache(t *testing.T) {
	var c *BasicAuthCache
	// Should not panic
	c.SetFromHeader("Basic header", &JWTClaims{Username: "bob"})
}

func TestBasicAuthCache_GetFromHeader_Miss(t *testing.T) {
	c := NewBasicAuthCache(10, time.Minute)
	v, ok := c.GetFromHeader("Basic bm90Y2FjaGVk")
	assert.Nil(t, v)
	assert.False(t, ok)
}

func TestBasicAuthCache_GetFromHeader_Expired(t *testing.T) {
	c := NewBasicAuthCache(10, 1*time.Nanosecond)
	claims := &JWTClaims{Username: "eve"}
	c.SetFromHeader("Basic ZXZlOnBhc3M=", claims)

	// Let TTL expire
	time.Sleep(2 * time.Millisecond)

	v, ok := c.GetFromHeader("Basic ZXZlOnBhc3M=")
	assert.Nil(t, v)
	assert.False(t, ok)
}

func TestNewBasicAuthCache_InvalidParams(t *testing.T) {
	assert.Nil(t, NewBasicAuthCache(0, time.Minute))
	assert.Nil(t, NewBasicAuthCache(10, 0))
	assert.Nil(t, NewBasicAuthCache(-1, time.Minute))
}
