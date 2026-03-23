package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoverBadgerClosedPanic_ReturnsErrStorageClosed(t *testing.T) {
	err := recoverBadgerClosedPanic(func() error {
		panic("DB Closed")
	})
	require.ErrorIs(t, err, ErrStorageClosed)
}

func TestRecoverBadgerClosedPanic_RepanicsUnexpectedPanic(t *testing.T) {
	assert.PanicsWithValue(t, "boom", func() {
		_ = recoverBadgerClosedPanic(func() error {
			panic("boom")
		})
	})
}
