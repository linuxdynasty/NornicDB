package encryption

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/kms"
	"github.com/stretchr/testify/require"
)

type testReencryptor struct {
	updated int
}

func (t *testReencryptor) ReencryptWithDataKey(context.Context, *kms.DataKey) (int, error) {
	return t.updated, nil
}

func TestRotationManager_PerformRotation(t *testing.T) {
	t.Parallel()
	provider, err := kms.NewLocalProvider(kms.LocalConfig{
		MasterKey: []byte("0123456789abcdef0123456789abcdef"),
		KeyURI:    "kms://local/rotate",
	})
	require.NoError(t, err)
	defer kms.CloseProvider(provider)

	rm := NewRotationManager(provider, &testReencryptor{updated: 12}, RotationConfig{
		Enabled:  true,
		Interval: time.Minute,
	})
	n, err := rm.performRotation(context.Background())
	require.NoError(t, err)
	require.Equal(t, 12, n)
}
