package kms

import (
	"context"
	"testing"
	"time"

	wrapping "github.com/hashicorp/go-kms-wrapping/v2"
	"github.com/stretchr/testify/require"
)

func TestWrappingAdapter_RoundTripAndRotate(t *testing.T) {
	t.Parallel()
	tw := wrapping.NewTestEnvelopeWrapper([]byte("0123456789abcdef0123456789abcdef"))
	tw.SetKeyId("kms://test/wrapper")
	a := newWrappingAdapter(tw)

	dek, err := a.GenerateDataKey(context.Background(), KeyGenOpts{
		TTL:       time.Minute,
		Algorithm: "AES-256-GCM",
	})
	require.NoError(t, err)
	require.NotEmpty(t, dek.Ciphertext)
	require.Len(t, dek.Plaintext, 32)

	plain, err := a.DecryptDataKey(context.Background(), dek.Ciphertext, DecryptOpts{})
	require.NoError(t, err)
	require.Equal(t, dek.Plaintext, plain)

	rot, err := a.RotateDataKey(context.Background(), dek.Ciphertext, RotateOpts{TTL: time.Minute})
	require.NoError(t, err)
	require.NotEmpty(t, rot.Ciphertext)
	rotPlain, err := a.DecryptDataKey(context.Background(), rot.Ciphertext, DecryptOpts{})
	require.NoError(t, err)
	require.Equal(t, dek.Plaintext, rotPlain)
}
