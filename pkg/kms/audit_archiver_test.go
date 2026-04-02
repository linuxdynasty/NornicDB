package kms

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAuditArchiver_Archive(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	arch, err := NewAuditArchiver(AuditArchiverConfig{
		LocalPath:  path,
		SignEvents: true,
		SignKey:    []byte("audit-archiver-signing-key"),
	})
	require.NoError(t, err)

	err = arch.Archive(context.Background(), AuditEvent{
		EventType: "KEY_GENERATED",
		KeyURI:    "kms://local/test",
		Status:    "SUCCESS",
		Timestamp: time.Now().UTC(),
	})
	require.NoError(t, err)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(raw), "\"event_type\":\"KEY_GENERATED\"")
	require.Contains(t, string(raw), "\"signature\":\"")
	require.True(t, strings.HasSuffix(string(raw), "\n"))
}
