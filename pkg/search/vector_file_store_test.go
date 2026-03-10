package search

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVectorFileStore_CompactionReclaimsObsoleteRecords(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_OBSOLETE", "1")
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_SIZE_MB", "0")
	t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_DEAD_RATIO", "0")

	base := filepath.Join(t.TempDir(), "vectors")
	vfs, err := NewVectorFileStore(base, 4)
	require.NoError(t, err)
	defer func() { _ = vfs.Close() }()

	// Seed initial vectors.
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("id-%d", i)
		require.NoError(t, vfs.Add(id, []float32{1, 0, 0, float32(i)}))
	}
	// Update all vectors (creates stale append-only records).
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("id-%d", i)
		require.NoError(t, vfs.Add(id, []float32{0, 1, 0, float32(i)}))
	}
	// Delete half of ids (live set should become 50).
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("id-%d", i)
		require.True(t, vfs.Remove(id))
	}

	require.NoError(t, vfs.Sync())
	require.NoError(t, vfs.Save())

	before, err := os.Stat(base + ".vec")
	require.NoError(t, err)
	require.Greater(t, before.Size(), int64(0))

	compacted, err := vfs.CompactIfNeeded()
	require.NoError(t, err)
	require.True(t, compacted, "expected compaction when obsolete records exist")
	require.NoError(t, vfs.Sync())
	require.NoError(t, vfs.Save())

	after, err := os.Stat(base + ".vec")
	require.NoError(t, err)
	require.Less(t, after.Size(), before.Size(), "compaction should shrink vec file")
	require.Equal(t, 50, vfs.Count(), "only live vectors should remain")

	// Verify persisted state reloads with only live IDs.
	require.NoError(t, vfs.Close())
	vfs2, err := NewVectorFileStore(base, 4)
	require.NoError(t, err)
	defer func() { _ = vfs2.Close() }()
	require.NoError(t, vfs2.Load())
	require.Equal(t, 50, vfs2.Count())
	for i := 0; i < 50; i++ {
		_, ok := vfs2.GetVector(fmt.Sprintf("id-%d", i))
		require.False(t, ok, "removed id should not be live after reload")
	}
	for i := 50; i < 100; i++ {
		_, ok := vfs2.GetVector(fmt.Sprintf("id-%d", i))
		require.True(t, ok, "live id should exist after reload")
	}
}

func TestVectorFileStore_SyncDoesNotBlockAdd(t *testing.T) {
	base := filepath.Join(t.TempDir(), "vectors")
	vfs, err := NewVectorFileStore(base, 4)
	require.NoError(t, err)
	defer func() { _ = vfs.Close() }()

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	vfs.syncFile = func(*os.File) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	}

	syncDone := make(chan error, 1)
	go func() {
		syncDone <- vfs.Sync()
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("sync hook did not start")
	}

	addDone := make(chan error, 1)
	go func() {
		addDone <- vfs.Add("id-1", []float32{1, 0, 0, 0})
	}()

	// Add should complete even while Sync is blocked.
	select {
	case err := <-addDone:
		require.NoError(t, err)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Add blocked behind Sync (lock contention regression)")
	}

	close(release)
	require.NoError(t, <-syncDone)
	require.Equal(t, 1, vfs.Count())
}

func TestVectorFileStore_AddDoesNotBlockGetVector(t *testing.T) {
	base := filepath.Join(t.TempDir(), "vectors")
	vfs, err := NewVectorFileStore(base, 4)
	require.NoError(t, err)
	defer func() { _ = vfs.Close() }()

	require.NoError(t, vfs.Add("seed", []float32{1, 0, 0, 0}))

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	vfs.writeRecord = func(f *os.File, id string, vec []float32) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return writeVectorRecord(f, id, vec)
	}

	addDone := make(chan error, 1)
	go func() {
		addDone <- vfs.Add("id-2", []float32{0, 1, 0, 0})
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("add write hook did not start")
	}

	// GetVector on an existing entry should not block while another Add is in progress.
	getDone := make(chan struct{}, 1)
	go func() {
		_, ok := vfs.GetVector("seed")
		require.True(t, ok)
		getDone <- struct{}{}
	}()
	select {
	case <-getDone:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("GetVector blocked behind Add (lock contention regression)")
	}

	close(release)
	require.NoError(t, <-addDone)
}

func TestVectorFileStore_HasAndBuildIndexedCount(t *testing.T) {
	base := filepath.Join(t.TempDir(), "vectors")
	vfs, err := NewVectorFileStore(base, 3)
	require.NoError(t, err)
	defer func() { _ = vfs.Close() }()

	require.NoError(t, vfs.Add("id-1", []float32{1, 0, 0}))
	require.True(t, vfs.Has("id-1"))
	require.False(t, vfs.Has("missing"))

	vfs.SetBuildIndexedCount(42)
	require.Equal(t, int64(42), vfs.GetBuildIndexedCount())
}

func TestVectorFileStore_ScoreCandidatesDotAndScratchHelpers(t *testing.T) {
	base := filepath.Join(t.TempDir(), "vectors")
	vfs, err := NewVectorFileStore(base, 3)
	require.NoError(t, err)
	defer func() { _ = vfs.Close() }()

	require.NoError(t, vfs.Add("a", []float32{1, 0, 0}))
	require.NoError(t, vfs.Add("b", []float32{0, 1, 0}))

	scored, err := vfs.scoreCandidatesDot(context.Background(), []float32{1, 0, 0}, []Candidate{
		{ID: "a", Score: 0.1},
		{ID: "b", Score: 0.1},
		{ID: "missing", Score: 0.1},
	})
	require.NoError(t, err)
	require.NotEmpty(t, scored)
	require.Equal(t, "a", scored[0].ID)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = vfs.scoreCandidatesDot(cancelled, []float32{1, 0, 0}, []Candidate{{ID: "a"}})
	require.ErrorIs(t, err, context.Canceled)

	scratch := vfs.getScoreScratch(2, 128)
	require.NotNil(t, scratch)
	require.GreaterOrEqual(t, cap(scratch.batch), 128)
	vfs.putScoreScratch(scratch)
	vfs.putScoreScratch(nil)

	// Closed store returns nil score list without error.
	require.NoError(t, vfs.Close())
	scored, err = vfs.scoreCandidatesDot(context.Background(), []float32{1, 0, 0}, []Candidate{{ID: "a"}})
	require.NoError(t, err)
	require.Nil(t, scored)
}
