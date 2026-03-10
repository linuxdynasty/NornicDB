package search

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

	// Reuse branch: when capacities are sufficient, slice should be reset, not reallocated.
	scratch2 := vfs.getScoreScratch(1, 64)
	require.NotNil(t, scratch2)
	require.GreaterOrEqual(t, cap(scratch2.offsets), 1)
	require.GreaterOrEqual(t, cap(scratch2.batch), 64)
	vfs.putScoreScratch(scratch2)

	// Closed store returns nil score list without error.
	require.NoError(t, vfs.Close())
	scored, err = vfs.scoreCandidatesDot(context.Background(), []float32{1, 0, 0}, []Candidate{{ID: "a"}})
	require.NoError(t, err)
	require.Nil(t, scored)
}

func TestVectorFileStore_IterateChunked_Branches(t *testing.T) {
	base := filepath.Join(t.TempDir(), "vectors")
	vfs, err := NewVectorFileStore(base, 2)
	require.NoError(t, err)
	defer func() { _ = vfs.Close() }()

	// Large ID ensures IterateChunked grows its internal record buffer.
	longID := strings.Repeat("x", 400)
	require.NoError(t, vfs.Add(longID, []float32{1, 0}))
	require.NoError(t, vfs.Add("b", []float32{0, 1}))

	var chunks int
	err = vfs.IterateChunked(0, func(ids []string, vecs [][]float32) error { // 0 -> default chunk size branch
		chunks++
		require.Len(t, ids, len(vecs))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, chunks)

	// Callback error should be propagated.
	err = vfs.IterateChunked(1, func(ids []string, vecs [][]float32) error {
		return fmt.Errorf("stop-iteration")
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "stop-iteration")

	// Nil file branch.
	vfs.mu.Lock()
	saved := vfs.file
	vfs.file = nil
	vfs.mu.Unlock()
	err = vfs.IterateChunked(1, func(ids []string, vecs [][]float32) error { return nil })
	require.ErrorIs(t, err, errVecFileClosed)
	vfs.mu.Lock()
	vfs.file = saved
	vfs.mu.Unlock()

	// Closed branch.
	require.NoError(t, vfs.Close())
	err = vfs.IterateChunked(1, func(ids []string, vecs [][]float32) error { return nil })
	require.ErrorIs(t, err, errVecFileClosed)
}

func TestVectorFileStore_NewVectorFileStore_InvalidExistingHeader(t *testing.T) {
	t.Run("invalid magic", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "vectors")
		vecPath := base + ".vec"
		h := make([]byte, vecHeaderSize)
		copy(h[:4], []byte("BAD!"))
		require.NoError(t, os.WriteFile(vecPath, h, 0o644))

		_, err := NewVectorFileStore(base, 2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid vector file magic")
	})

	t.Run("unsupported version", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "vectors")
		vecPath := base + ".vec"
		h := make([]byte, vecHeaderSize)
		copy(h[:4], []byte(vecFileMagic))
		h[4] = 99
		binary.LittleEndian.PutUint32(h[5:9], uint32(2))
		require.NoError(t, os.WriteFile(vecPath, h, 0o644))

		_, err := NewVectorFileStore(base, 2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported vector file version")
	})

	t.Run("dimension mismatch", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "vectors")
		vecPath := base + ".vec"
		h := make([]byte, vecHeaderSize)
		copy(h[:4], []byte(vecFileMagic))
		h[4] = vecFileVersion
		binary.LittleEndian.PutUint32(h[5:9], uint32(3))
		require.NoError(t, os.WriteFile(vecPath, h, 0o644))

		_, err := NewVectorFileStore(base, 2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "dimensions")
	})
}

func TestVectorFileStore_CompactIfNeeded_DecisionBranches(t *testing.T) {
	t.Run("live_zero_and_no_obsolete_noop", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "vectors")
		vfs, err := NewVectorFileStore(base, 2)
		require.NoError(t, err)
		defer func() { _ = vfs.Close() }()

		compacted, err := vfs.CompactIfNeeded()
		require.NoError(t, err)
		require.False(t, compacted)
	})

	t.Run("live_zero_with_obsolete_rewrites_to_header_only", func(t *testing.T) {
		t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_OBSOLETE", "1")
		t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_SIZE_MB", "0")
		t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_DEAD_RATIO", "0")

		base := filepath.Join(t.TempDir(), "vectors")
		vfs, err := NewVectorFileStore(base, 2)
		require.NoError(t, err)
		defer func() { _ = vfs.Close() }()

		require.NoError(t, vfs.Add("id-1", []float32{1, 0}))
		require.True(t, vfs.Remove("id-1")) // obsoleteCount > 0, live == 0
		compacted, err := vfs.CompactIfNeeded()
		require.NoError(t, err)
		require.True(t, compacted)

		info, err := os.Stat(base + ".vec")
		require.NoError(t, err)
		require.Equal(t, int64(vecHeaderSize), info.Size())
		require.Equal(t, 0, vfs.Count())
	})

	t.Run("obsolete_below_threshold_skips", func(t *testing.T) {
		t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_OBSOLETE", "50")
		t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_SIZE_MB", "0")
		t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_DEAD_RATIO", "0")

		base := filepath.Join(t.TempDir(), "vectors")
		vfs, err := NewVectorFileStore(base, 2)
		require.NoError(t, err)
		defer func() { _ = vfs.Close() }()

		require.NoError(t, vfs.Add("id-1", []float32{1, 0}))
		require.NoError(t, vfs.Add("id-1", []float32{0, 1})) // obsoleteCount = 1
		compacted, err := vfs.CompactIfNeeded()
		require.NoError(t, err)
		require.False(t, compacted)
		require.Equal(t, 1, vfs.Count())
	})

	t.Run("dead_ratio_below_threshold_skips", func(t *testing.T) {
		t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_OBSOLETE", "1")
		t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_MIN_SIZE_MB", "0")
		t.Setenv("NORNICDB_VECTOR_VFS_COMPACT_DEAD_RATIO", "0.95")

		base := filepath.Join(t.TempDir(), "vectors")
		vfs, err := NewVectorFileStore(base, 2)
		require.NoError(t, err)
		defer func() { _ = vfs.Close() }()

		require.NoError(t, vfs.Add("id-1", []float32{1, 0}))
		require.NoError(t, vfs.Add("id-1", []float32{0, 1})) // one obsolete, one live => ratio 0.5
		compacted, err := vfs.CompactIfNeeded()
		require.NoError(t, err)
		require.False(t, compacted)
	})
}
