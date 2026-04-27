package errors

import (
	stderrors "errors"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	// TransientDeadlockDetected is the retryable wire error code for lock deadlocks.
	TransientDeadlockDetected = "Neo.TransientError.Transaction.DeadlockDetected"
	// TransientOutdated is the retryable wire error code for stale MVCC snapshots.
	TransientOutdated = "Neo.TransientError.Transaction.Outdated"
)

var (
	// ErrTransactionConflict aliases the storage conflict sentinel used when an
	// optimistic transaction observes data changed after its snapshot.
	ErrTransactionConflict = storage.ErrConflict
	// ErrMVCCResourcePressure aliases the storage admission sentinel used when a
	// snapshot cannot be kept alive under current MVCC pressure.
	ErrMVCCResourcePressure = storage.ErrMVCCResourcePressure
	// ErrMVCCSnapshotGracefulCancel aliases the storage sentinel for snapshots
	// cancelled during high MVCC pressure.
	ErrMVCCSnapshotGracefulCancel = storage.ErrMVCCSnapshotGracefulCancel
	// ErrMVCCSnapshotHardExpired aliases the storage sentinel for snapshots
	// forcibly expired during critical MVCC pressure.
	ErrMVCCSnapshotHardExpired = storage.ErrMVCCSnapshotHardExpired
	// ErrTransactionDeadlock marks lock-ordering deadlocks that drivers should
	// retry as Neo4j-compatible transient transaction failures.
	ErrTransactionDeadlock = stderrors.New("transaction deadlock")
)

// MapTransientTransactionError maps known transaction failure sentinels to
// Neo4j-compatible transient transaction codes. It intentionally classifies by
// error reference rather than message text so localized or templated messages do
// not change retry semantics.
func MapTransientTransactionError(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	if stderrors.Is(err, ErrTransactionDeadlock) {
		return TransientDeadlockDetected, true
	}
	if stderrors.Is(err, ErrTransactionConflict) ||
		stderrors.Is(err, ErrMVCCResourcePressure) ||
		stderrors.Is(err, ErrMVCCSnapshotGracefulCancel) ||
		stderrors.Is(err, ErrMVCCSnapshotHardExpired) {
		return TransientOutdated, true
	}
	return "", false
}
