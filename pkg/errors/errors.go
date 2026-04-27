package errors

import "strings"

const (
	// TransientDeadlockDetected is the retryable wire error code for lock deadlocks.
	TransientDeadlockDetected = "Neo.TransientError.Transaction.DeadlockDetected"
	// TransientOutdated is the retryable wire error code for stale MVCC snapshots.
	TransientOutdated = "Neo.TransientError.Transaction.Outdated"
)

// MapTransientTransactionError maps known MVCC conflict and deadlock messages
// to Neo4j-compatible transient transaction codes. It returns false for
// ordinary errors so callers can preserve their protocol-specific fallback.
func MapTransientTransactionError(message string) (string, bool) {
	m := strings.ToLower(strings.TrimSpace(message))
	if m == "" {
		return "", false
	}
	if strings.Contains(m, "deadlock") {
		return TransientDeadlockDetected, true
	}
	if strings.Contains(m, "changed after transaction start") ||
		strings.Contains(m, "transaction conflict") ||
		strings.Contains(m, "write conflict") ||
		strings.Contains(m, "mvcc: resource pressure") ||
		strings.Contains(m, "snapshot cancelled due to resource pressure") ||
		strings.Contains(m, "snapshot forcibly expired due to critical resource pressure") ||
		strings.Contains(m, "snapshot expired under resource pressure") ||
		strings.Contains(m, "conflict:") {
		return TransientOutdated, true
	}
	return "", false
}
