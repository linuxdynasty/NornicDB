package errors

import "testing"

// TestMapTransientTransactionError verifies the protocol-code boundary for
// retryable transaction failures and non-retryable ordinary errors.
func TestMapTransientTransactionError(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    string
		ok      bool
	}{
		{
			name:    "deadlock",
			message: "deadlock detected while waiting for transaction lock",
			want:    TransientDeadlockDetected,
			ok:      true,
		},
		{
			name:    "transaction conflict",
			message: "transaction conflict: node changed after transaction start",
			want:    TransientOutdated,
			ok:      true,
		},
		{
			name:    "resource pressure",
			message: "snapshot forcibly expired due to critical resource pressure",
			want:    TransientOutdated,
			ok:      true,
		},
		{
			name:    "ordinary error",
			message: "syntax error",
			ok:      false,
		},
		{
			name: "empty",
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := MapTransientTransactionError(tt.message)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v", ok, tt.ok)
			}
			if got != tt.want {
				t.Fatalf("code = %q, want %q", got, tt.want)
			}
		})
	}
}
