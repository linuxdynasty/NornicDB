package server

import "testing"

func TestMapTransientTransactionError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		message string
		want    string
		ok      bool
	}{
		{
			name:    "conflict changed after start",
			message: "failed to commit implicit transaction: conflict: node x changed after transaction start",
			want:    "Neo.TransientError.Transaction.Outdated",
			ok:      true,
		},
		{
			name:    "deadlock",
			message: "deadlock detected while waiting for lock",
			want:    "Neo.TransientError.Transaction.DeadlockDetected",
			ok:      true,
		},
		{
			name:    "graceful snapshot expiration",
			message: "failed to create node: mvcc: snapshot cancelled due to resource pressure",
			want:    "Neo.TransientError.Transaction.Outdated",
			ok:      true,
		},
		{
			name:    "hard snapshot expiration",
			message: "mvcc: snapshot forcibly expired due to critical resource pressure",
			want:    "Neo.TransientError.Transaction.Outdated",
			ok:      true,
		},
		{
			name:    "syntax error passthrough",
			message: "invalid input 'RETURNN'",
			want:    "",
			ok:      false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := mapTransientTransactionError(tc.message)
			if ok != tc.ok {
				t.Fatalf("ok mismatch: got %v want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Fatalf("code mismatch: got %q want %q", got, tc.want)
			}
		})
	}
}
