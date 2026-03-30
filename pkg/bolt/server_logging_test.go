package bolt

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w

	defer func() {
		os.Stdout = oldStdout
		_ = r.Close()
	}()

	fn()
	_ = w.Close()

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	return string(out)
}

func TestLogRunTiming_SuccessIsSilentByDefault(t *testing.T) {
	session := &Session{
		server: &Server{config: DefaultConfig()},
	}

	output := captureStdout(t, func() {
		session.logRunTiming("OK", "nornic", "RETURN 1", 250*time.Microsecond, 1, nil)
	})

	if output != "" {
		t.Fatalf("expected no stdout output for successful query timing with LogQueries disabled, got %q", output)
	}
}

func TestLogRunTiming_ErrorStillLogs(t *testing.T) {
	session := &Session{
		server: &Server{config: DefaultConfig()},
	}

	output := captureStdout(t, func() {
		session.logRunTiming("ERROR", "nornic", "RETURN 1", time.Millisecond, 0, io.EOF)
	})

	if !strings.Contains(output, "[BOLT] RUN") {
		t.Fatalf("expected error timing log to be emitted, got %q", output)
	}
	if !strings.Contains(output, "err=EOF") {
		t.Fatalf("expected error timing log to include the error, got %q", output)
	}
}
