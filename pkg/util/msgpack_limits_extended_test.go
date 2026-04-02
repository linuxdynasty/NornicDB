package util

import (
	"os"
	"sync"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestMaxMsgpackDecodeBytes_InvalidEnv(t *testing.T) {
	// Non-numeric env value should fall back to default
	t.Setenv(msgpackMaxBytesEnvKey, "not-a-number")
	maxMsgpackDecodeBytesOnce = sync.Once{}
	maxMsgpackDecodeBytes = 0

	got := MaxMsgpackDecodeBytes()
	if got != defaultMaxMsgpackDecodeBytes {
		t.Errorf("Expected default %d for invalid env, got %d", defaultMaxMsgpackDecodeBytes, got)
	}
}

func TestMaxMsgpackDecodeBytes_ZeroEnv(t *testing.T) {
	// Zero value should fall back to default (v <= 0)
	t.Setenv(msgpackMaxBytesEnvKey, "0")
	maxMsgpackDecodeBytesOnce = sync.Once{}
	maxMsgpackDecodeBytes = 0

	got := MaxMsgpackDecodeBytes()
	if got != defaultMaxMsgpackDecodeBytes {
		t.Errorf("Expected default %d for zero env, got %d", defaultMaxMsgpackDecodeBytes, got)
	}
}

func TestMaxMsgpackDecodeBytes_NegativeEnv(t *testing.T) {
	t.Setenv(msgpackMaxBytesEnvKey, "-100")
	maxMsgpackDecodeBytesOnce = sync.Once{}
	maxMsgpackDecodeBytes = 0

	got := MaxMsgpackDecodeBytes()
	if got != defaultMaxMsgpackDecodeBytes {
		t.Errorf("Expected default %d for negative env, got %d", defaultMaxMsgpackDecodeBytes, got)
	}
}

func TestValidateMsgpackPayloadSize_NegativeSize(t *testing.T) {
	maxMsgpackDecodeBytesOnce = sync.Once{}
	maxMsgpackDecodeBytes = 0
	// Clear env so we get the default
	t.Setenv(msgpackMaxBytesEnvKey, "")

	err := ValidateMsgpackPayloadSize(-1)
	if err == nil {
		t.Fatal("Expected error for negative size")
	}
}

func TestDecodeMsgpackFile_SuccessfulDecode(t *testing.T) {
	t.Setenv(msgpackMaxBytesEnvKey, "1048576")
	maxMsgpackDecodeBytesOnce = sync.Once{}
	maxMsgpackDecodeBytes = 0

	tmp, err := os.CreateTemp(t.TempDir(), "msgpack-*.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer tmp.Close()

	payload, err := msgpack.Marshal(map[string]string{"hello": "world"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.Write(payload); err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.Seek(0, 0); err != nil {
		t.Fatal(err)
	}

	var out map[string]string
	if err := DecodeMsgpackFile(tmp, &out); err != nil {
		t.Fatalf("Expected successful decode, got %v", err)
	}
	if out["hello"] != "world" {
		t.Errorf("Expected world, got %q", out["hello"])
	}
}
