package util

import (
	"os"
	"sync"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func resetMsgpackLimitCacheForTest() {
	maxMsgpackDecodeBytesOnce = sync.Once{}
	maxMsgpackDecodeBytes = 0
}

func TestValidateMsgpackPayloadSize_RejectsOversized(t *testing.T) {
	t.Setenv(msgpackMaxBytesEnvKey, "64")
	resetMsgpackLimitCacheForTest()

	if err := ValidateMsgpackPayloadSize(65); err == nil {
		t.Fatalf("expected oversized payload to be rejected")
	}
	if err := ValidateMsgpackPayloadSize(64); err != nil {
		t.Fatalf("expected size at limit to pass, got %v", err)
	}
}

func TestDecodeMsgpackBytes_RespectsLimit(t *testing.T) {
	payload, err := msgpack.Marshal(map[string]string{"k": "v"})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	t.Setenv(msgpackMaxBytesEnvKey, "1")
	resetMsgpackLimitCacheForTest()
	var out map[string]string
	if err := DecodeMsgpackBytes(payload, &out); err == nil {
		t.Fatalf("expected decode to fail for payload above configured limit")
	}

	t.Setenv(msgpackMaxBytesEnvKey, "1048576")
	resetMsgpackLimitCacheForTest()
	if err := DecodeMsgpackBytes(payload, &out); err != nil {
		t.Fatalf("expected decode to pass, got %v", err)
	}
	if out["k"] != "v" {
		t.Fatalf("unexpected decode result: %#v", out)
	}
}

func TestDecodeMsgpackFile_RespectsLimit(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "msgpack-*.bin")
	if err != nil {
		t.Fatalf("CreateTemp failed: %v", err)
	}
	defer tmp.Close()

	payload, err := msgpack.Marshal(map[string]string{"k": "v"})
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if _, err := tmp.Seek(0, 0); err != nil {
		t.Fatalf("seek failed: %v", err)
	}

	t.Setenv(msgpackMaxBytesEnvKey, "1")
	resetMsgpackLimitCacheForTest()
	var out map[string]string
	if err := DecodeMsgpackFile(tmp, &out); err == nil {
		t.Fatalf("expected file decode to fail for payload above configured limit")
	}
}
