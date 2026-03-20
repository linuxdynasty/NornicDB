package util

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

const (
	defaultMaxMsgpackDecodeBytes = 256 * 1024 * 1024 // 256 MiB
	msgpackMaxBytesEnvKey        = "NORNICDB_MAX_MSGPACK_DECODE_BYTES"
)

var (
	maxMsgpackDecodeBytesOnce sync.Once
	maxMsgpackDecodeBytes     int64
)

// MaxMsgpackDecodeBytes returns the configured maximum number of bytes accepted
// for msgpack decode operations.
func MaxMsgpackDecodeBytes() int64 {
	maxMsgpackDecodeBytesOnce.Do(func() {
		maxMsgpackDecodeBytes = defaultMaxMsgpackDecodeBytes
		raw := os.Getenv(msgpackMaxBytesEnvKey)
		if raw == "" {
			return
		}
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v <= 0 {
			return
		}
		maxMsgpackDecodeBytes = v
	})
	return maxMsgpackDecodeBytes
}

// ValidateMsgpackPayloadSize rejects payloads larger than the configured cap.
func ValidateMsgpackPayloadSize(size int64) error {
	limit := MaxMsgpackDecodeBytes()
	if size < 0 {
		return fmt.Errorf("invalid msgpack payload size: %d", size)
	}
	if size > limit {
		return fmt.Errorf("msgpack payload exceeds decode limit (%d > %d bytes)", size, limit)
	}
	return nil
}

// DecodeMsgpackBytes safely decodes msgpack from an in-memory payload with size validation.
func DecodeMsgpackBytes(data []byte, dst any) error {
	if err := ValidateMsgpackPayloadSize(int64(len(data))); err != nil {
		return err
	}
	return msgpack.NewDecoder(bytes.NewReader(data)).Decode(dst)
}

// DecodeMsgpackFile safely decodes msgpack from a file, rejecting oversized payloads.
func DecodeMsgpackFile(file *os.File, dst any) error {
	stat, err := file.Stat()
	if err == nil {
		if err := ValidateMsgpackPayloadSize(stat.Size()); err != nil {
			return err
		}
	}
	return msgpack.NewDecoder(file).Decode(dst)
}
