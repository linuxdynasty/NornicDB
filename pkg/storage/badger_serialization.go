// Package storage - Serialization helpers for BadgerDB.
package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/util"
	"github.com/vmihailenco/msgpack/v5"
)

// StorageSerializer selects the serialization format used for nodes/edges/embeddings.
type StorageSerializer string

const (
	StorageSerializerGob     StorageSerializer = "gob"
	StorageSerializerMsgpack StorageSerializer = "msgpack"
)

const (
	serializationMagic   = "\xffNDB"
	serializationVersion = byte(1)
	serializerIDGob      = byte(1)
	serializerIDMsgpack  = byte(2)
)

var activeSerializer atomic.Value

// init registers types with gob for proper encoding/decoding of property values.
// gob requires type registration for interface{} values in maps.
func init() {
	// Register primitive types that can appear in Properties map
	gob.Register(int(0))
	gob.Register(int32(0))
	gob.Register(int64(0))
	gob.Register(float32(0))
	gob.Register(float64(0))
	gob.Register("")
	gob.Register(true)
	gob.Register(time.Time{})

	// Register slice types for list properties
	gob.Register([]interface{}{})
	gob.Register([]string{})
	gob.Register([]int{})
	gob.Register([]int32{})
	gob.Register([]int64{})
	gob.Register([]float32{})
	gob.Register([]float64{})
	gob.Register([]bool{})

	// Register map types for nested properties
	gob.Register(map[string]interface{}{})

	activeSerializer.Store(StorageSerializerGob)
}

// SetStorageSerializer sets the active serializer for storage encoding.
func SetStorageSerializer(serializer StorageSerializer) error {
	normalized := normalizeStorageSerializer(string(serializer))
	switch normalized {
	case StorageSerializerGob, StorageSerializerMsgpack:
		activeSerializer.Store(normalized)
		return nil
	default:
		return fmt.Errorf("unsupported storage serializer: %s", serializer)
	}
}

// ParseStorageSerializer normalizes and validates serializer input.
func ParseStorageSerializer(value string) (StorageSerializer, error) {
	normalized := normalizeStorageSerializer(value)
	switch normalized {
	case StorageSerializerGob, StorageSerializerMsgpack:
		return normalized, nil
	default:
		return "", fmt.Errorf("unsupported storage serializer: %s", value)
	}
}

func currentStorageSerializer() StorageSerializer {
	if v := activeSerializer.Load(); v != nil {
		return v.(StorageSerializer)
	}
	return StorageSerializerGob
}

func normalizeStorageSerializer(value string) StorageSerializer {
	return StorageSerializer(strings.ToLower(strings.TrimSpace(value)))
}

func serializerIDFor(serializer StorageSerializer) (byte, error) {
	switch serializer {
	case StorageSerializerGob:
		return serializerIDGob, nil
	case StorageSerializerMsgpack:
		return serializerIDMsgpack, nil
	default:
		return 0, fmt.Errorf("unsupported storage serializer: %s", serializer)
	}
}

func serializerFromID(id byte) (StorageSerializer, error) {
	switch id {
	case serializerIDGob:
		return StorageSerializerGob, nil
	case serializerIDMsgpack:
		return StorageSerializerMsgpack, nil
	default:
		return "", fmt.Errorf("unsupported storage serializer id: %d", id)
	}
}

func encodeValue(value any) ([]byte, error) {
	serializer := currentStorageSerializer()
	payload, err := encodeWithSerializer(serializer, value)
	if err != nil {
		return nil, err
	}
	header := []byte(serializationMagic)
	header = append(header, serializationVersion)
	id, err := serializerIDFor(serializer)
	if err != nil {
		return nil, err
	}
	header = append(header, id)
	out := make([]byte, 0, len(header)+len(payload))
	out = append(out, header...)
	out = append(out, payload...)
	return out, nil
}

func decodeValue(data []byte, value any) error {
	serializer, payload, ok, err := splitSerializationHeader(data)
	if err != nil {
		return err
	}
	if ok {
		return decodeWithSerializer(serializer, payload, value)
	}

	// Legacy fallback: gob without header.
	gobErr := decodeWithSerializer(StorageSerializerGob, data, value)
	if gobErr == nil {
		return nil
	}
	if currentStorageSerializer() != StorageSerializerGob {
		if err := decodeWithSerializer(currentStorageSerializer(), data, value); err == nil {
			return nil
		}
	}
	return gobErr
}

func splitSerializationHeader(data []byte) (StorageSerializer, []byte, bool, error) {
	if len(data) < len(serializationMagic)+2 {
		return "", nil, false, nil
	}
	if string(data[:len(serializationMagic)]) != serializationMagic {
		return "", nil, false, nil
	}
	version := data[len(serializationMagic)]
	if version != serializationVersion {
		return "", nil, false, fmt.Errorf("unsupported serialization version: %d", version)
	}
	serializerID := data[len(serializationMagic)+1]
	serializer, err := serializerFromID(serializerID)
	if err != nil {
		return "", nil, false, err
	}
	return serializer, data[len(serializationMagic)+2:], true, nil
}

func encodeWithSerializer(serializer StorageSerializer, value any) ([]byte, error) {
	switch serializer {
	case StorageSerializerGob:
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(value); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case StorageSerializerMsgpack:
		return msgpack.Marshal(value)
	default:
		return nil, fmt.Errorf("unsupported storage serializer: %s", serializer)
	}
}

func decodeWithSerializer(serializer StorageSerializer, data []byte, value any) error {
	switch serializer {
	case StorageSerializerGob:
		return gob.NewDecoder(bytes.NewReader(data)).Decode(value)
	case StorageSerializerMsgpack:
		return util.DecodeMsgpackBytes(data, value)
	default:
		return fmt.Errorf("unsupported storage serializer: %s", serializer)
	}
}

// serializeNode converts a Node to bytes for BadgerDB storage.
// The active serializer is stored in a small header for auto-detection.
func serializeNode(node *Node) ([]byte, error) {
	data, err := encodeValue(node)
	if err != nil {
		return nil, fmt.Errorf("encoding node: %w", err)
	}
	return data, nil
}

// deserializeNode converts gob bytes back to a Node.
func deserializeNode(data []byte) (*Node, error) {
	var node Node
	if err := decodeValue(data, &node); err != nil {
		return nil, fmt.Errorf("decoding node: %w", err)
	}
	return &node, nil
}

// serializeEdge converts an Edge to bytes for BadgerDB storage.
func serializeEdge(edge *Edge) ([]byte, error) {
	data, err := encodeValue(edge)
	if err != nil {
		return nil, fmt.Errorf("encoding edge: %w", err)
	}
	return data, nil
}

// deserializeEdge converts gob bytes back to an Edge.
func deserializeEdge(data []byte) (*Edge, error) {
	var edge Edge
	if err := decodeValue(data, &edge); err != nil {
		return nil, fmt.Errorf("decoding edge: %w", err)
	}
	return &edge, nil
}
