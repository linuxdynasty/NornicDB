package storage

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStorageSerializerMsgpackRoundTrip(t *testing.T) {
	prev := currentStorageSerializer()
	require.NoError(t, SetStorageSerializer(StorageSerializerMsgpack))
	t.Cleanup(func() {
		_ = SetStorageSerializer(prev)
	})

	node := &Node{
		ID:         NodeID("node-1"),
		Labels:     []string{"Person"},
		Properties: map[string]any{"age": int64(42), "name": "Alice"},
		CreatedAt:  time.Unix(1700000000, 0).UTC(),
	}

	data, _, err := encodeNode(node)
	require.NoError(t, err)

	decoded, err := decodeNode(data)
	require.NoError(t, err)
	require.Equal(t, node.ID, decoded.ID)
	require.Equal(t, node.Labels, decoded.Labels)
	require.Equal(t, node.Properties, decoded.Properties)
	require.True(t, decoded.CreatedAt.Equal(node.CreatedAt))
}

func TestDecodeNode_LegacyGobFallback(t *testing.T) {
	prev := currentStorageSerializer()
	require.NoError(t, SetStorageSerializer(StorageSerializerMsgpack))
	t.Cleanup(func() {
		_ = SetStorageSerializer(prev)
	})

	node := &Node{
		ID:         NodeID("legacy-node"),
		Labels:     []string{"Legacy"},
		Properties: map[string]any{"count": int64(7)},
	}

	legacyData, err := encodeWithSerializer(StorageSerializerGob, node)
	require.NoError(t, err)

	decoded, err := decodeNode(legacyData)
	require.NoError(t, err)
	require.Equal(t, node.ID, decoded.ID)
	require.Equal(t, node.Labels, decoded.Labels)
	require.Equal(t, node.Properties, decoded.Properties)
}

func TestDetectStoredSerializerMismatchUsesDetected(t *testing.T) {
	dir := t.TempDir()

	base, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:    dir,
		Serializer: StorageSerializerGob,
	})
	require.NoError(t, err)
	engine := NewNamespacedEngine(base, "test")
	_, err = engine.CreateNode(&Node{
		ID:     NodeID("node-1"),
		Labels: []string{"Person"},
	})
	require.NoError(t, err)
	require.NoError(t, base.Close())

	base2, err := NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:    dir,
		Serializer: StorageSerializerMsgpack,
	})
	require.NoError(t, err)
	defer base2.Close()

	require.Equal(t, StorageSerializerGob, currentStorageSerializer())
}

func TestStorageSerializer_HelperCoverage(t *testing.T) {
	t.Run("parse and set serializer validation", func(t *testing.T) {
		prev := currentStorageSerializer()
		t.Cleanup(func() {
			_ = SetStorageSerializer(prev)
		})

		parsed, err := ParseStorageSerializer("  MSGPACK ")
		require.NoError(t, err)
		require.Equal(t, StorageSerializerMsgpack, parsed)

		require.NoError(t, SetStorageSerializer(StorageSerializerMsgpack))
		require.Equal(t, StorageSerializerMsgpack, currentStorageSerializer())

		_, err = ParseStorageSerializer("bogus")
		require.ErrorContains(t, err, "unsupported storage serializer")

		err = SetStorageSerializer(StorageSerializer("bogus"))
		require.ErrorContains(t, err, "unsupported storage serializer")
	})

	t.Run("current serializer falls back to gob when unset", func(t *testing.T) {
		prev := activeSerializer
		activeSerializer = atomic.Value{}
		t.Cleanup(func() {
			activeSerializer = prev
		})
		require.Equal(t, StorageSerializerGob, currentStorageSerializer())
	})

	t.Run("serializer id mappings and invalid ids", func(t *testing.T) {
		id, err := serializerIDFor(StorageSerializerGob)
		require.NoError(t, err)
		require.Equal(t, serializerIDGob, id)

		id, err = serializerIDFor(StorageSerializerMsgpack)
		require.NoError(t, err)
		require.Equal(t, serializerIDMsgpack, id)

		_, err = serializerIDFor(StorageSerializer("bad"))
		require.ErrorContains(t, err, "unsupported storage serializer")

		serializer, err := serializerFromID(serializerIDGob)
		require.NoError(t, err)
		require.Equal(t, StorageSerializerGob, serializer)

		serializer, err = serializerFromID(serializerIDMsgpack)
		require.NoError(t, err)
		require.Equal(t, StorageSerializerMsgpack, serializer)

		_, err = serializerFromID(99)
		require.ErrorContains(t, err, "unsupported storage serializer id")
	})

	t.Run("split header handles version and serializer errors", func(t *testing.T) {
		serializer, payload, ok, err := splitSerializationHeader([]byte("tiny"))
		require.NoError(t, err)
		require.False(t, ok)
		require.Empty(t, serializer)
		require.Nil(t, payload)

		serializer, payload, ok, err = splitSerializationHeader([]byte("plain-gob-data"))
		require.NoError(t, err)
		require.False(t, ok)
		require.Empty(t, serializer)
		require.Nil(t, payload)

		badVersion := append([]byte(serializationMagic), byte(99), serializerIDGob)
		_, _, _, err = splitSerializationHeader(badVersion)
		require.ErrorContains(t, err, "unsupported serialization version")

		badID := append([]byte(serializationMagic), serializationVersion, byte(99))
		_, _, _, err = splitSerializationHeader(badID)
		require.ErrorContains(t, err, "unsupported storage serializer id")
	})

	t.Run("encode and decode with serializer validation", func(t *testing.T) {
		type sample struct {
			Name string
			Age  int
		}

		encoded, err := encodeWithSerializer(StorageSerializerMsgpack, sample{Name: "Alice", Age: 7})
		require.NoError(t, err)

		var decoded sample
		require.NoError(t, decodeWithSerializer(StorageSerializerMsgpack, encoded, &decoded))
		require.Equal(t, sample{Name: "Alice", Age: 7}, decoded)

		_, err = encodeWithSerializer(StorageSerializer("bad"), sample{})
		require.ErrorContains(t, err, "unsupported storage serializer")

		err = decodeWithSerializer(StorageSerializer("bad"), encoded, &decoded)
		require.ErrorContains(t, err, "unsupported storage serializer")
	})

	t.Run("serialize and deserialize edge plus error paths", func(t *testing.T) {
		prev := currentStorageSerializer()
		require.NoError(t, SetStorageSerializer(StorageSerializerMsgpack))
		t.Cleanup(func() {
			_ = SetStorageSerializer(prev)
		})

		edge := &Edge{
			ID:        EdgeID("edge-1"),
			StartNode: NodeID("node-1"),
			EndNode:   NodeID("node-2"),
			Type:      "KNOWS",
			Properties: map[string]any{
				"weight": int64(3),
			},
		}

		data, err := serializeEdge(edge)
		require.NoError(t, err)

		decoded, err := deserializeEdge(data)
		require.NoError(t, err)
		require.Equal(t, edge.ID, decoded.ID)
		require.Equal(t, edge.Type, decoded.Type)
		require.Equal(t, edge.Properties, decoded.Properties)

		_, err = deserializeEdge([]byte("not-a-valid-edge"))
		require.ErrorContains(t, err, "decoding edge")

		_, err = deserializeNode([]byte("not-a-valid-node"))
		require.ErrorContains(t, err, "decoding node")
	})
}
