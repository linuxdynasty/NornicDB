package cypher

import (
	"reflect"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func getNodePropertyValue(node *storage.Node, propName string) (any, bool) {
	if node == nil {
		return nil, false
	}
	if propName == "has_embedding" {
		if node.EmbedMeta != nil {
			if val, ok := node.EmbedMeta["has_embedding"]; ok {
				return val, true
			}
		}
		return len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0, true
	}
	val, ok := node.Properties[propName]
	return val, ok
}

func getBindingNodeValue(node *storage.Node, propName string) (any, bool) {
	if node == nil {
		return nil, false
	}
	if propName == "id" {
		if val, ok := node.Properties["id"]; ok {
			return val, true
		}
		return string(node.ID), true
	}
	return getNodePropertyValue(node, propName)
}

func buildComparableMembershipIndex(items []interface{}) (map[interface{}]struct{}, []interface{}) {
	comparableSet := make(map[interface{}]struct{}, len(items))
	nonComparable := make([]interface{}, 0)
	for _, item := range items {
		if item == nil {
			continue
		}
		if isComparableValue(item) {
			comparableSet[item] = struct{}{}
			continue
		}
		nonComparable = append(nonComparable, item)
	}
	return comparableSet, nonComparable
}

func evaluateComparableMembership(actual interface{}, comparableSet map[interface{}]struct{}, nonComparable []interface{}, equals func(interface{}, interface{}) bool) bool {
	if actual == nil {
		return false
	}
	if isComparableValue(actual) {
		if _, hit := comparableSet[actual]; hit {
			return true
		}
	}
	for _, item := range nonComparable {
		if equals(actual, item) {
			return true
		}
	}
	return false
}

func isComparableValue(v interface{}) bool {
	if v == nil {
		return false
	}
	return reflect.TypeOf(v).Comparable()
}
