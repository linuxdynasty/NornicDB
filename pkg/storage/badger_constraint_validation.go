package storage

import (
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// compareValues compares two property values for equality.
// This mirrors Cypher semantics for numeric comparisons across int/float types.
func compareValues(a, b interface{}) bool {
	// Handle different numeric types
	switch v1 := a.(type) {
	case int:
		switch v2 := b.(type) {
		case int:
			return v1 == v2
		case int64:
			return int64(v1) == v2
		case float64:
			return float64(v1) == v2
		}
	case int64:
		switch v2 := b.(type) {
		case int:
			return v1 == int64(v2)
		case int64:
			return v1 == v2
		case float64:
			return float64(v1) == v2
		}
	case float64:
		switch v2 := b.(type) {
		case int:
			return v1 == float64(v2)
		case int64:
			return v1 == float64(v2)
		case float64:
			return v1 == v2
		}
	case string:
		if v2, ok := b.(string); ok {
			return v1 == v2
		}
	case bool:
		if v2, ok := b.(bool); ok {
			return v1 == v2
		}
	}

	// Default comparison
	return a == b
}

func (b *BadgerEngine) validateNodeConstraintsInTxn(txn *badger.Txn, node *Node, schema *SchemaManager, namespace string, excludeNodeID NodeID) error {
	if node == nil || schema == nil {
		return nil
	}

	constraints := schema.GetConstraintsForLabels(node.Labels)
	for _, c := range constraints {
		switch c.Type {
		case ConstraintUnique:
			if len(c.Properties) != 1 {
				continue
			}
			prop := c.Properties[0]
			value := node.Properties[prop]
			if value == nil {
				continue
			}
			if err := b.scanForUniqueViolationInTxn(txn, namespace, c.Label, prop, value, excludeNodeID); err != nil {
				return err
			}
		case ConstraintNodeKey:
			values := make([]interface{}, len(c.Properties))
			for i, prop := range c.Properties {
				values[i] = node.Properties[prop]
				if values[i] == nil {
					return &ConstraintViolationError{
						Type:       ConstraintNodeKey,
						Label:      c.Label,
						Properties: c.Properties,
						Message:    fmt.Sprintf("NODE KEY property %s cannot be null", prop),
					}
				}
			}
			if err := b.scanForNodeKeyViolationInTxn(txn, namespace, c.Label, c.Properties, values, excludeNodeID); err != nil {
				return err
			}
		case ConstraintExists:
			if len(c.Properties) != 1 {
				continue
			}
			prop := c.Properties[0]
			if node.Properties == nil {
				return &ConstraintViolationError{
					Type:       ConstraintExists,
					Label:      c.Label,
					Properties: []string{prop},
					Message:    fmt.Sprintf("Required property %s is missing", prop),
				}
			}
			if val, ok := node.Properties[prop]; !ok || val == nil {
				return &ConstraintViolationError{
					Type:       ConstraintExists,
					Label:      c.Label,
					Properties: []string{prop},
					Message:    fmt.Sprintf("Required property %s is missing", prop),
				}
			}
		case ConstraintTemporal:
			if len(c.Properties) != 3 {
				return fmt.Errorf("TEMPORAL constraint requires 3 properties (key, valid_from, valid_to)")
			}
			keyProp := c.Properties[0]
			startProp := c.Properties[1]
			endProp := c.Properties[2]

			keyVal := node.Properties[keyProp]
			if keyVal == nil {
				return &ConstraintViolationError{
					Type:       ConstraintTemporal,
					Label:      c.Label,
					Properties: c.Properties,
					Message:    fmt.Sprintf("TEMPORAL key property %s cannot be null", keyProp),
				}
			}
			start, ok := coerceTemporalTime(node.Properties[startProp])
			if !ok {
				return &ConstraintViolationError{
					Type:       ConstraintTemporal,
					Label:      c.Label,
					Properties: c.Properties,
					Message:    fmt.Sprintf("TEMPORAL start property %s must be a datetime", startProp),
				}
			}
			end, hasEnd := coerceTemporalTime(node.Properties[endProp])

			if err := b.scanForTemporalOverlapInTxn(txn, namespace, c.Label, keyProp, startProp, endProp, keyVal, start, end, hasEnd, excludeNodeID); err != nil {
				return err
			}
		}
	}

	typeConstraints := schema.GetPropertyTypeConstraintsForLabels(node.Labels)
	for _, c := range typeConstraints {
		value := node.Properties[c.Property]
		if err := ValidatePropertyType(value, c.ExpectedType); err != nil {
			return &ConstraintViolationError{
				Type:       ConstraintPropertyType,
				Label:      c.Label,
				Properties: []string{c.Property},
				Message:    fmt.Sprintf("Property %s must be %s (%v)", c.Property, c.ExpectedType, err),
			}
		}
	}

	return nil
}

func (b *BadgerEngine) scanForUniqueViolationInTxn(txn *badger.Txn, namespace, label, property string, value interface{}, excludeNodeID NodeID) error {
	prefix := labelIndexPrefix(label)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	iter := txn.NewIterator(opts)
	defer iter.Close()

	labelLen := len(strings.ToLower(label))
	nsPrefix := namespace + ":"
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		key := iter.Item().Key()
		nodeID := extractNodeIDFromLabelIndex(key, labelLen)
		if nodeID == "" || nodeID == excludeNodeID {
			continue
		}
		if !strings.HasPrefix(string(nodeID), nsPrefix) {
			continue
		}

		item, err := txn.Get(nodeKey(nodeID))
		if err != nil {
			continue
		}

		var nodeBytes []byte
		if err := item.Value(func(val []byte) error {
			nodeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			continue
		}

		existingNode, err := decodeNode(nodeBytes)
		if err != nil {
			continue
		}

		if existingValue, ok := existingNode.Properties[property]; ok {
			if compareValues(existingValue, value) {
				return &ConstraintViolationError{
					Type:       ConstraintUnique,
					Label:      label,
					Properties: []string{property},
					Message:    fmt.Sprintf("Node with %s=%v already exists (nodeID: %s)", property, value, existingNode.ID),
				}
			}
		}
	}

	return nil
}

func (b *BadgerEngine) scanForNodeKeyViolationInTxn(txn *badger.Txn, namespace, label string, properties []string, values []interface{}, excludeNodeID NodeID) error {
	prefix := labelIndexPrefix(label)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	iter := txn.NewIterator(opts)
	defer iter.Close()

	labelLen := len(strings.ToLower(label))
	nsPrefix := namespace + ":"
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		key := iter.Item().Key()
		nodeID := extractNodeIDFromLabelIndex(key, labelLen)
		if nodeID == "" || nodeID == excludeNodeID {
			continue
		}
		if !strings.HasPrefix(string(nodeID), nsPrefix) {
			continue
		}

		item, err := txn.Get(nodeKey(nodeID))
		if err != nil {
			continue
		}

		var nodeBytes []byte
		if err := item.Value(func(val []byte) error {
			nodeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			continue
		}

		existingNode, err := decodeNode(nodeBytes)
		if err != nil {
			continue
		}

		match := true
		for i, prop := range properties {
			existingValue, ok := existingNode.Properties[prop]
			if !ok || !compareValues(existingValue, values[i]) {
				match = false
				break
			}
		}
		if match {
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      label,
				Properties: properties,
				Message:    fmt.Sprintf("Node with key %v=%v already exists (nodeID: %s)", properties, values, existingNode.ID),
			}
		}
	}

	return nil
}

func (b *BadgerEngine) legacyScanForTemporalOverlapInTxn(txn *badger.Txn, namespace, label, keyProp, startProp, endProp string, keyValue interface{}, start time.Time, end time.Time, hasEnd bool, excludeNodeID NodeID) error {
	prefix := labelIndexPrefix(label)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	iter := txn.NewIterator(opts)
	defer iter.Close()

	labelLen := len(strings.ToLower(label))
	nsPrefix := namespace + ":"
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		key := iter.Item().Key()
		nodeID := extractNodeIDFromLabelIndex(key, labelLen)
		if nodeID == "" || nodeID == excludeNodeID {
			continue
		}
		if !strings.HasPrefix(string(nodeID), nsPrefix) {
			continue
		}

		item, err := txn.Get(nodeKey(nodeID))
		if err != nil {
			continue
		}

		var nodeBytes []byte
		if err := item.Value(func(val []byte) error {
			nodeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			continue
		}

		existingNode, err := decodeNode(nodeBytes)
		if err != nil {
			continue
		}

		existingKey, ok := existingNode.Properties[keyProp]
		if !ok || existingKey == nil {
			continue
		}
		if !compareValues(existingKey, keyValue) {
			continue
		}

		existingStart, ok := coerceTemporalTime(existingNode.Properties[startProp])
		if !ok {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      label,
				Properties: []string{keyProp, startProp, endProp},
				Message:    fmt.Sprintf("TEMPORAL constraint requires %s for node %s", startProp, existingNode.ID),
			}
		}
		existingEnd, existingHasEnd := coerceTemporalTime(existingNode.Properties[endProp])

		if intervalsOverlap(temporalInterval{
			start:  start,
			end:    end,
			hasEnd: hasEnd,
		}, temporalInterval{
			start:  existingStart,
			end:    existingEnd,
			hasEnd: existingHasEnd,
		}) {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      label,
				Properties: []string{keyProp, startProp, endProp},
				Message: fmt.Sprintf("TEMPORAL constraint violation: overlap with node %s for %s=%v",
					existingNode.ID, keyProp, keyValue),
			}
		}
	}

	return nil
}

func (b *BadgerEngine) scanForTemporalOverlapInTxn(txn *badger.Txn, namespace, label, keyProp, startProp, endProp string, keyValue interface{}, start time.Time, end time.Time, hasEnd bool, excludeNodeID NodeID) error {
	constraint := Constraint{
		Type:       ConstraintTemporal,
		Label:      label,
		Properties: []string{keyProp, startProp, endProp},
	}
	target := temporalRefreshTarget{
		constraint: constraint,
		desc:       makeTemporalDescriptor(namespace, constraint, keyValue),
		keyValue:   keyValue,
	}
	prefix := temporalHistoryPrefix(target.desc)
	hasIndexedEntries := false
	it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
	for it.Rewind(); it.Valid(); it.Next() {
		hasIndexedEntries = true
		break
	}
	it.Close()
	if !hasIndexedEntries {
		return b.legacyScanForTemporalOverlapInTxn(txn, namespace, label, keyProp, startProp, endProp, keyValue, start, end, hasEnd, excludeNodeID)
	}

	prevNode, nextNode, err := b.temporalAdjacentNodesInTxn(txn, target, start, excludeNodeID)
	if err != nil {
		return err
	}
	if prevNode != nil {
		_, prevStart, prevEnd, prevHasEnd, ok := temporalNodeState(prevNode, constraint)
		if ok && intervalsOverlap(
			temporalInterval{start: start, end: end, hasEnd: hasEnd},
			temporalInterval{start: prevStart, end: prevEnd, hasEnd: prevHasEnd},
		) {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      label,
				Properties: []string{keyProp, startProp, endProp},
				Message: fmt.Sprintf("TEMPORAL constraint violation: overlap with node %s for %s=%v",
					prevNode.ID, keyProp, keyValue),
			}
		}
	}
	if nextNode != nil {
		_, nextStart, nextEnd, nextHasEnd, ok := temporalNodeState(nextNode, constraint)
		if ok && intervalsOverlap(
			temporalInterval{start: start, end: end, hasEnd: hasEnd},
			temporalInterval{start: nextStart, end: nextEnd, hasEnd: nextHasEnd},
		) {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      label,
				Properties: []string{keyProp, startProp, endProp},
				Message: fmt.Sprintf("TEMPORAL constraint violation: overlap with node %s for %s=%v",
					nextNode.ID, keyProp, keyValue),
			}
		}
	}

	return nil
}

func constraintValueKey(value interface{}) string {
	return NewCompositeKey(value).Hash
}

func constraintCompositeKey(values []interface{}) string {
	return NewCompositeKey(values...).Hash
}
