package cypher

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ========================================
// Temporal Helper Procedures
// ========================================

// callDbTemporalAssertNoOverlap implements db.temporal.assertNoOverlap
// Syntax:
//
//	CALL db.temporal.assertNoOverlap(label, keyProp, validFromProp, validToProp, keyValue, newValidFrom, newValidTo)
//
// This returns ok=true if no overlaps are detected, otherwise returns an error.
// newValidTo can be null to indicate an open-ended interval.
func (e *StorageExecutor) callDbTemporalAssertNoOverlap(ctx context.Context, cypher string) (*ExecuteResult, error) {
	args, err := parseTemporalCallArgs(ctx, cypher, "DB.TEMPORAL.ASSERTNOOVERLAP")
	if err != nil {
		return nil, err
	}
	if len(args) < 7 {
		return nil, fmt.Errorf("db.temporal.assertNoOverlap requires 7 parameters")
	}

	label, err := coerceStringArg(args[0], "label")
	if err != nil {
		return nil, err
	}
	keyProp, err := coerceStringArg(args[1], "keyProp")
	if err != nil {
		return nil, err
	}
	validFromProp, err := coerceStringArg(args[2], "validFromProp")
	if err != nil {
		return nil, err
	}
	validToProp, err := coerceStringArg(args[3], "validToProp")
	if err != nil {
		return nil, err
	}

	keyValue := args[4]
	newStart, ok := coerceDateTime(args[5])
	if !ok {
		return nil, fmt.Errorf("newValidFrom must be a valid datetime")
	}
	newEnd, newHasEnd := coerceDateTimeOptional(args[6])

	nodes, err := e.storage.GetNodesByLabel(label)
	if err != nil {
		return nil, fmt.Errorf("failed to read nodes for label %q: %w", label, err)
	}

	for _, node := range nodes {
		if node == nil {
			continue
		}
		if !valuesEqual(node.Properties[keyProp], keyValue) {
			continue
		}

		existingStart, ok := coerceDateTime(node.Properties[validFromProp])
		if !ok {
			continue
		}
		existingEnd, existingHasEnd := coerceDateTimeOptional(node.Properties[validToProp])

		if intervalsOverlap(newStart, newEnd, newHasEnd, existingStart, existingEnd, existingHasEnd) {
			return nil, fmt.Errorf("temporal overlap detected for %s=%v", keyProp, keyValue)
		}
	}

	return &ExecuteResult{
		Columns: []string{"ok"},
		Rows:    [][]interface{}{{true}},
	}, nil
}

// callDbTemporalAsOf implements db.temporal.asOf
// Syntax:
//
//	CALL db.temporal.asOf(label, keyProp, keyValue, validFromProp, validToProp, asOf) YIELD node
//
// Returns the most recent node whose [valid_from, valid_to) covers asOf.
func (e *StorageExecutor) callDbTemporalAsOf(ctx context.Context, cypher string) (*ExecuteResult, error) {
	args, err := parseTemporalCallArgs(ctx, cypher, "DB.TEMPORAL.ASOF")
	if err != nil {
		return nil, err
	}
	if len(args) < 6 {
		return nil, fmt.Errorf("db.temporal.asOf requires 6 parameters")
	}

	label, err := coerceStringArg(args[0], "label")
	if err != nil {
		return nil, err
	}
	keyProp, err := coerceStringArg(args[1], "keyProp")
	if err != nil {
		return nil, err
	}
	keyValue := args[2]
	validFromProp, err := coerceStringArg(args[3], "validFromProp")
	if err != nil {
		return nil, err
	}
	validToProp, err := coerceStringArg(args[4], "validToProp")
	if err != nil {
		return nil, err
	}
	asOf, ok := coerceDateTime(args[5])
	if !ok {
		return nil, fmt.Errorf("asOf must be a valid datetime")
	}

	if temporalLookup, ok := e.storage.(storage.TemporalLookupEngine); ok {
		node, err := temporalLookup.GetTemporalNodeAsOf(label, keyProp, keyValue, validFromProp, validToProp, asOf)
		if err != nil {
			return nil, fmt.Errorf("temporal lookup failed for label %q: %w", label, err)
		}
		if node != nil {
			return &ExecuteResult{
				Columns: []string{"node"},
				Rows:    [][]interface{}{{node}},
			}, nil
		}
	}

	nodes, err := e.storage.GetNodesByLabel(label)
	if err != nil {
		return nil, fmt.Errorf("failed to read nodes for label %q: %w", label, err)
	}

	var bestNode interface{}
	var bestStart time.Time
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if !valuesEqual(node.Properties[keyProp], keyValue) {
			continue
		}

		start, ok := coerceDateTime(node.Properties[validFromProp])
		if !ok {
			continue
		}
		end, hasEnd := coerceDateTimeOptional(node.Properties[validToProp])

		if asOf.Before(start) {
			continue
		}
		if hasEnd && !asOf.Before(end) {
			continue
		}

		if bestNode == nil || start.After(bestStart) {
			bestNode = node
			bestStart = start
		}
	}

	if bestNode == nil {
		return &ExecuteResult{
			Columns: []string{"node"},
			Rows:    [][]interface{}{},
		}, nil
	}

	return &ExecuteResult{
		Columns: []string{"node"},
		Rows:    [][]interface{}{{bestNode}},
	}, nil
}

func parseTemporalCallArgs(ctx context.Context, cypher, callName string) ([]interface{}, error) {
	upper := strings.ToUpper(cypher)
	needle := strings.ToUpper(callName) + "("
	start := strings.Index(upper, needle)
	if start == -1 {
		return nil, fmt.Errorf("invalid %s syntax", strings.ToLower(callName))
	}
	start += len(needle)
	endRel := strings.Index(cypher[start:], ")")
	if endRel == -1 {
		return nil, fmt.Errorf("missing closing parenthesis in %s", strings.ToLower(callName))
	}
	rawArgs := strings.TrimSpace(cypher[start : start+endRel])
	parts := splitTopLevelComma(rawArgs)

	args := make([]interface{}, 0, len(parts))
	for _, part := range parts {
		value := resolveTemporalArg(ctx, strings.TrimSpace(part))
		args = append(args, value)
	}
	return args, nil
}

func resolveTemporalArg(ctx context.Context, raw string) interface{} {
	if raw == "" {
		return nil
	}
	upper := strings.ToUpper(raw)
	if upper == "NULL" {
		return nil
	}
	if strings.HasPrefix(raw, "$") {
		if params := getParamsFromContext(ctx); params != nil {
			name := strings.TrimPrefix(raw, "$")
			if val, ok := params[name]; ok {
				return val
			}
		}
		return nil
	}
	if (strings.HasPrefix(raw, "'") && strings.HasSuffix(raw, "'")) ||
		(strings.HasPrefix(raw, "\"") && strings.HasSuffix(raw, "\"")) {
		return strings.Trim(raw, "\"'")
	}
	if i, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil {
		return f
	}
	return raw
}

func coerceStringArg(val interface{}, name string) (string, error) {
	if val == nil {
		return "", fmt.Errorf("%s is required", name)
	}
	switch v := val.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return "", fmt.Errorf("%s cannot be empty", name)
		}
		return v, nil
	default:
		return fmt.Sprint(val), nil
	}
}

func coerceDateTimeOptional(val interface{}) (time.Time, bool) {
	if val == nil {
		return time.Time{}, false
	}
	return coerceDateTime(val)
}

func coerceDateTime(val interface{}) (time.Time, bool) {
	switch v := val.(type) {
	case time.Time:
		return v, true
	case string:
		t := parseDateTime(v)
		if t.IsZero() {
			return time.Time{}, false
		}
		return t, true
	case int64:
		return time.Unix(v, 0).UTC(), true
	case float64:
		return time.Unix(int64(v), 0).UTC(), true
	default:
		if s, ok := val.(fmt.Stringer); ok {
			t := parseDateTime(s.String())
			if t.IsZero() {
				return time.Time{}, false
			}
			return t, true
		}
		return time.Time{}, false
	}
}

func intervalsOverlap(aStart, aEnd time.Time, aHasEnd bool, bStart, bEnd time.Time, bHasEnd bool) bool {
	if aStart.IsZero() || bStart.IsZero() {
		return false
	}
	if bHasEnd && !aStart.Before(bEnd) {
		return false
	}
	if aHasEnd && !bStart.Before(aEnd) {
		return false
	}
	return true
}

func valuesEqual(a, b interface{}) bool {
	if reflect.DeepEqual(a, b) {
		return true
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}
