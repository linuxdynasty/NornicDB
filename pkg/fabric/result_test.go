package fabric

import (
	"testing"
)

func TestResultStream_Empty(t *testing.T) {
	var nilStream *ResultStream
	if !nilStream.Empty() {
		t.Error("nil stream should be empty")
	}

	emptyStream := &ResultStream{}
	if !emptyStream.Empty() {
		t.Error("empty stream should be empty")
	}

	nonEmpty := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{1}},
	}
	if nonEmpty.Empty() {
		t.Error("non-empty stream should not be empty")
	}
}

func TestResultStream_RowCount(t *testing.T) {
	var nilStream *ResultStream
	if nilStream.RowCount() != 0 {
		t.Errorf("nil stream should have 0 rows, got %d", nilStream.RowCount())
	}

	stream := &ResultStream{
		Columns: []string{"a"},
		Rows:    [][]interface{}{{1}, {2}, {3}},
	}
	if stream.RowCount() != 3 {
		t.Errorf("expected 3 rows, got %d", stream.RowCount())
	}
}

func TestResultStream_Merge(t *testing.T) {
	a := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{1}, {2}},
	}
	b := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{3}, {4}},
	}

	a.Merge(b)
	if a.RowCount() != 4 {
		t.Errorf("expected 4 rows after merge, got %d", a.RowCount())
	}
}

func TestResultStream_MergeAdoptsColumns(t *testing.T) {
	a := &ResultStream{}
	b := &ResultStream{
		Columns: []string{"y"},
		Rows:    [][]interface{}{{99}},
	}

	a.Merge(b)
	if len(a.Columns) != 1 || a.Columns[0] != "y" {
		t.Errorf("expected columns [y], got %v", a.Columns)
	}
}

func TestResultStream_MergeNil(t *testing.T) {
	a := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{1}},
	}

	a.Merge(nil)
	if a.RowCount() != 1 {
		t.Errorf("expected 1 row after nil merge, got %d", a.RowCount())
	}
}

func TestResultStream_MergeEmptyRows(t *testing.T) {
	a := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{1}},
	}
	b := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{},
	}

	a.Merge(b)
	if a.RowCount() != 1 {
		t.Errorf("expected 1 row after empty merge, got %d", a.RowCount())
	}
}

func TestResultStream_MergeSkipsMismatchedColumns(t *testing.T) {
	a := &ResultStream{
		Columns: []string{"x"},
		Rows:    [][]interface{}{{1}},
	}
	b := &ResultStream{
		Columns: []string{"y"},
		Rows:    [][]interface{}{{2}},
	}

	a.Merge(b)
	if got := a.RowCount(); got != 1 {
		t.Fatalf("expected mismatched merge to be ignored, got %d rows", got)
	}
}
