package fabric

// ResultStream holds the tabular output of a fragment execution.
// It carries column names and rows in the same shape as cypher.ExecuteResult.
type ResultStream struct {
	// Columns is the ordered list of column names.
	Columns []string

	// Rows holds the result data. Each row has len(Columns) elements.
	Rows [][]interface{}
}

// Empty returns true if the result stream has no rows.
func (r *ResultStream) Empty() bool {
	return r == nil || len(r.Rows) == 0
}

// RowCount returns the number of rows.
func (r *ResultStream) RowCount() int {
	if r == nil {
		return 0
	}
	return len(r.Rows)
}

// Merge appends the rows from other into this result stream.
// Columns must match. If this stream has no columns yet, they are adopted from other.
func (r *ResultStream) Merge(other *ResultStream) {
	if other == nil || len(other.Rows) == 0 {
		return
	}
	if len(r.Columns) == 0 {
		r.Columns = other.Columns
	}
	r.Rows = append(r.Rows, other.Rows...)
}
