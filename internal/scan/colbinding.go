package scan

// ColBinding is a helper for map scanner.
type ColBinding struct {
	columns []string
	values  []any
	ptrs    []any
}

func NewColBinding(columns []string) *ColBinding {
	if len(columns) == 0 {
		panic("sqlz/scan: columns length must be > 0")
	}
	cb := &ColBinding{
		columns: columns,
		values:  make([]any, len(columns)),
		ptrs:    make([]any, len(columns)),
	}

	for i := range cb.values {
		cb.ptrs[i] = &cb.values[i]
	}

	return cb
}

func (cb *ColBinding) Value(i int) any {
	v := cb.values[i]
	if v, ok := v.([]byte); ok {
		return string(v)
	}
	return v
}
