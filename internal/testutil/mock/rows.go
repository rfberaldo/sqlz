package mock

type Rows struct {
	CloseFunc   func() error
	ColumnsFunc func() ([]string, error)
	ErrFunc     func() error
	NextFunc    func() bool
	ScanFunc    func(dest ...any) error
}

func (m *Rows) Close() error {
	if m.CloseFunc == nil {
		return nil
	}
	return m.CloseFunc()
}

func (m *Rows) Columns() ([]string, error) {
	if m.ColumnsFunc == nil {
		return nil, nil
	}
	return m.ColumnsFunc()
}

func (m *Rows) Err() error {
	if m.ErrFunc == nil {
		return nil
	}
	return m.ErrFunc()
}

func (m *Rows) Next() bool {
	if m.NextFunc == nil {
		return false
	}
	return m.NextFunc()
}

func (m *Rows) Scan(dest ...any) error {
	if m.ScanFunc == nil {
		return nil
	}
	return m.ScanFunc(dest...)
}
