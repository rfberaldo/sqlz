package mock

import "database/sql"

type Rows struct {
	CloseFunc         func() error
	ColumnTypesFunc   func() ([]*sql.ColumnType, error)
	ColumnsFunc       func() ([]string, error)
	ErrFunc           func() error
	NextFunc          func() bool
	NextResultSetFunc func() bool
	ScanFunc          func(dest ...any) error
}

func (m *Rows) Close() error {
	if m.CloseFunc == nil {
		return nil
	}
	return m.CloseFunc()
}

func (m *Rows) ColumnTypes() ([]*sql.ColumnType, error) {
	if m.ColumnTypesFunc == nil {
		return nil, nil
	}
	return m.ColumnTypesFunc()
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

func (m *Rows) NextResultSet() bool {
	if m.NextResultSetFunc == nil {
		return false
	}
	return m.NextResultSetFunc()
}

func (m *Rows) Scan(dest ...any) error {
	if m.ScanFunc == nil {
		return nil
	}
	return m.ScanFunc(dest...)
}
