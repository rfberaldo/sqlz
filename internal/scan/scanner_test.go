package scan

import (
	"cmp"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rfberaldo/sqlz/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func connect(driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		log.Printf("error connecting to %v: %v", driverName, err)
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		log.Printf("error pinging to %v: %v", driverName, err)
		db.Close()
		return nil, err
	}

	return db, nil
}

func TestScan(t *testing.T) {
	dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
	db, err := connect("mysql", dsn)
	require.NoError(t, err)

	rows, err := db.Query("SELECT * FROM user")
	require.NoError(t, err)

	var id, age int
	var username string
	var value float64

	defer rows.Close()

	fmt.Println(rows.Columns())
	ct, err := rows.ColumnTypes()
	require.NoError(t, err)

	for _, v := range ct {
		fmt.Printf("%#v\n", *v)
	}

	for rows.Next() {
		err = rows.Scan(&id, &username, &age, &value)
		require.NoError(t, err)
	}

	err = rows.Err()
	require.NoError(t, err)

	fmt.Println(id, username, age, value)
}

func TestScanner_ScanMap(t *testing.T) {
	dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
	db, err := connect("mysql", dsn)
	require.NoError(t, err)

	t.Run("should work on allocated map", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM user LIMIT 1")
		require.NoError(t, err)
		scanner := &Scanner{queryRow: true, rows: rows}
		m := make(map[string]any)
		m["abc"] = 2
		err = scanner.Scan(&m)
		require.NoError(t, err)
		assert.Equal(t, 5, len(m))
	})

	t.Run("should work on allocated map pointer", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM user LIMIT 1")
		require.NoError(t, err)
		scanner := &Scanner{queryRow: true, rows: rows}
		m := make(map[string]any)
		err = scanner.Scan(&m)
		require.NoError(t, err)
		assert.Equal(t, 4, len(m))
	})
}

func TestScanner_ScanSliceMap(t *testing.T) {
	dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
	db, err := connect("mysql", dsn)
	require.NoError(t, err)

	t.Run("should work map slice", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM user")
		require.NoError(t, err)
		scanner := &Scanner{rows: rows}
		var m []map[string]any
		err = scanner.Scan(&m)
		require.NoError(t, err)
		assert.Len(t, m, 3)
	})
}

func TestScanner_ScanPrimitive(t *testing.T) {
	dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
	db, err := connect("mysql", dsn)
	require.NoError(t, err)

	t.Run("string", func(t *testing.T) {
		rows, err := db.Query("SELECT username FROM user LIMIT 1")
		require.NoError(t, err)
		scanner := &Scanner{queryRow: true, rows: rows}
		var m string
		err = scanner.Scan(&m)
		require.NoError(t, err)
		assert.Contains(t, m, "abc")
	})

	t.Run("int", func(t *testing.T) {
		rows, err := db.Query("SELECT age FROM user LIMIT 1")
		require.NoError(t, err)
		scanner := &Scanner{queryRow: true, rows: rows}
		var m int
		err = scanner.Scan(&m)
		require.NoError(t, err)
		assert.Equal(t, m, 18)
	})

	t.Run("int8", func(t *testing.T) {
		rows, err := db.Query("SELECT age FROM user LIMIT 1")
		require.NoError(t, err)
		scanner := &Scanner{queryRow: true, rows: rows}
		var m int8
		err = scanner.Scan(&m)
		require.NoError(t, err)
		assert.Equal(t, m, int8(18))
	})

	t.Run("float32", func(t *testing.T) {
		rows, err := db.Query("SELECT value FROM user LIMIT 1")
		require.NoError(t, err)
		scanner := &Scanner{queryRow: true, rows: rows}
		var m float32
		err = scanner.Scan(&m)
		require.NoError(t, err)
		assert.Equal(t, m, float32(3.14))
	})

	t.Run("float64", func(t *testing.T) {
		rows, err := db.Query("SELECT value FROM user LIMIT 1")
		require.NoError(t, err)
		scanner := &Scanner{queryRow: true, rows: rows}
		var m float64
		err = scanner.Scan(&m)
		require.NoError(t, err)
		assert.Equal(t, m, 3.14)
	})

	t.Run("slice string", func(t *testing.T) {
		rows, err := db.Query("SELECT username FROM user")
		require.NoError(t, err)
		scanner := &Scanner{rows: rows}
		var m []string
		err = scanner.Scan(&m)
		require.NoError(t, err)
		assert.Equal(t, len(m), 3)
	})

	t.Run("slice int", func(t *testing.T) {
		rows, err := db.Query("SELECT age FROM user")
		require.NoError(t, err)
		scanner := &Scanner{rows: rows}
		var m []int
		err = scanner.Scan(&m)
		require.NoError(t, err)
		assert.Equal(t, len(m), 3)
	})

	t.Run("slice string pointer", func(t *testing.T) {
		rows, err := db.Query("SELECT username FROM user")
		require.NoError(t, err)
		scanner := &Scanner{rows: rows}
		var m []*string
		err = scanner.Scan(&m)
		require.NoError(t, err)
		assert.Equal(t, len(m), 3)
	})

	t.Run("slice int pointer", func(t *testing.T) {
		rows, err := db.Query("SELECT age FROM user")
		require.NoError(t, err)
		scanner := &Scanner{rows: rows}
		var m []*int
		err = scanner.Scan(&m)
		require.NoError(t, err)
		assert.Equal(t, len(m), 3)
	})
}

func TestScanner_ScanArgs(t *testing.T) {
	scanner := &Scanner{rows: &MockRows{
		ColumnsFunc: func() ([]string, error) {
			return []string{"user"}, nil
		},
	}}

	t.Run("should fail if not a pointer", func(t *testing.T) {
		var m string
		err := scanner.Scan(m)
		require.Error(t, err)
		assert.ErrorContains(t, err, "arg must be a pointer")
	})

	t.Run("should not fail on nil map pointer", func(t *testing.T) {
		var m map[string]any
		err := scanner.Scan(&m)
		require.NoError(t, err)
	})

	t.Run("should not fail on allocated map", func(t *testing.T) {
		m := make(map[string]any)
		err := scanner.Scan(&m)
		require.NoError(t, err)
	})

	t.Run("should not fail on primitive pointer", func(t *testing.T) {
		var m string
		err := scanner.Scan(&m)
		require.NoError(t, err)
	})

	t.Run("should not fail on nil pointer", func(t *testing.T) {
		var m *string
		err := scanner.Scan(&m)
		require.NoError(t, err)
	})

	t.Run("should fail on interface pointer", func(t *testing.T) {
		var m any
		err := scanner.Scan(&m)
		require.Error(t, err)
	})
}

type MockRows struct {
	CloseFunc         func() error
	ColumnTypesFunc   func() ([]*sql.ColumnType, error)
	ColumnsFunc       func() ([]string, error)
	ErrFunc           func() error
	NextFunc          func() bool
	NextResultSetFunc func() bool
	ScanFunc          func(dest ...any) error
}

func (m *MockRows) Close() error {
	if m.CloseFunc == nil {
		return nil
	}
	return m.CloseFunc()
}

func (m *MockRows) ColumnTypes() ([]*sql.ColumnType, error) {
	if m.ColumnTypesFunc == nil {
		return nil, nil
	}
	return m.ColumnTypesFunc()
}

func (m *MockRows) Columns() ([]string, error) {
	if m.ColumnsFunc == nil {
		return nil, nil
	}
	return m.ColumnsFunc()
}

func (m *MockRows) Err() error {
	if m.ErrFunc == nil {
		return nil
	}
	return m.ErrFunc()
}

func (m *MockRows) Next() bool {
	if m.NextFunc == nil {
		return false
	}
	return m.NextFunc()
}

func (m *MockRows) NextResultSet() bool {
	if m.NextResultSetFunc == nil {
		return false
	}
	return m.NextResultSetFunc()
}

func (m *MockRows) Scan(dest ...any) error {
	if m.ScanFunc == nil {
		return nil
	}
	return m.ScanFunc(dest...)
}

func BenchmarkMapScan(b *testing.B) {
	dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
	db, err := connect("mysql", dsn)
	require.NoError(b, err)

	for b.Loop() {
		m := make(map[string]any)
		rows, err := db.Query("SELECT * FROM user LIMIT 1")
		require.NoError(b, err)
		scanner := &Scanner{queryRow: true, rows: rows}
		err = scanner.Scan(&m)
		require.NoError(b, err)
		assert.Equal(b, 4, len(m))
	}
}

func BenchmarkMapSliceScan(b *testing.B) {
	dsn := cmp.Or(os.Getenv("MYSQL_DSN"), testutil.MYSQL_DSN)
	db, err := connect("mysql", dsn)
	require.NoError(b, err)

	for b.Loop() {
		var m []map[string]any
		rows, err := db.Query("SELECT * FROM user")
		require.NoError(b, err)
		scanner := &Scanner{rows: rows}
		err = scanner.Scan(&m)
		require.NoError(b, err)
		assert.Equal(b, 3, len(m))
	}
}
