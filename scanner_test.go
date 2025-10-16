package sqlz

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/rfberaldo/sqlz/internal/parser"
	"github.com/rfberaldo/sqlz/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type CustomScan struct {
	Key1 string
	Key2 string
}

func (cst *CustomScan) Scan(val any) error {
	switch v := val.(type) {
	case []byte:
		return json.Unmarshal(v, cst)
	case string:
		return json.Unmarshal([]byte(v), cst)
	default:
		return fmt.Errorf("unsupported type: %T", v)
	}
}

func allocDest(dest any) any {
	destType := reflect.TypeOf(dest)
	if destType.Kind() == reflect.Map {
		v := reflect.New(destType)
		v.Elem().Set(reflect.MakeMap(destType))
		return v.Interface()
	}
	return reflect.New(destType).Interface()
}

func derefDest(dest any) any {
	v := reflect.ValueOf(dest)
	return reflect.Indirect(v).Interface()
}

func TestScanner_Scan(t *testing.T) {
	testutil.RunConn(t, func(t *testing.T, conn *testutil.Conn) {
		ts, _ := time.Parse(time.DateTime, "2025-09-29 12:00:00")
		testCases := []struct {
			name     string
			query    string
			expected any
		}{
			{
				name:     "string",
				query:    "SELECT 'Alice' AS name",
				expected: "Alice",
			},
			{
				name: "struct",
				query: `
				SELECT
					1         AS id,
					'Alice'   AS name,
					69420.42  AS salary,
					TRUE      AS is_active,
					TIMESTAMP '2025-09-29 12:00:00' AS created_at
			`,
				expected: struct {
					Id        int
					Name      string
					Salary    float64
					IsActive  bool
					CreatedAt time.Time
				}{
					Id:        1,
					Name:      "Alice",
					Salary:    69420.42,
					IsActive:  true,
					CreatedAt: ts,
				},
			},
			{
				name: "struct with pointer fields",
				query: `
				SELECT
					1         AS id,
					'Alice'   AS name,
					69420.42  AS salary,
					TRUE      AS is_active,
					TIMESTAMP '2025-09-29 12:00:00' AS created_at
			`,
				expected: struct {
					Id        *int
					Name      *string
					Salary    *float64
					IsActive  *bool
					CreatedAt *time.Time
				}{
					Id:        testutil.PtrTo(1),
					Name:      testutil.PtrTo("Alice"),
					Salary:    testutil.PtrTo(69420.42),
					IsActive:  testutil.PtrTo(true),
					CreatedAt: testutil.PtrTo(ts),
				},
			},
			{
				name: "struct with sql.NullX fields",
				query: `
				SELECT
					1         AS id,
					'Alice'   AS name,
					69420.42  AS salary,
					TRUE      AS is_active,
					TIMESTAMP '2025-09-29 12:00:00' AS created_at
			`,
				expected: struct {
					Id        sql.NullInt64
					Name      sql.NullString
					Salary    sql.NullFloat64
					IsActive  sql.NullBool
					CreatedAt sql.NullTime
				}{
					Id:        sql.NullInt64{Int64: 1, Valid: true},
					Name:      sql.NullString{String: "Alice", Valid: true},
					Salary:    sql.NullFloat64{Float64: 69420.42, Valid: true},
					IsActive:  sql.NullBool{Bool: true, Valid: true},
					CreatedAt: sql.NullTime{Time: ts, Valid: true},
				},
			},
			{
				name: "map",
				query: `
				SELECT
					1            AS id,
					'Alice'      AS name,
					69420.42      AS salary,
					TIMESTAMP '2025-09-29 12:00:00' AS created_at
			`,
				expected: map[string]any{
					"id":         int64(1),
					"name":       "Alice",
					"salary":     "69420.42",
					"created_at": ts,
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				rows, err := conn.DB.Query(tc.query)
				require.NoError(t, err)
				scanner := newRowScanner(rows, nil)
				dst := allocDest(tc.expected)
				err = scanner.Scan(dst)
				require.NoError(t, err)
				assert.Equal(t, tc.expected, derefDest(dst))
			})
		}
	})
}

func TestScanner_Scan_slices(t *testing.T) {
	testutil.RunConn(t, func(t *testing.T, conn *testutil.Conn) {
		testCases := []struct {
			name     string
			query    string
			expected any
		}{
			{
				name: "slice of structs",
				query: `
				SELECT *
				FROM (
					SELECT 'foo val', 'bar val'
					UNION ALL
					SELECT 'foo val 2', 'bar val 2'
					UNION ALL
					SELECT 'foo val 3', 'bar val 3'
				) AS t (foo, bar)
			`,
				expected: []struct {
					Foo string
					Bar string
				}{
					{Foo: "foo val", Bar: "bar val"},
					{Foo: "foo val 2", Bar: "bar val 2"},
					{Foo: "foo val 3", Bar: "bar val 3"},
				},
			},
			{
				name: "slice of structs by ptr",
				query: `
				SELECT *
				FROM (
					SELECT 'foo val', 'bar val'
					UNION ALL
					SELECT 'foo val 2', 'bar val 2'
					UNION ALL
					SELECT 'foo val 3', 'bar val 3'
				) AS t (foo, bar)
			`,
				expected: []*struct {
					Foo string
					Bar string
				}{
					{Foo: "foo val", Bar: "bar val"},
					{Foo: "foo val 2", Bar: "bar val 2"},
					{Foo: "foo val 3", Bar: "bar val 3"},
				},
			},
			{
				name: "slice of maps",
				query: `
				SELECT *
				FROM (
					SELECT 'foo val', 'bar val'
					UNION ALL
					SELECT 'foo val 2', 'bar val 2'
					UNION ALL
					SELECT 'foo val 3', 'bar val 3'
				) AS t (foo, bar)
			`,
				expected: []map[string]any{
					{"foo": "foo val", "bar": "bar val"},
					{"foo": "foo val 2", "bar": "bar val 2"},
					{"foo": "foo val 3", "bar": "bar val 3"},
				},
			},
			{
				name: "slice of strings",
				query: `
				SELECT *
				FROM (
					SELECT 'foo val'
					UNION ALL
					SELECT 'foo val 2'
					UNION ALL
					SELECT 'foo val 3'
				) AS t (foo)
			`,
				expected: []string{"foo val", "foo val 2", "foo val 3"},
			},
			{
				name: "slice of *strings",
				query: `
				SELECT *
				FROM (
					SELECT 'foo val'
					UNION ALL
					SELECT NULL
					UNION ALL
					SELECT 'foo val 3'
				) AS t (foo)
			`,
				expected: []*string{
					testutil.PtrTo("foo val"),
					nil,
					testutil.PtrTo("foo val 3"),
				},
			},
			{
				name: "slice of ints",
				query: `
				SELECT *
				FROM (
					SELECT 1
					UNION ALL
					SELECT 2
					UNION ALL
					SELECT 3
				) AS t (foo)
			`,
				expected: []int{1, 2, 3},
			},
			{
				name: "slice of *ints",
				query: `
				SELECT *
				FROM (
					SELECT 1
					UNION ALL
					SELECT NULL
					UNION ALL
					SELECT 3
				) AS t (foo)
			`,
				expected: []*int{testutil.PtrTo(1), nil, testutil.PtrTo(3)},
			},
			{
				name: "slice of sql.NullString",
				query: `
				SELECT *
				FROM (
					SELECT 'foo val'
					UNION ALL
					SELECT 'foo val 2'
					UNION ALL
					SELECT 'foo val 3'
				) AS t (foo)
			`,
				expected: []sql.NullString{
					{String: "foo val", Valid: true},
					{String: "foo val 2", Valid: true},
					{String: "foo val 3", Valid: true},
				},
			},
			{
				name: "slice of CustomScan",
				query: `
				SELECT *
				FROM (
					SELECT '{"key1": "foo val 1", "key2": "bar val 1"}'
					UNION ALL
					SELECT '{"key1": "foo val 2", "key2": "bar val 2"}'
				) AS t (foo)
			`,
				expected: []CustomScan{
					{Key1: "foo val 1", Key2: "bar val 1"},
					{Key1: "foo val 2", Key2: "bar val 2"},
				},
			},
			{
				name: "slice of *CustomScan",
				query: `
				SELECT *
				FROM (
					SELECT '{"key1": "foo val 1", "key2": "bar val 1"}'
					UNION ALL
					SELECT NULL
					UNION ALL
					SELECT '{"key1": "foo val 2", "key2": "bar val 2"}'
				) AS t (foo)
			`,
				expected: []*CustomScan{
					{Key1: "foo val 1", Key2: "bar val 1"},
					nil,
					{Key1: "foo val 2", Key2: "bar val 2"},
				},
			},
		}

		for _, tc := range testCases {
			t.Run("Scan "+tc.name, func(t *testing.T) {
				rows, err := conn.DB.Query(tc.query)
				require.NoError(t, err)
				scanner := newScanner(rows, nil)
				dst := allocDest(tc.expected)
				err = scanner.Scan(dst)
				require.NoError(t, err)
				assert.Equal(t, tc.expected, derefDest(dst))
			})

			t.Run("ScanRow "+tc.name, func(t *testing.T) {
				rows, err := conn.DB.Query(tc.query)
				require.NoError(t, err)
				scanner := newScanner(rows, nil)
				dst := allocDest(tc.expected)

				defer scanner.Close()
				for scanner.NextRow() {
					err = scanner.ScanRow(dst)
					require.NoError(t, err)
				}
				require.NoError(t, scanner.Err())

				assert.Equal(t, tc.expected, derefDest(dst))
			})
		}
	})
}

func TestScanner_Scan_no_rows(t *testing.T) {
	testutil.RunConn(t, func(t *testing.T, conn *testutil.Conn) {
		query := `SELECT NULL LIMIT 0`

		t.Run("queryRow=false do not return error", func(t *testing.T) {
			rows, err := conn.DB.Query(query)
			require.NoError(t, err)
			scanner := newScanner(rows, nil)
			var tmp []string
			err = scanner.Scan(&tmp)
			require.NoError(t, err)
		})

		t.Run("queryRow=true return error", func(t *testing.T) {
			rows, err := conn.DB.Query(query)
			require.NoError(t, err)
			scanner := newRowScanner(rows, nil)
			var tmp string
			err = scanner.Scan(&tmp)
			require.Error(t, err)
			require.ErrorIs(t, err, sql.ErrNoRows)
		})
	})
}

func TestScanner_Scan_multiple_rows(t *testing.T) {
	testutil.RunConn(t, func(t *testing.T, conn *testutil.Conn) {
		query := `
			SELECT *
			FROM (
				SELECT 'val1'
				UNION ALL
				SELECT 'val2'
			) AS t (foo)`

		t.Run("queryRow=false do not return error", func(t *testing.T) {
			rows, err := conn.DB.Query(query)
			require.NoError(t, err)
			scanner := newScanner(rows, nil)
			var tmp []string
			err = scanner.Scan(&tmp)
			require.NoError(t, err)
		})

		t.Run("queryRow=true return error", func(t *testing.T) {
			rows, err := conn.DB.Query(query)
			require.NoError(t, err)
			scanner := newRowScanner(rows, nil)
			var tmp string
			err = scanner.Scan(&tmp)
			require.Error(t, err)
			require.ErrorContains(t, err, "expected one row")
		})
	})
}

func TestScanner_Scan_struct_missing_fields(t *testing.T) {
	testutil.RunConn(t, func(t *testing.T, conn *testutil.Conn) {
		query := `
		SELECT
			1         AS id,
			'Alice'   AS name,
			'alice'   AS username,
			69420.42  AS salary,
			TRUE      AS is_active`

		type User struct {
			Id       int
			Name     string
			Salary   float64
			IsActive bool
		}

		t.Run("missing field error", func(t *testing.T) {
			rows, err := conn.DB.Query(query)
			require.NoError(t, err)
			scanner := newRowScanner(rows, nil)
			var user User
			err = scanner.Scan(&user)
			require.Error(t, err)
			assert.ErrorContains(t, err, "field not found")
		})

		t.Run("ignore missing fields", func(t *testing.T) {
			expect := &User{
				Id:       1,
				Name:     "Alice",
				Salary:   69420.42,
				IsActive: true,
			}

			rows, err := conn.DB.Query(query)
			require.NoError(t, err)
			scanner := newRowScanner(rows, &config{ignoreMissingFields: true})
			var user *User
			err = scanner.Scan(&user)
			require.NoError(t, err)
			assert.Equal(t, expect, user)
		})
	})
}

func TestScanner_Scan_struct_nested(t *testing.T) {
	testutil.RunConn(t, func(t *testing.T, conn *testutil.Conn) {
		query := `
		SELECT
			1         AS id,
			'Alice'   AS name,
			'alice'   AS username,
			69420.42  AS salary,
			TRUE      AS is_active,
			1         AS profession_id,
			'Dev'     AS profession_name`

		type Profession struct {
			Id   int
			Name string
		}

		type User struct {
			Id         int
			Name       string
			Username   string
			Salary     float64
			Profession *Profession
			IsActive   bool
		}

		expect := User{
			Id:       1,
			Name:     "Alice",
			Username: "alice",
			Salary:   69420.42,
			IsActive: true,
			Profession: &Profession{
				Id:   1,
				Name: "Dev",
			},
		}

		rows, err := conn.DB.Query(query)
		require.NoError(t, err)
		scanner := newRowScanner(rows, nil)
		var user User
		err = scanner.Scan(&user)
		require.NoError(t, err)
		assert.Equal(t, expect, user)
	})
}

func TestScanner_Scan_struct_embed(t *testing.T) {
	testutil.RunConn(t, func(t *testing.T, conn *testutil.Conn) {
		query := `
		SELECT
			1         AS id,
			'Alice'   AS name,
			'alice'   AS username,
			69420.42  AS salary,
			TRUE      AS is_active,
			1         AS profession_id,
			'Dev'     AS profession_name`

		type Profession struct {
			ProfessionId   int
			ProfessionName string
		}

		type User struct {
			Id       int
			Name     string
			Username string
			Salary   float64
			*Profession
			IsActive bool
		}

		expect := User{
			Id:       1,
			Name:     "Alice",
			Username: "alice",
			Salary:   69420.42,
			IsActive: true,
			Profession: &Profession{
				ProfessionId:   1,
				ProfessionName: "Dev",
			},
		}

		rows, err := conn.DB.Query(query)
		require.NoError(t, err)
		scanner := newRowScanner(rows, nil)
		var user User
		err = scanner.Scan(&user)
		require.NoError(t, err)
		assert.Equal(t, expect, user)
	})
}

func TestScanner_Scan_map(t *testing.T) {
	testutil.RunConn(t, func(t *testing.T, conn *testutil.Conn) {
		query := `
		SELECT
			99         AS id,
			'Alice'   AS name,
			69420.42  AS salary`

		expect := map[string]any{
			"id":     int64(99),
			"name":   "Alice",
			"salary": "69420.42",
		}

		t.Run("allocated map", func(t *testing.T) {
			rows, err := conn.DB.Query(query)
			require.NoError(t, err)
			scanner := newRowScanner(rows, nil)
			user := make(map[string]any)
			err = scanner.Scan(&user)
			require.NoError(t, err)
			assert.EqualValues(t, expect, user)
		})

		t.Run("non allocated map", func(t *testing.T) {
			rows, err := conn.DB.Query(query)
			require.NoError(t, err)
			scanner := newRowScanner(rows, nil)
			var user map[string]any
			err = scanner.Scan(&user)
			require.NoError(t, err)
			assert.EqualValues(t, expect, user)
		})
	})
}

type mockRows struct {
	CloseFunc   func() error
	ColumnsFunc func() ([]string, error)
	ErrFunc     func() error
	NextFunc    func() bool
	ScanFunc    func(dest ...any) error
}

func (m *mockRows) Close() error {
	if m.CloseFunc == nil {
		return nil
	}
	return m.CloseFunc()
}

func (m *mockRows) Columns() ([]string, error) {
	if m.ColumnsFunc == nil {
		return nil, nil
	}
	return m.ColumnsFunc()
}

func (m *mockRows) Err() error {
	if m.ErrFunc == nil {
		return nil
	}
	return m.ErrFunc()
}

func (m *mockRows) Next() bool {
	if m.NextFunc == nil {
		return false
	}
	return m.NextFunc()
}

func (m *mockRows) Scan(dest ...any) error {
	if m.ScanFunc == nil {
		return nil
	}
	return m.ScanFunc(dest...)
}

func TestScanner_Scan_validate_dest(t *testing.T) {
	newRows := func() *mockRows {
		count := 0
		return &mockRows{
			ColumnsFunc: func() ([]string, error) {
				return []string{"user"}, nil
			},
			NextFunc: func() bool {
				if count > 0 {
					return false
				}
				count++
				return true
			},
			ScanFunc: func(dest ...any) error {
				for i := range dest {
					dest[i] = nil
				}
				return nil
			},
		}
	}

	errAddressable := "destination must be addressable"

	t.Run("no ref to string", func(t *testing.T) {
		scanner := newRowScanner(newRows(), nil)
		var m string
		err := scanner.Scan(m)
		require.Error(t, err)
		assert.ErrorContains(t, err, errAddressable)
	})

	t.Run("no ref to pointer string", func(t *testing.T) {
		scanner := newRowScanner(newRows(), nil)
		var m *string
		err := scanner.Scan(m)
		require.Error(t, err)
		assert.ErrorContains(t, err, errAddressable)
	})

	t.Run("ref to string", func(t *testing.T) {
		scanner := newRowScanner(newRows(), nil)
		var m string
		err := scanner.Scan(&m)
		require.NoError(t, err)
	})

	t.Run("ref to pointer string", func(t *testing.T) {
		scanner := newRowScanner(newRows(), nil)
		var m *string
		err := scanner.Scan(&m)
		require.NoError(t, err)
	})

	t.Run("ref to map", func(t *testing.T) {
		scanner := newRowScanner(newRows(), nil)
		var m map[string]any
		err := scanner.Scan(&m)
		require.NoError(t, err)
	})

	t.Run("no ref to map", func(t *testing.T) {
		scanner := newRowScanner(newRows(), nil)
		var m map[string]any
		err := scanner.Scan(m)
		require.Error(t, err)
		assert.ErrorContains(t, err, errAddressable)
	})

	t.Run("no ref to slice", func(t *testing.T) {
		scanner := newScanner(newRows(), nil)
		var s []string
		err := scanner.Scan(s)
		require.Error(t, err)
		assert.ErrorContains(t, err, errAddressable)
	})

	t.Run("ref to slice", func(t *testing.T) {
		scanner := newScanner(newRows(), nil)
		var s []string
		err := scanner.Scan(&s)
		require.NoError(t, err)
	})

	t.Run("ref to interface", func(t *testing.T) {
		scanner := newRowScanner(newRows(), nil)
		var m any
		err := scanner.Scan(&m)
		require.NoError(t, err)
	})

	t.Run("no ref to pointer struct", func(t *testing.T) {
		scanner := newRowScanner(newRows(), nil)
		type User struct{}
		var user *User
		err := scanner.Scan(user)
		require.Error(t, err)
		assert.ErrorContains(t, err, errAddressable)
	})

	t.Run("ref to pointer struct", func(t *testing.T) {
		scanner := newRowScanner(newRows(), &config{ignoreMissingFields: true})
		type User struct{}
		var user *User
		err := scanner.Scan(&user)
		require.NoError(t, err)
	})
}

func TestScanner_resolveDestType(t *testing.T) {
	t.Run("unsupported destination", func(t *testing.T) {
		scanner := newScanner(&mockRows{}, nil)
		err := scanner.resolveDestType(new([1]string))
		require.Error(t, err)
		assert.ErrorContains(t, err, "unsupported destination")
	})

	t.Run("must be slice", func(t *testing.T) {
		scanner := newScanner(&mockRows{}, nil)
		err := scanner.resolveDestType(new(string))
		require.Error(t, err)
		assert.ErrorContains(t, err, "destination must be a slice")
	})

	t.Run("primitive expects 1 column", func(t *testing.T) {
		scanner := newScanner(&mockRows{
			ColumnsFunc: func() ([]string, error) {
				return []string{"col1", "col2"}, nil
			},
		}, nil)
		scanner.queryRow = true
		err := scanner.resolveDestType(new(string))
		require.Error(t, err)
		assert.ErrorContains(t, err, "query must return 1 column")
	})

	t.Run("primitive slice expects 1 column", func(t *testing.T) {
		scanner := newScanner(&mockRows{
			ColumnsFunc: func() ([]string, error) {
				return []string{"col1", "col2"}, nil
			},
		}, nil)
		scanner.queryRow = true
		err := scanner.resolveDestType(new([]string))
		require.Error(t, err)
		assert.ErrorContains(t, err, "query must return 1 column")
	})
}

func TestScanner_resolveColumns(t *testing.T) {
	t.Run("columns error", func(t *testing.T) {
		scanner := newScanner(&mockRows{
			ColumnsFunc: func() ([]string, error) {
				return nil, assert.AnError
			},
		}, nil)
		err := scanner.resolveColumns()
		require.Error(t, err)
		assert.ErrorContains(t, err, "getting column names")
	})

	t.Run("no rows", func(t *testing.T) {
		scanner := newScanner(&mockRows{
			ColumnsFunc: func() ([]string, error) {
				return []string{}, nil
			},
		}, nil)
		err := scanner.resolveColumns()
		require.Error(t, err)
		assert.ErrorContains(t, err, "no columns")
	})

	t.Run("duplicate columns", func(t *testing.T) {
		scanner := newScanner(&mockRows{
			ColumnsFunc: func() ([]string, error) {
				return []string{"user", "user"}, nil
			},
		}, nil)
		err := scanner.resolveColumns()
		require.Error(t, err)
		assert.ErrorContains(t, err, "duplicate column")
	})
}

func setupTestTable(t testing.TB, db *sql.DB) *testutil.TableHelper {
	th := testutil.NewTableHelper(t, db, parser.BindQuestion)
	query := th.Fmt(`
		CREATE TABLE IF NOT EXISTS %s (
			id int auto_increment NOT NULL,
			name varchar(100) NULL,
			age int NULL,
			username varchar(100) NOT NULL,
			created_at datetime NOT NULL,
			PRIMARY KEY (id)
		)`)

	_, err := db.Exec(query)
	require.NoError(t, err)

	for range 1000 {
		_, err = db.Exec(
			th.Fmt("INSERT INTO %s (name, age, username, created_at) VALUES (?,?,?,?)"),
			"Bob D", 42, "bob", time.Now(),
		)
		require.NoError(t, err)
	}

	return th
}

// BenchmarkScan_MapSlice-12    	    1256	    962938 ns/op	  537083 B/op	   13784 allocs/op
func BenchmarkScan_MapSlice(b *testing.B) {
	conn := testutil.GetMySQL(b)
	require.NotNil(b, conn.DB)
	th := setupTestTable(b, conn.DB)

	for b.Loop() {
		var m []map[string]any
		rows, err := conn.DB.Query(th.Fmt("SELECT * FROM %s"))
		require.NoError(b, err)
		scanner := newScanner(rows, nil)
		err = scanner.Scan(&m)
		require.NoError(b, err)
		assert.Equal(b, 1000, len(m))
	}
}

// BenchmarkScan_StructSlice-12    	    1144	   1023294 ns/op	  265537 B/op	    8709 allocs/op
func BenchmarkScan_StructSlice(b *testing.B) {
	conn := testutil.GetMySQL(b)
	require.NotNil(b, conn.DB)
	th := setupTestTable(b, conn.DB)

	type User struct {
		Id        int
		Name      *string
		Age       *int
		Username  string
		CreatedAt time.Time
	}

	for b.Loop() {
		var users []User
		rows, err := conn.DB.Query(th.Fmt("SELECT * FROM %s"))
		require.NoError(b, err)
		scanner := newScanner(rows, nil)
		err = scanner.Scan(&users)
		require.NoError(b, err)
		assert.Equal(b, 1000, len(users))
	}
}

// BenchmarkScan_StructSlice_manual-12    	    1017	   1216979 ns/op	  265769 B/op	    8710 allocs/op
func BenchmarkScan_StructSlice_manual(b *testing.B) {
	conn := testutil.GetMySQL(b)
	require.NotNil(b, conn.DB)
	th := setupTestTable(b, conn.DB)

	type User struct {
		Id        int
		Name      *string
		Age       *int
		Username  string
		CreatedAt time.Time
	}

	for b.Loop() {
		rows, err := conn.DB.Query(th.Fmt("SELECT * FROM %s"))
		require.NoError(b, err)
		scanner := newScanner(rows, nil)

		var users []User
		var user User
		defer scanner.Close()
		for scanner.NextRow() {
			err = scanner.ScanRow(&user)
			require.NoError(b, err)
			users = append(users, user)
		}
		require.NoError(b, scanner.Err())
		assert.Equal(b, 1000, len(users))
	}
}

// BenchmarkScan_Primitivelice-12    	    3118	    367826 ns/op	   65298 B/op	    2033 allocs/op
func BenchmarkScan_Primitivelice(b *testing.B) {
	conn := testutil.GetMySQL(b)
	require.NotNil(b, conn.DB)
	th := setupTestTable(b, conn.DB)

	for b.Loop() {
		var names []string
		rows, err := conn.DB.Query(th.Fmt("SELECT name FROM %s"))
		require.NoError(b, err)
		scanner := newScanner(rows, nil)
		err = scanner.Scan(&names)
		require.NoError(b, err)
		assert.Equal(b, 1000, len(names))
	}
}

// BenchmarkScan_Struct-12    	    9987	    113177 ns/op	    2354 B/op	      58 allocs/op
func BenchmarkScan_Struct(b *testing.B) {
	conn := testutil.GetMySQL(b)
	require.NotNil(b, conn.DB)
	th := setupTestTable(b, conn.DB)

	type User struct {
		Id        int
		Name      *string
		Age       *int
		Username  string
		CreatedAt time.Time
	}

	for b.Loop() {
		var user User
		rows, err := conn.DB.Query(th.Fmt("SELECT * FROM %s LIMIT 1"))
		require.NoError(b, err)
		scanner := newRowScanner(rows, nil)
		err = scanner.Scan(&user)
		require.NoError(b, err)
	}
}

// BenchmarkScan_Map-12    	   10000	    111461 ns/op	    1769 B/op	      40 allocs/op
func BenchmarkScan_Map(b *testing.B) {
	conn := testutil.GetMySQL(b)
	require.NotNil(b, conn.DB)
	th := setupTestTable(b, conn.DB)

	for b.Loop() {
		m := make(map[string]any)
		rows, err := conn.DB.Query(th.Fmt("SELECT * FROM %s LIMIT 1"))
		require.NoError(b, err)
		scanner := newRowScanner(rows, nil)
		err = scanner.Scan(&m)
		require.NoError(b, err)
	}
}
