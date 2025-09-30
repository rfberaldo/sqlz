package sqlz_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rfberaldo/sqlz"
	"github.com/rfberaldo/sqlz/internal/reflectutil"
	"github.com/rfberaldo/sqlz/internal/testutil"
	"github.com/rfberaldo/sqlz/internal/testutil/mock"
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
	return reflectutil.DerefValue(v).Interface()
}

func TestScanner_Scan(t *testing.T) {
	mdb := testutil.NewMultiDB(t)
	ts, _ := time.Parse(time.DateTime, "2025-09-29 12:00:00")

	testCases := []struct {
		name     string
		query    string
		expected any
	}{
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

	mdb.Run(t, func(t *testing.T, db *sql.DB) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				rows, err := db.Query(tc.query)
				require.NoError(t, err)
				scanner, err := sqlz.NewScanner(rows, &sqlz.ScannerOptions{QueryRow: true})
				require.NoError(t, err)
				dst := allocDest(tc.expected)
				scanner.Scan(dst)
				assert.Equal(t, tc.expected, derefDest(dst))
			})
		}
	})
}

func TestScanner_ScanSlices(t *testing.T) {
	mdb := testutil.NewMultiDB(t)

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
					SELECT 'foo val' AS foo, 'bar val' AS bar
					UNION ALL
					SELECT 'foo val 2', 'bar val 2'
					UNION ALL
					SELECT 'foo val 3', 'bar val 3'
				) AS t
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
					SELECT 'foo val' AS foo, 'bar val' AS bar
					UNION ALL
					SELECT 'foo val 2', 'bar val 2'
					UNION ALL
					SELECT 'foo val 3', 'bar val 3'
				) AS t
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
					SELECT 'foo val' AS foo, 'bar val' AS bar
					UNION ALL
					SELECT 'foo val 2', 'bar val 2'
					UNION ALL
					SELECT 'foo val 3', 'bar val 3'
				) AS t
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
					SELECT 'foo val' AS foo
					UNION ALL
					SELECT 'foo val 2'
					UNION ALL
					SELECT 'foo val 3'
				) AS t
			`,
			expected: []string{"foo val", "foo val 2", "foo val 3"},
		},
		{
			name: "slice of *strings",
			query: `
				SELECT *
				FROM (
					SELECT 'foo val' AS foo
					UNION ALL
					SELECT NULL
					UNION ALL
					SELECT 'foo val 3'
				) AS t
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
					SELECT 1 AS foo
					UNION ALL
					SELECT 2
					UNION ALL
					SELECT 3
				) AS t
			`,
			expected: []int{1, 2, 3},
		},
		{
			name: "slice of *ints",
			query: `
				SELECT *
				FROM (
					SELECT 1 AS foo
					UNION ALL
					SELECT NULL
					UNION ALL
					SELECT 3
				) AS t
			`,
			expected: []*int{testutil.PtrTo(1), nil, testutil.PtrTo(3)},
		},
		{
			name: "slice of sql.NullString",
			query: `
				SELECT *
				FROM (
					SELECT 'foo val' AS foo
					UNION ALL
					SELECT 'foo val 2'
					UNION ALL
					SELECT 'foo val 3'
				) AS t
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
					SELECT '{"key1": "foo val 1", "key2": "bar val 1"}' AS foo
					UNION ALL
					SELECT '{"key1": "foo val 2", "key2": "bar val 2"}'
				) AS t
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
					SELECT '{"key1": "foo val 1", "key2": "bar val 1"}' AS foo
					UNION ALL
					SELECT NULL
					UNION ALL
					SELECT '{"key1": "foo val 2", "key2": "bar val 2"}'
				) AS t
			`,
			expected: []*CustomScan{
				{Key1: "foo val 1", Key2: "bar val 1"},
				nil,
				{Key1: "foo val 2", Key2: "bar val 2"},
			},
		},
	}

	mdb.Run(t, func(t *testing.T, db *sql.DB) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				rows, err := db.Query(tc.query)
				require.NoError(t, err)
				scanner, err := sqlz.NewScanner(rows, nil)
				require.NoError(t, err)
				dst := allocDest(tc.expected)
				scanner.Scan(dst)
				assert.Equal(t, tc.expected, derefDest(dst))
			})
		}
	})
}

func TestScanner_ScanStructMissingFields(t *testing.T) {
	mdb := testutil.NewMultiDB(t)
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

	expect := &User{
		Id:       1,
		Name:     "Alice",
		Salary:   69420.42,
		IsActive: true,
	}

	mdb.Run(t, func(t *testing.T, db *sql.DB) {
		t.Run("missing field error", func(t *testing.T) {
			rows, err := db.Query(query)
			require.NoError(t, err)
			scanner, err := sqlz.NewScanner(rows, nil)
			require.NoError(t, err)
			var user User
			err = scanner.Scan(&user)
			require.Error(t, err)
			assert.ErrorContains(t, err, "field not found")
		})

		t.Run("ignore missing fields", func(t *testing.T) {
			rows, err := db.Query(query)
			require.NoError(t, err)
			scanner, err := sqlz.NewScanner(rows, &sqlz.ScannerOptions{IgnoreMissingFields: true})
			require.NoError(t, err)
			var user *User
			err = scanner.Scan(&user)
			require.NoError(t, err)
			assert.Equal(t, expect, user)
		})
	})
}

func TestScanner_ScanStructNested(t *testing.T) {
	mdb := testutil.NewMultiDB(t)
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
		Id         int
		Name       string
		Salary     float64
		Profession *Profession
		IsActive   bool
	}

	expect := User{
		Id:       1,
		Name:     "Alice",
		Salary:   69420.42,
		IsActive: true,
		Profession: &Profession{
			ProfessionId:   1,
			ProfessionName: "Dev",
		},
	}

	mdb.Run(t, func(t *testing.T, db *sql.DB) {
		rows, err := db.Query(query)
		require.NoError(t, err)
		scanner, err := sqlz.NewScanner(rows, &sqlz.ScannerOptions{IgnoreMissingFields: true})
		require.NoError(t, err)
		var user User
		err = scanner.Scan(&user)
		require.NoError(t, err)
		assert.Equal(t, expect, user)
	})
}

func TestScanner_ScanStructEmbed(t *testing.T) {
	mdb := testutil.NewMultiDB(t)
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
		Id     int
		Name   string
		Salary float64
		*Profession
		IsActive bool
	}

	expect := User{
		Id:       1,
		Name:     "Alice",
		Salary:   69420.42,
		IsActive: true,
		Profession: &Profession{
			ProfessionId:   1,
			ProfessionName: "Dev",
		},
	}

	mdb.Run(t, func(t *testing.T, db *sql.DB) {
		rows, err := db.Query(query)
		require.NoError(t, err)
		scanner, err := sqlz.NewScanner(rows, &sqlz.ScannerOptions{IgnoreMissingFields: true})
		require.NoError(t, err)
		var user User
		err = scanner.Scan(&user)
		require.NoError(t, err)
		assert.Equal(t, expect, user)
	})
}

func TestScanner_ScanMap(t *testing.T) {
	mdb := testutil.NewMultiDB(t)
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

	mdb.Run(t, func(t *testing.T, db *sql.DB) {
		t.Run("allocated map", func(t *testing.T) {
			rows, err := db.Query(query)
			require.NoError(t, err)
			scanner, err := sqlz.NewScanner(rows, nil)
			require.NoError(t, err)
			user := make(map[string]any)
			err = scanner.Scan(&user)
			require.NoError(t, err)
			assert.EqualValues(t, expect, user)
		})

		t.Run("non allocated map", func(t *testing.T) {
			rows, err := db.Query(query)
			require.NoError(t, err)
			scanner, err := sqlz.NewScanner(rows, nil)
			require.NoError(t, err)
			var user map[string]any
			err = scanner.Scan(&user)
			require.NoError(t, err)
			assert.EqualValues(t, expect, user)
		})
	})
}

func TestScanner_CheckDest(t *testing.T) {
	scanner, err := sqlz.NewScanner(&mock.Rows{
		ColumnsFunc: func() ([]string, error) {
			return []string{"user"}, nil
		},
	}, nil)
	require.NoError(t, err)

	errAddressable := "destination must be addressable"

	t.Run("no ref to string", func(t *testing.T) {
		var m string
		err := scanner.Scan(m)
		require.Error(t, err)
		assert.ErrorContains(t, err, errAddressable)
	})

	t.Run("no ref to pointer string", func(t *testing.T) {
		var m *string
		err := scanner.Scan(m)
		require.Error(t, err)
		assert.ErrorContains(t, err, errAddressable)
	})

	t.Run("ref to string", func(t *testing.T) {
		var m string
		err := scanner.Scan(&m)
		require.NoError(t, err)
	})

	t.Run("ref to pointer string", func(t *testing.T) {
		var m *string
		err := scanner.Scan(&m)
		require.NoError(t, err)
	})

	t.Run("ref to map", func(t *testing.T) {
		var m map[string]any
		err := scanner.Scan(&m)
		require.NoError(t, err)
	})

	t.Run("no ref to map", func(t *testing.T) {
		var m map[string]any
		err := scanner.Scan(m)
		require.Error(t, err)
		assert.ErrorContains(t, err, errAddressable)
	})

	t.Run("no ref to slice", func(t *testing.T) {
		var s []string
		err := scanner.Scan(s)
		require.Error(t, err)
		assert.ErrorContains(t, err, errAddressable)
	})

	t.Run("ref to slice", func(t *testing.T) {
		var s []string
		err := scanner.Scan(&s)
		require.NoError(t, err)
	})

	t.Run("ref to interface", func(t *testing.T) {
		var m any
		err := scanner.Scan(&m)
		require.Error(t, err)
		assert.ErrorContains(t, err, "unsupported destination type")
	})

	t.Run("no ref to pointer struct", func(t *testing.T) {
		type User struct{}
		var user *User
		err = scanner.Scan(user)
		require.Error(t, err)
		assert.ErrorContains(t, err, errAddressable)
	})

	t.Run("ref to pointer struct", func(t *testing.T) {
		type User struct{}
		var user *User
		err = scanner.Scan(&user)
		require.NoError(t, err)
	})
}
