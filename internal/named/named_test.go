package named

import (
	"testing"

	"github.com/rfberaldo/sqlz/binds"
	"github.com/rfberaldo/sqlz/internal/testutil"
	"github.com/stretchr/testify/assert"
)

func TestNamed(t *testing.T) {
	type basicStruct struct {
		Identifier int    `db:"id"`
		FullName   string `db:"name"`
	}

	type basicStructJsonTag struct {
		Identifier int    `json:"id"`
		FullName   string `json:"name"`
	}

	type address struct {
		City string `db:"city"`
	}

	type nestedStruct struct {
		ID      int     `db:"id"`
		Name    string  `db:"name"`
		Address address `db:"address"`
	}

	type nestedStructNoTag struct {
		ID      int
		Name    string
		Address address
	}

	type nestedStructSameSubField struct {
		ID       int     `db:"id"`
		Name     string  `db:"name"`
		Address  address `db:"address"`
		Address2 address `db:"address2"`
	}

	type nestedStructWithPointers struct {
		ID      int `db:"id"`
		Name    *string
		Address *address `db:"address"`
	}

	tests := []struct {
		name             string
		inputQuery       string
		inputArg         any
		structTag        string
		expectedAt       string
		expectedColon    string
		expectedDollar   string
		expectedQuestion string
		expectedArgs     []any
		expectError      bool
	}{
		{
			name:             "map with named parameters",
			inputQuery:       "SELECT * FROM user WHERE id = :id AND name = :name",
			inputArg:         map[string]any{"id": 1, "name": "Alice"},
			expectedAt:       "SELECT * FROM user WHERE id = @p1 AND name = @p2",
			expectedColon:    "SELECT * FROM user WHERE id = :id AND name = :name",
			expectedDollar:   "SELECT * FROM user WHERE id = $1 AND name = $2",
			expectedQuestion: "SELECT * FROM user WHERE id = ? AND name = ?",
			expectedArgs:     []any{1, "Alice"},
			expectError:      false,
		},
		{
			name:             "insert query",
			inputQuery:       `INSERT INTO user (id, username, email, password, age) VALUES (:id, :username, :email, :password, :age)`,
			inputArg:         map[string]any{"id": 1, "username": "user123", "email": "user@example.com", "password": "abc123", "age": 18},
			expectedAt:       `INSERT INTO user (id, username, email, password, age) VALUES (@p1, @p2, @p3, @p4, @p5)`,
			expectedColon:    `INSERT INTO user (id, username, email, password, age) VALUES (:id, :username, :email, :password, :age)`,
			expectedDollar:   `INSERT INTO user (id, username, email, password, age) VALUES ($1, $2, $3, $4, $5)`,
			expectedQuestion: `INSERT INTO user (id, username, email, password, age) VALUES (?, ?, ?, ?, ?)`,
			expectedArgs:     []any{1, "user123", "user@example.com", "abc123", 18},
			expectError:      false,
		},
		{
			name:       "insert query with array of map",
			inputQuery: `INSERT INTO user (id, username, email, password, age) VALUES (:id, :username, :email, :password, :age);`,
			inputArg: [2]map[string]any{
				{"id": 1, "username": "user123", "email": "user@example.com", "password": "abc123", "age": 18},
				{"id": 2, "username": "user456", "email": "user2@example.com", "password": "abc456", "age": 19},
			},
			expectedAt:       `INSERT INTO user (id, username, email, password, age) VALUES (@p1, @p2, @p3, @p4, @p5),(@p6, @p7, @p8, @p9, @p10)`,
			expectedColon:    `INSERT INTO user (id, username, email, password, age) VALUES (:id, :username, :email, :password, :age),(:id, :username, :email, :password, :age)`,
			expectedDollar:   `INSERT INTO user (id, username, email, password, age) VALUES ($1, $2, $3, $4, $5),($6, $7, $8, $9, $10)`,
			expectedQuestion: `INSERT INTO user (id, username, email, password, age) VALUES (?, ?, ?, ?, ?),(?, ?, ?, ?, ?)`,
			expectedArgs:     []any{1, "user123", "user@example.com", "abc123", 18, 2, "user456", "user2@example.com", "abc456", 19},
			expectError:      false,
		},
		{
			name:             "struct with named parameters",
			inputQuery:       "SELECT * FROM user WHERE id = :id AND name = :name",
			inputArg:         basicStruct{Identifier: 1, FullName: "Alice"},
			structTag:        "db",
			expectedAt:       "SELECT * FROM user WHERE id = @p1 AND name = @p2",
			expectedColon:    "SELECT * FROM user WHERE id = :id AND name = :name",
			expectedDollar:   "SELECT * FROM user WHERE id = $1 AND name = $2",
			expectedQuestion: "SELECT * FROM user WHERE id = ? AND name = ?",
			expectedArgs:     []any{1, "Alice"},
			expectError:      false,
		},
		{
			name:             "struct with named parameters and custom struct tag",
			inputQuery:       "SELECT * FROM user WHERE id = :id AND name = :name",
			inputArg:         basicStructJsonTag{Identifier: 1, FullName: "Alice"},
			structTag:        "json",
			expectedAt:       "SELECT * FROM user WHERE id = @p1 AND name = @p2",
			expectedColon:    "SELECT * FROM user WHERE id = :id AND name = :name",
			expectedDollar:   "SELECT * FROM user WHERE id = $1 AND name = $2",
			expectedQuestion: "SELECT * FROM user WHERE id = ? AND name = ?",
			expectedArgs:     []any{1, "Alice"},
			expectError:      false,
		},
		{
			name:       "nested struct with named parameters",
			inputQuery: "SELECT * FROM user WHERE id = :id AND name = :name AND address.city = :address.city",
			inputArg: nestedStruct{
				ID: 1, Name: "Alice", Address: address{City: "Wonderland"},
			},
			structTag:        "db",
			expectedAt:       "SELECT * FROM user WHERE id = @p1 AND name = @p2 AND address.city = @p3",
			expectedColon:    "SELECT * FROM user WHERE id = :id AND name = :name AND address.city = :address.city",
			expectedDollar:   "SELECT * FROM user WHERE id = $1 AND name = $2 AND address.city = $3",
			expectedQuestion: "SELECT * FROM user WHERE id = ? AND name = ? AND address.city = ?",
			expectedArgs:     []any{1, "Alice", "Wonderland"},
			expectError:      false,
		},
		{
			name:       "nested struct with same sub-parameter name",
			inputQuery: "SELECT * FROM user WHERE id = :id AND name = :name AND address.city = :address.city OR address2.city = :address2.city",
			inputArg: nestedStructSameSubField{
				ID: 1, Name: "Alice",
				Address:  address{City: "Wonderland"},
				Address2: address{City: "Not Wonderland"},
			},
			structTag:        "db",
			expectedAt:       "SELECT * FROM user WHERE id = @p1 AND name = @p2 AND address.city = @p3 OR address2.city = @p4",
			expectedColon:    "SELECT * FROM user WHERE id = :id AND name = :name AND address.city = :address.city OR address2.city = :address2.city",
			expectedDollar:   "SELECT * FROM user WHERE id = $1 AND name = $2 AND address.city = $3 OR address2.city = $4",
			expectedQuestion: "SELECT * FROM user WHERE id = ? AND name = ? AND address.city = ? OR address2.city = ?",
			expectedArgs:     []any{1, "Alice", "Wonderland", "Not Wonderland"},
			expectError:      false,
		},
		{
			name:             "nested struct without db tag",
			inputQuery:       "SELECT * FROM user WHERE id = :id AND name = :name AND address.city = :address.city",
			inputArg:         nestedStructNoTag{ID: 1, Name: "Alice", Address: address{City: "Wonderland"}},
			expectedAt:       "SELECT * FROM user WHERE id = @p1 AND name = @p2 AND address.city = @p3",
			expectedColon:    "SELECT * FROM user WHERE id = :id AND name = :name AND address.city = :address.city",
			expectedDollar:   "SELECT * FROM user WHERE id = $1 AND name = $2 AND address.city = $3",
			expectedQuestion: "SELECT * FROM user WHERE id = ? AND name = ? AND address.city = ?",
			expectedArgs:     []any{1, "Alice", "Wonderland"},
			expectError:      false,
		},
		{
			name:             "nested struct with field pointers",
			inputQuery:       "SELECT * FROM user WHERE id = :id AND name = :name AND address.city = :address.city",
			inputArg:         nestedStructWithPointers{ID: 1, Name: testutil.PtrTo("Alice"), Address: &address{City: "Wonderland"}},
			structTag:        "db",
			expectedAt:       "SELECT * FROM user WHERE id = @p1 AND name = @p2 AND address.city = @p3",
			expectedColon:    "SELECT * FROM user WHERE id = :id AND name = :name AND address.city = :address.city",
			expectedDollar:   "SELECT * FROM user WHERE id = $1 AND name = $2 AND address.city = $3",
			expectedQuestion: "SELECT * FROM user WHERE id = ? AND name = ? AND address.city = ?",
			expectedArgs:     []any{1, "Alice", "Wonderland"},
			expectError:      false,
		},
		{
			name:             "nested nil struct",
			inputQuery:       "SELECT * FROM user WHERE id = :id AND name = :name AND address.city = :address",
			inputArg:         nestedStructWithPointers{ID: 1, Name: testutil.PtrTo("Alice")},
			structTag:        "db",
			expectedAt:       "SELECT * FROM user WHERE id = @p1 AND name = @p2 AND address.city = @p3",
			expectedColon:    "SELECT * FROM user WHERE id = :id AND name = :name AND address.city = :address",
			expectedDollar:   "SELECT * FROM user WHERE id = $1 AND name = $2 AND address.city = $3",
			expectedQuestion: "SELECT * FROM user WHERE id = ? AND name = ? AND address.city = ?",
			expectedArgs:     []any{1, "Alice", nil},
			expectError:      false,
		},
		{
			name:             "nested map with named parameters",
			inputQuery:       "SELECT * FROM user WHERE id = :id AND name = :name AND address.city = :address.city",
			inputArg:         map[string]any{"id": 1, "name": "Alice", "address": map[string]any{"city": "Wonderland"}},
			expectedAt:       "SELECT * FROM user WHERE id = @p1 AND name = @p2 AND address.city = @p3",
			expectedColon:    "SELECT * FROM user WHERE id = :id AND name = :name AND address.city = :address.city",
			expectedDollar:   "SELECT * FROM user WHERE id = $1 AND name = $2 AND address.city = $3",
			expectedQuestion: "SELECT * FROM user WHERE id = ? AND name = ? AND address.city = ?",
			expectedArgs:     []any{1, "Alice", "Wonderland"},
			expectError:      false,
		},
		{
			name:             "map slice with named parameters",
			inputQuery:       "INSERT INTO users (id, name) VALUES (:id, :name)",
			inputArg:         []map[string]any{{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}},
			expectedAt:       "INSERT INTO users (id, name) VALUES (@p1, @p2),(@p3, @p4)",
			expectedColon:    "INSERT INTO users (id, name) VALUES (:id, :name),(:id, :name)",
			expectedDollar:   "INSERT INTO users (id, name) VALUES ($1, $2),($3, $4)",
			expectedQuestion: "INSERT INTO users (id, name) VALUES (?, ?),(?, ?)",
			expectedArgs:     []any{1, "Alice", 2, "Bob"},
			expectError:      false,
		},
		{
			name:       "struct slice with named parameters",
			inputQuery: "INSERT INTO users (id, name) VALUES (:id, :name)",
			inputArg: []basicStruct{
				{Identifier: 1, FullName: "Alice"},
				{Identifier: 2, FullName: "Bob"},
			},
			structTag:        "db",
			expectedAt:       "INSERT INTO users (id, name) VALUES (@p1, @p2),(@p3, @p4)",
			expectedColon:    "INSERT INTO users (id, name) VALUES (:id, :name),(:id, :name)",
			expectedDollar:   "INSERT INTO users (id, name) VALUES ($1, $2),($3, $4)",
			expectedQuestion: "INSERT INTO users (id, name) VALUES (?, ?),(?, ?)",
			expectedArgs:     []any{1, "Alice", 2, "Bob"},
			expectError:      false,
		},
		{
			name:       "pointer slice with named parameters",
			inputQuery: "INSERT INTO users (id, name) VALUES (:id, :name)",
			inputArg: []*basicStruct{
				{Identifier: 1, FullName: "Alice"},
				{Identifier: 2, FullName: "Bob"},
			},
			structTag:        "db",
			expectedAt:       "INSERT INTO users (id, name) VALUES (@p1, @p2),(@p3, @p4)",
			expectedColon:    "INSERT INTO users (id, name) VALUES (:id, :name),(:id, :name)",
			expectedDollar:   "INSERT INTO users (id, name) VALUES ($1, $2),($3, $4)",
			expectedQuestion: "INSERT INTO users (id, name) VALUES (?, ?),(?, ?)",
			expectedArgs:     []any{1, "Alice", 2, "Bob"},
			expectError:      false,
		},
		{
			name:             "in clause with named map",
			inputQuery:       "SELECT * FROM user WHERE id IN (:ids)",
			inputArg:         map[string]any{"ids": []int{4, 5, 6}},
			expectedAt:       "SELECT * FROM user WHERE id IN (@p1,@p2,@p3)",
			expectedColon:    "SELECT * FROM user WHERE id IN (:ids,:ids,:ids)",
			expectedDollar:   "SELECT * FROM user WHERE id IN ($1,$2,$3)",
			expectedQuestion: "SELECT * FROM user WHERE id IN (?,?,?)",
			expectedArgs:     []any{4, 5, 6},
			expectError:      false,
		},
		{
			name:             "in clause with multiple named map",
			inputQuery:       "SELECT * FROM user WHERE name = :name AND id IN (:ids)",
			inputArg:         map[string]any{"name": "Alice", "ids": []int{4, 5, 6}},
			expectedAt:       "SELECT * FROM user WHERE name = @p1 AND id IN (@p2,@p3,@p4)",
			expectedColon:    "SELECT * FROM user WHERE name = :name AND id IN (:ids,:ids,:ids)",
			expectedDollar:   "SELECT * FROM user WHERE name = $1 AND id IN ($2,$3,$4)",
			expectedQuestion: "SELECT * FROM user WHERE name = ? AND id IN (?,?,?)",
			expectedArgs:     []any{"Alice", 4, 5, 6},
			expectError:      false,
		},
		{
			name:             "in clause with named struct",
			inputQuery:       "SELECT * FROM user WHERE id IN (:ids)",
			inputArg:         struct{ Ids []int }{Ids: []int{4, 5, 6}},
			expectedAt:       "SELECT * FROM user WHERE id IN (@p1,@p2,@p3)",
			expectedColon:    "SELECT * FROM user WHERE id IN (:ids,:ids,:ids)",
			expectedDollar:   "SELECT * FROM user WHERE id IN ($1,$2,$3)",
			expectedQuestion: "SELECT * FROM user WHERE id IN (?,?,?)",
			expectedArgs:     []any{4, 5, 6},
			expectError:      false,
		},
		{
			name:       "in clause with multiple named struct",
			inputQuery: "SELECT * FROM user WHERE name = :name AND id IN (:ids)",
			inputArg: struct {
				Name string
				Ids  []int
			}{Name: "Alice", Ids: []int{4, 5, 6}},
			expectedAt:       "SELECT * FROM user WHERE name = @p1 AND id IN (@p2,@p3,@p4)",
			expectedColon:    "SELECT * FROM user WHERE name = :name AND id IN (:ids,:ids,:ids)",
			expectedDollar:   "SELECT * FROM user WHERE name = $1 AND id IN ($2,$3,$4)",
			expectedQuestion: "SELECT * FROM user WHERE name = ? AND id IN (?,?,?)",
			expectedArgs:     []any{"Alice", 4, 5, 6},
			expectError:      false,
		},
		{
			name:        "invalid argument type",
			inputQuery:  "SELECT * FROM user WHERE id = :id",
			inputArg:    123, // Not a struct, map, array, or slice
			expectError: true,
		},
		{
			name:        "missing args",
			inputQuery:  "SELECT * FROM user WHERE id = :id",
			inputArg:    nil,
			expectError: true,
		},
		{
			name:        "empty query",
			inputQuery:  "",
			inputArg:    map[string]any{"id": 1},
			expectError: true,
		},
		{
			name:       "missing named parameter in struct",
			inputQuery: "SELECT * FROM user WHERE id = :id AND name = :name",
			inputArg: struct {
				ID int `db:"id"`
			}{ID: 1},
			expectError: true,
		},
		{
			name:        "missing named parameter in map",
			inputQuery:  "SELECT * FROM user WHERE id = :id AND name = :name",
			inputArg:    map[string]any{"id": 1},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := Compile(binds.At, tt.structTag, tt.inputQuery, tt.inputArg)
			assert.Equal(t, tt.expectError, err != nil, err)
			assert.Equal(t, tt.expectedAt, query)
			assert.Equal(t, tt.expectedArgs, args)

			query, args, err = Compile(binds.Colon, tt.structTag, tt.inputQuery, tt.inputArg)
			assert.Equal(t, tt.expectError, err != nil, err)
			assert.Equal(t, tt.expectedColon, query)
			assert.Equal(t, tt.expectedArgs, args)

			query, args, err = Compile(binds.Dollar, tt.structTag, tt.inputQuery, tt.inputArg)
			assert.Equal(t, tt.expectError, err != nil, err)
			assert.Equal(t, tt.expectedDollar, query)
			assert.Equal(t, tt.expectedArgs, args)

			query, args, err = Compile(binds.Question, tt.structTag, tt.inputQuery, tt.inputArg)
			assert.Equal(t, tt.expectError, err != nil, err)
			assert.Equal(t, tt.expectedQuestion, query)
			assert.Equal(t, tt.expectedArgs, args)
		})
	}
}

// testing nested fields with same key but different positions
func TestConcurrency(t *testing.T) {
	type withId1 struct {
		Id   int
		Name string
	}
	type withId2 struct {
		Name string
		Id   int
	}
	type person struct {
		User1 withId1
		User2 withId2
	}
	inputQuery := "INSERT INTO person (user1, user2) VALUES (:user1.id, :user2.id)"
	var persons []person
	for i := range 5 {
		p := person{
			User1: withId1{i, "name1"},
			User2: withId2{"name2", i * -1},
		}
		persons = append(persons, p)
	}
	expectedQuery := "INSERT INTO person (user1, user2) VALUES (?, ?),(?, ?),(?, ?),(?, ?),(?, ?)"
	expectedArgs := []any{0, 0, 1, -1, 2, -2, 3, -3, 4, -4}

	for range 1000 {
		go func() {
			query, args, err := Compile(binds.Question, "db", inputQuery, persons)
			assert.Equal(t, expectedQuery, query)
			assert.Equal(t, expectedArgs, args)
			assert.NoError(t, err)
		}()
	}
}

// goos: linux
// goarch: amd64
// pkg: github.com/rfberaldo/sqlz/named
// cpu: AMD Ryzen 5 5600X 6-Core Processor
// BenchmarkNamedMap-12    	    2404	    493504 ns/op	 1008546 B/op	    4075 allocs/op
func BenchmarkNamedMap(b *testing.B) {
	input := `INSERT INTO user (id, username, email, password, age) VALUES (:id, :username, :email, :password, :age)`

	var args []map[string]any
	for i := range 1000 {
		args = append(args, map[string]any{"id": i + 1, "username": "user123", "email": "user@example.com", "password": "abc123", "age": 18})
	}

	for range b.N {
		_, _, err := Compile(binds.Question, "db", input, args)
		assert.NoError(b, err)
	}
}

// goos: linux
// goarch: amd64
// pkg: github.com/rfberaldo/sqlz/named
// cpu: AMD Ryzen 5 5600X 6-Core Processor
// BenchmarkNamedStruct-12    	    2025	    592866 ns/op	 1071677 B/op	    5106 allocs/op
func BenchmarkNamedStruct(b *testing.B) {
	input := `INSERT INTO user (id, username, email, password, age) VALUES (:id, :username, :email, :password, :age)`

	type user struct {
		Id       int
		Username string
		Email    string
		Password string
		Age      int
	}
	var args []user
	for i := range 1000 {
		args = append(args, user{i + 1, "user123", "user@example.com", "abc123", 18})
	}

	for range b.N {
		_, _, err := Compile(binds.Question, "db", input, args)
		assert.NoError(b, err)
	}
}
