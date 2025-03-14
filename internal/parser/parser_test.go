package parser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		expectedAt       string
		expectedColon    string
		expectedDollar   string
		expectedQuestion string
		expectedIdents   []string
	}{
		{
			name:             "no named parameters",
			input:            "SELECT * FROM user WHERE id = 1",
			expectedAt:       "SELECT * FROM user WHERE id = 1",
			expectedColon:    "SELECT * FROM user WHERE id = 1",
			expectedDollar:   "SELECT * FROM user WHERE id = 1",
			expectedQuestion: "SELECT * FROM user WHERE id = 1",
			expectedIdents:   nil,
		},
		{
			name:             "single named parameter",
			input:            "SELECT * FROM user WHERE id = :id",
			expectedAt:       "SELECT * FROM user WHERE id = @p1",
			expectedColon:    "SELECT * FROM user WHERE id = :id",
			expectedDollar:   "SELECT * FROM user WHERE id = $1",
			expectedQuestion: "SELECT * FROM user WHERE id = ?",
			expectedIdents:   []string{"id"},
		},
		{
			name:             "multiple named parameters",
			input:            "SELECT * FROM user WHERE id = :id AND name = :name",
			expectedAt:       "SELECT * FROM user WHERE id = @p1 AND name = @p2",
			expectedColon:    "SELECT * FROM user WHERE id = :id AND name = :name",
			expectedDollar:   "SELECT * FROM user WHERE id = $1 AND name = $2",
			expectedQuestion: "SELECT * FROM user WHERE id = ? AND name = ?",
			expectedIdents:   []string{"id", "name"},
		},
		{
			name:             "insert parameters with numbers",
			input:            `INSERT INTO user (id, username, email, password, age) VALUES (:id, :username1, :email2, :pass3word, :age)`,
			expectedAt:       `INSERT INTO user (id, username, email, password, age) VALUES (@p1, @p2, @p3, @p4, @p5)`,
			expectedColon:    `INSERT INTO user (id, username, email, password, age) VALUES (:id, :username1, :email2, :pass3word, :age)`,
			expectedDollar:   `INSERT INTO user (id, username, email, password, age) VALUES ($1, $2, $3, $4, $5)`,
			expectedQuestion: `INSERT INTO user (id, username, email, password, age) VALUES (?, ?, ?, ?, ?)`,
			expectedIdents:   []string{"id", "username1", "email2", "pass3word", "age"},
		},
		{
			name:             "insert parameters with trailing semicolon",
			input:            `INSERT INTO user (id, username) VALUES (:id, :username);`,
			expectedAt:       `INSERT INTO user (id, username) VALUES (@p1, @p2)`,
			expectedColon:    `INSERT INTO user (id, username) VALUES (:id, :username)`,
			expectedDollar:   `INSERT INTO user (id, username) VALUES ($1, $2)`,
			expectedQuestion: `INSERT INTO user (id, username) VALUES (?, ?)`,
			expectedIdents:   []string{"id", "username"},
		},
		{
			name:             "escaped colon",
			input:            `SELECT "::foo" FROM user WHERE id = :id AND name = '::name'`,
			expectedAt:       `SELECT ":foo" FROM user WHERE id = @p1 AND name = ':name'`,
			expectedColon:    `SELECT ":foo" FROM user WHERE id = :id AND name = ':name'`,
			expectedDollar:   `SELECT ":foo" FROM user WHERE id = $1 AND name = ':name'`,
			expectedQuestion: `SELECT ":foo" FROM user WHERE id = ? AND name = ':name'`,
			expectedIdents:   []string{"id"},
		},
		{
			name:             "variable assignment",
			input:            `SELECT @name := "name", :age, :first, :last`,
			expectedAt:       `SELECT @name := "name", @p1, @p2, @p3`,
			expectedColon:    `SELECT @name := "name", :age, :first, :last`,
			expectedDollar:   `SELECT @name := "name", $1, $2, $3`,
			expectedQuestion: `SELECT @name := "name", ?, ?, ?`,
			expectedIdents:   []string{"age", "first", "last"},
		},
		{
			name:             "named parameter with underscore",
			input:            "SELECT * FROM user WHERE user_id = :user_id",
			expectedAt:       "SELECT * FROM user WHERE user_id = @p1",
			expectedColon:    "SELECT * FROM user WHERE user_id = :user_id",
			expectedDollar:   "SELECT * FROM user WHERE user_id = $1",
			expectedQuestion: "SELECT * FROM user WHERE user_id = ?",
			expectedIdents:   []string{"user_id"},
		},
		{
			name:             "escaped multiple colon",
			input:            `SELECT 'a::b::c' || first_name, '::::ABC::_::' FROM person WHERE first_name=:first_name AND last_name=:last_name`,
			expectedAt:       `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=@p1 AND last_name=@p2`,
			expectedColon:    `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=:first_name AND last_name=:last_name`,
			expectedDollar:   `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=$1 AND last_name=$2`,
			expectedQuestion: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=? AND last_name=?`,
			expectedIdents:   []string{"first_name", "last_name"},
		},
		{
			name:             "named parameter with dot",
			input:            "SELECT * FROM user WHERE user.name = :user.name",
			expectedAt:       "SELECT * FROM user WHERE user.name = @p1",
			expectedColon:    "SELECT * FROM user WHERE user.name = :user.name",
			expectedDollar:   "SELECT * FROM user WHERE user.name = $1",
			expectedQuestion: "SELECT * FROM user WHERE user.name = ?",
			expectedIdents:   []string{"user.name"},
		},
		{
			name:             "mixed named parameters and escaped colons",
			input:            "SELECT * FROM user WHERE id = :id AND name = '::name' AND age = :age",
			expectedAt:       "SELECT * FROM user WHERE id = @p1 AND name = ':name' AND age = @p2",
			expectedColon:    "SELECT * FROM user WHERE id = :id AND name = ':name' AND age = :age",
			expectedDollar:   "SELECT * FROM user WHERE id = $1 AND name = ':name' AND age = $2",
			expectedQuestion: "SELECT * FROM user WHERE id = ? AND name = ':name' AND age = ?",
			expectedIdents:   []string{"id", "age"},
		},
		{
			name:             "parenthesis around named parameters",
			input:            "SELECT * FROM user WHERE id = (:id) AND name = :name",
			expectedAt:       "SELECT * FROM user WHERE id = (@p1) AND name = @p2",
			expectedColon:    "SELECT * FROM user WHERE id = (:id) AND name = :name",
			expectedDollar:   "SELECT * FROM user WHERE id = ($1) AND name = $2",
			expectedQuestion: "SELECT * FROM user WHERE id = (?) AND name = ?",
			expectedIdents:   []string{"id", "name"},
		},
		{
			name:             "empty input",
			input:            "",
			expectedAt:       "",
			expectedColon:    "",
			expectedDollar:   "",
			expectedQuestion: "",
			expectedIdents:   nil,
		},
		// TODO: add an option to parse using runes instead of bytes
		// {
		// 	name:             "non english characters",
		// 	input:            "INSERT INTO foo (a, b, c) VALUES (:あ, :b, :名前)",
		// 	expectedAt:       "INSERT INTO foo (a, b, c) VALUES (@p1, @p2, @p3)",
		// 	expectedColon:    "INSERT INTO foo (a, b, c) VALUES (:あ, :b, :名前)",
		// 	expectedDollar:   "INSERT INTO foo (a, b, c) VALUES ($1, $2, $3)",
		// 	expectedQuestion: "INSERT INTO foo (a, b, c) VALUES (?, ?, ?)",
		// 	expectedIdents:   []string{"あ", "b", "名前"},
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, idents := ParseNamed(BindAt, tt.input)
			assert.Equal(t, tt.expectedAt, query)
			assert.Equal(t, tt.expectedIdents, idents)
			query = ParseQuery(BindAt, tt.input)
			assert.Equal(t, tt.expectedAt, query)
			idents = ParseIdents(BindAt, tt.input)
			assert.Equal(t, tt.expectedIdents, idents)

			query, idents = ParseNamed(BindColon, tt.input)
			assert.Equal(t, tt.expectedColon, query)
			assert.Equal(t, tt.expectedIdents, idents)
			query = ParseQuery(BindColon, tt.input)
			assert.Equal(t, tt.expectedColon, query)
			idents = ParseIdents(BindColon, tt.input)
			assert.Equal(t, tt.expectedIdents, idents)

			query, idents = ParseNamed(BindDollar, tt.input)
			assert.Equal(t, tt.expectedDollar, query)
			assert.Equal(t, tt.expectedIdents, idents)
			query = ParseQuery(BindDollar, tt.input)
			assert.Equal(t, tt.expectedDollar, query)
			idents = ParseIdents(BindDollar, tt.input)
			assert.Equal(t, tt.expectedIdents, idents)

			query, idents = ParseNamed(BindQuestion, tt.input)
			assert.Equal(t, tt.expectedQuestion, query)
			assert.Equal(t, tt.expectedIdents, idents)
			query = ParseQuery(BindQuestion, tt.input)
			assert.Equal(t, tt.expectedQuestion, query)
			idents = ParseIdents(BindQuestion, tt.input)
			assert.Equal(t, tt.expectedIdents, idents)
		})
	}
}

func TestParseInClause(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		inputArgs        []any
		expectedArgs     []any
		expectedAt       string
		expectedColon    string
		expectedDollar   string
		expectedQuestion string
		expectError      bool
	}{
		{
			name:        "no named parameters",
			input:       "SELECT * FROM user WHERE id = 1",
			inputArgs:   nil,
			expectError: true,
		},
		{
			name:             "one IN parameter",
			input:            "SELECT * FROM user WHERE id IN (:ids)",
			inputArgs:        []any{[]int{3, 4, 5}},
			expectedArgs:     []any{3, 4, 5},
			expectedAt:       "SELECT * FROM user WHERE id IN (@p1,@p2,@p3)",
			expectedColon:    "SELECT * FROM user WHERE id IN (:ids,:ids,:ids)",
			expectedDollar:   "SELECT * FROM user WHERE id IN ($1,$2,$3)",
			expectedQuestion: "SELECT * FROM user WHERE id IN (?,?,?)",
		},
		{
			name:             "one IN parameter with one 1-len slice",
			input:            "SELECT * FROM user WHERE id IN (:ids)",
			inputArgs:        []any{[]int{32}},
			expectedArgs:     []any{32},
			expectedAt:       "SELECT * FROM user WHERE id IN (@p1)",
			expectedColon:    "SELECT * FROM user WHERE id IN (:ids)",
			expectedDollar:   "SELECT * FROM user WHERE id IN ($1)",
			expectedQuestion: "SELECT * FROM user WHERE id IN (?)",
		},
		{
			name:             "multiple named parameters and one IN parameter",
			input:            "SELECT * FROM user WHERE name = :name AND id IN (:ids) AND email = :email",
			inputArgs:        []any{"Alice", []int{4, 8, 16}, "alice@inchains.com"},
			expectedArgs:     []any{"Alice", 4, 8, 16, "alice@inchains.com"},
			expectedAt:       "SELECT * FROM user WHERE name = @p1 AND id IN (@p2,@p3,@p4) AND email = @p5",
			expectedColon:    "SELECT * FROM user WHERE name = :name AND id IN (:ids,:ids,:ids) AND email = :email",
			expectedDollar:   "SELECT * FROM user WHERE name = $1 AND id IN ($2,$3,$4) AND email = $5",
			expectedQuestion: "SELECT * FROM user WHERE name = ? AND id IN (?,?,?) AND email = ?",
		},
		{
			name:             "multiple named parameters and IN parameter",
			input:            "SELECT * FROM user WHERE name = :name AND id IN (:ids) AND email = :email AND company IN (:companies)",
			inputArgs:        []any{"Alice", []int{4, 8, 16}, "alice@inchains.com", []string{"The Band", "Wonderland"}},
			expectedArgs:     []any{"Alice", 4, 8, 16, "alice@inchains.com", "The Band", "Wonderland"},
			expectedAt:       "SELECT * FROM user WHERE name = @p1 AND id IN (@p2,@p3,@p4) AND email = @p5 AND company IN (@p6,@p7)",
			expectedColon:    "SELECT * FROM user WHERE name = :name AND id IN (:ids,:ids,:ids) AND email = :email AND company IN (:companies,:companies)",
			expectedDollar:   "SELECT * FROM user WHERE name = $1 AND id IN ($2,$3,$4) AND email = $5 AND company IN ($6,$7)",
			expectedQuestion: "SELECT * FROM user WHERE name = ? AND id IN (?,?,?) AND email = ? AND company IN (?,?)",
		},
		{
			name:        "an empty slice",
			input:       "SELECT * FROM user WHERE id IN (:ids)",
			inputArgs:   []any{[]int{}},
			expectError: true,
		},
		{
			name:        "wrong number of idents",
			input:       "SELECT * FROM user WHERE id IN (:ids) AND name = :name",
			inputArgs:   []any{[]int{2}},
			expectError: true,
		},
		{
			name:        "empty input",
			input:       "",
			inputArgs:   nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := ParseInNamed(BindAt, tt.input, tt.inputArgs)
			assert.Equal(t, tt.expectError, err != nil, err)
			assert.Equal(t, tt.expectedAt, query)
			assert.Equal(t, tt.expectedArgs, args)

			query, args, err = ParseInNamed(BindColon, tt.input, tt.inputArgs)
			assert.Equal(t, tt.expectError, err != nil, err)
			assert.Equal(t, tt.expectedColon, query)
			assert.Equal(t, tt.expectedArgs, args)

			query, args, err = ParseInNamed(BindDollar, tt.input, tt.inputArgs)
			assert.Equal(t, tt.expectError, err != nil, err)
			assert.Equal(t, tt.expectedDollar, query)
			assert.Equal(t, tt.expectedArgs, args)

			query, args, err = ParseInNamed(BindQuestion, tt.input, tt.inputArgs)
			assert.Equal(t, tt.expectError, err != nil, err)
			assert.Equal(t, tt.expectedQuestion, query)
			assert.Equal(t, tt.expectedArgs, args)
		})
	}
}

func TestParseIn(t *testing.T) {
	tests := []struct {
		name                 string
		input                string
		args                 []any
		expectedOutput       string
		expectedArgs         []any
		expectError          bool
		expectWrongBindError bool
	}{
		{
			name:           "no bind vars and no arg",
			input:          "SELECT * FROM user WHERE id = 1",
			args:           nil,
			expectedOutput: "SELECT * FROM user WHERE id = 1",
			expectedArgs:   nil,
			expectError:    false,
		},
		{
			name:           "no bind vars and empty arg",
			input:          "SELECT * FROM user WHERE id = 1",
			args:           []any{},
			expectedOutput: "SELECT * FROM user WHERE id = 1",
			expectedArgs:   []any{},
			expectError:    false,
		},
		{
			name:           "one bind var but no slice",
			input:          "SELECT * FROM user WHERE id = ?",
			args:           []any{8},
			expectedOutput: "SELECT * FROM user WHERE id = ?",
			expectedArgs:   []any{8},
			expectError:    false,
		},
		{
			name:                 "one bind var with slice",
			input:                "SELECT * FROM user WHERE id IN (?)",
			args:                 []any{[]int{4, 8, 16}},
			expectedOutput:       "SELECT * FROM user WHERE id IN (?,?,?)",
			expectedArgs:         []any{4, 8, 16},
			expectError:          false,
			expectWrongBindError: true,
		},
		{
			name:                 "one bind var with 1-len slice",
			input:                "SELECT * FROM user WHERE id IN (?)",
			args:                 []any{[]int{4}},
			expectedOutput:       "SELECT * FROM user WHERE id IN (?)",
			expectedArgs:         []any{4},
			expectError:          false,
			expectWrongBindError: true,
		},
		{
			name:                 "two bind var and one slice",
			input:                "SELECT * FROM user WHERE name = ? AND id IN (?)",
			args:                 []any{"Alice", []int{4, 8, 16}},
			expectedOutput:       "SELECT * FROM user WHERE name = ? AND id IN (?,?,?)",
			expectedArgs:         []any{"Alice", 4, 8, 16},
			expectError:          false,
			expectWrongBindError: true,
		},
		{
			name:                 "multiple bind var and two slices",
			input:                "SELECT * FROM user WHERE name = ? AND id IN (?) AND band_id IN (?)",
			args:                 []any{"Alice", []int{4, 8, 16}, []int{8, 16, 32, 64}},
			expectedOutput:       "SELECT * FROM user WHERE name = ? AND id IN (?,?,?) AND band_id IN (?,?,?,?)",
			expectedArgs:         []any{"Alice", 4, 8, 16, 8, 16, 32, 64},
			expectError:          false,
			expectWrongBindError: true,
		},
		{
			name:                 "multiple bind var and one escaped",
			input:                "SELECT * FROM user WHERE name = '??' AND id IN (?) AND band_id IN (?)",
			args:                 []any{[]int{4, 8, 16}, []int{8, 16, 32, 64}},
			expectedOutput:       "SELECT * FROM user WHERE name = '?' AND id IN (?,?,?) AND band_id IN (?,?,?,?)",
			expectedArgs:         []any{4, 8, 16, 8, 16, 32, 64},
			expectError:          false,
			expectWrongBindError: true,
		},
		{
			name:                 "wrong number of placeholders",
			input:                "SELECT * FROM user WHERE name = ? AND id IN (?)",
			args:                 []any{4, []int{8, 16, 32, 64}, 8},
			expectError:          true,
			expectWrongBindError: true,
		},
		{
			name:                 "empty slice expects error",
			input:                "SELECT * FROM user WHERE id IN (?)",
			args:                 []any{[]int{}},
			expectError:          true,
			expectWrongBindError: true,
		},
		{
			name:                 "empty input",
			input:                "",
			args:                 nil,
			expectedOutput:       "",
			expectedArgs:         nil,
			expectError:          false,
			expectWrongBindError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := ParseIn(BindAt, tt.input, tt.args...)
			assert.Equal(t, tt.expectWrongBindError, err != nil, err)
			if !tt.expectWrongBindError {
				assert.Equal(t, tt.input, query)
				assert.Equal(t, tt.args, args)
			}

			query, args, err = ParseIn(BindColon, tt.input, tt.args...)
			assert.Equal(t, tt.expectWrongBindError, err != nil, err)
			if !tt.expectWrongBindError {
				assert.Equal(t, tt.input, query)
				assert.Equal(t, tt.args, args)
			}

			query, args, err = ParseIn(BindDollar, tt.input, tt.args...)
			assert.Equal(t, tt.expectWrongBindError, err != nil, err)
			if !tt.expectWrongBindError {
				assert.Equal(t, tt.input, query)
				assert.Equal(t, tt.args, args)
			}

			query, args, err = ParseIn(BindQuestion, tt.input, tt.args...)
			assert.Equal(t, tt.expectError, err != nil, err)
			if !tt.expectError {
				assert.Equal(t, tt.expectedOutput, query)
				assert.Equal(t, tt.expectedArgs, args)
			}
		})
	}
}

func TestConcurrency(t *testing.T) {
	input := "SELECT * FROM user WHERE id = :id"
	expectedQuery := "SELECT * FROM user WHERE id = ?"
	expectedIdents := []string{"id"}

	for range 1000 {
		go func() {
			query, idents := ParseNamed(BindQuestion, input)
			assert.Equal(t, expectedQuery, query)
			assert.Equal(t, expectedIdents, idents)
		}()
	}
}

// goos: linux
// goarch: amd64
// pkg: github.com/rafaberaldo/sqlz/named-parser
// cpu: AMD Ryzen 5 5600X 6-Core Processor
// BenchmarkParser-12    	    7802	    144034 ns/op	  302981 B/op	      32 allocs/op
func BenchmarkParser(b *testing.B) {
	var sb strings.Builder
	sb.WriteString(`INSERT INTO user (id, username, email, password, age) VALUES (:id, :username, :email, :password, :age)`)
	for range 1000 {
		sb.WriteString(`,(:id, :username, :email, :password, :age)`)
	}

	input := sb.String()

	for range b.N {
		ParseNamed(BindQuestion, input)
	}
}
