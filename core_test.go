package sqlz

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/rfberaldo/sqlz/internal/testutil"
	"github.com/stretchr/testify/assert"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var ctx = context.Background()

func TestCore_BasicQueryMethods(t *testing.T) {
	multi := testutil.NewMultiConn(t)
	multi.Run(t, func(t *testing.T, conn *testutil.Conn) {
		var err error
		var s string
		var ss []string

		query := "SELECT 'Hello World'"
		expected := "Hello World"
		expectedSlice := []string{"Hello World"}

		err = Query(ctx, conn.DB, conn.Bind, defaultStructTag, &ss, query)
		assert.NoError(t, err)
		assert.Equal(t, expectedSlice, ss)

		err = QueryRow(ctx, conn.DB, conn.Bind, defaultStructTag, &s, query)
		assert.NoError(t, err)
		assert.Equal(t, expected, s)
	})
}

func TestCore_ShouldReturnErrorForWrongQuery(t *testing.T) {
	multi := testutil.NewMultiConn(t)
	multi.Run(t, func(t *testing.T, conn *testutil.Conn) {
		var err error
		var dst any
		const query = "WRONG QUERY"
		const shouldContain = "WRONG"

		_, err = Exec(ctx, conn.DB, conn.Bind, defaultStructTag, query)
		assert.Error(t, err)
		assert.ErrorContains(t, err, shouldContain)

		err = Query(ctx, conn.DB, conn.Bind, defaultStructTag, &dst, query)
		assert.Error(t, err)
		assert.ErrorContains(t, err, shouldContain)

		err = QueryRow(ctx, conn.DB, conn.Bind, defaultStructTag, &dst, query)
		assert.Error(t, err)
		assert.ErrorContains(t, err, shouldContain)
	})
}

func TestCore_ShouldReturnNotFoundOnQueryRow(t *testing.T) {
	multi := testutil.NewMultiConn(t)
	multi.Run(t, func(t *testing.T, conn *testutil.Conn) {
		th := testutil.NewTableHelper(t, conn.DB, conn.Bind)

		_, err := conn.DB.Exec(th.Fmt(`CREATE TABLE %s (id INT PRIMARY KEY)`))
		assert.NoError(t, err)

		q := th.Fmt("SELECT * FROM %s")

		var s any
		err = QueryRow(ctx, conn.DB, conn.Bind, defaultStructTag, &s, q)
		assert.Error(t, err)
		assert.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestCore_QueryArgs(t *testing.T) {
	multi := testutil.NewMultiConn(t)
	multi.Run(t, func(t *testing.T, conn *testutil.Conn) {
		th := testutil.NewTableHelper(t, conn.DB, conn.Bind)

		_, err := conn.DB.Exec(th.Fmt(`
			CREATE TABLE %s (
				id INT PRIMARY KEY,
				username VARCHAR(255),
				age INT,
				active BOOL,
				created_at TIMESTAMP
			)`,
		))
		assert.NoError(t, err)

		ts := time.Now().UTC().Truncate(time.Second)

		q := th.Fmt(`
		INSERT INTO %s (id, username, age, active, created_at)
		VALUES (?,?,?,?,?),(?,?,?,?,?),(?,?,?,?,?)`)

		_, err = conn.DB.Exec(q,
			1, "Alice", 18, true, ts,
			2, "Rob", 38, true, ts,
			3, "John", 24, false, ts,
		)
		assert.NoError(t, err)

		type User struct {
			Id        int
			Username  string
			Age       int
			Active    bool
			CreatedAt time.Time
		}

		t.Run("query without args should perform a regular query", func(t *testing.T) {
			expected := []User{
				{1, "Alice", 18, true, ts},
				{2, "Rob", 38, true, ts},
				{3, "John", 24, false, ts},
			}
			var users []User
			q := th.Fmt("SELECT * FROM %s")
			err = Query(ctx, conn.DB, conn.Bind, defaultStructTag, &users, q)
			assert.NoError(t, err)
			assert.Equal(t, 3, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should work with multiple default placeholders", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
				{3, "John", 24, false, ts},
			}
			q := th.Fmt(`SELECT * FROM %s WHERE id = ? OR id = ?`)
			var users []User
			err = Query(ctx, conn.DB, conn.Bind, defaultStructTag, &users, q, 2, 3)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should parse IN clause using default placeholder", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
				{3, "John", 24, false, ts},
			}
			q := th.Fmt(`SELECT * FROM %s WHERE id IN (?)`)
			var users []User
			ids := []int{2, 3}
			err = Query(ctx, conn.DB, conn.Bind, defaultStructTag, &users, q, ids)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should work with struct named arg", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
			}
			q := th.Fmt(`SELECT * FROM %s WHERE id = :id`)
			var users []User
			arg := struct{ Id int }{Id: 2}
			err = Query(ctx, conn.DB, conn.Bind, defaultStructTag, &users, q, arg)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should work with map named arg", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
			}
			q := th.Fmt(`SELECT * FROM %s WHERE id = :id`)
			var users []User
			arg := map[string]any{"id": 2}
			err = Query(ctx, conn.DB, conn.Bind, defaultStructTag, &users, q, arg)
			assert.NoError(t, err)
			assert.Equal(t, 1, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should parse IN clause using named arg", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
				{3, "John", 24, false, ts},
			}
			q := th.Fmt(`SELECT * FROM %s WHERE id IN (:ids)`)
			var users []User
			arg := map[string]any{"ids": []int{2, 3}}
			err = Query(ctx, conn.DB, conn.Bind, defaultStructTag, &users, q, arg)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should return length 0 if no result is found", func(t *testing.T) {
			q := th.Fmt(`SELECT * FROM %s WHERE id = 42`)
			var users []User
			err = Query(ctx, conn.DB, conn.Bind, defaultStructTag, &users, q)
			assert.NoError(t, err)
			assert.Equal(t, 0, len(users))
		})
	})
}

func TestCore_QueryRowArgs(t *testing.T) {
	multi := testutil.NewMultiConn(t)
	multi.Run(t, func(t *testing.T, conn *testutil.Conn) {
		th := testutil.NewTableHelper(t, conn.DB, conn.Bind)

		_, err := conn.DB.Exec(th.Fmt(`
			CREATE TABLE %s (
				id INT PRIMARY KEY,
				username VARCHAR(255),
				age INT,
				active BOOL,
				created_at TIMESTAMP
			)`,
		))
		assert.NoError(t, err)

		ts := time.Now().UTC().Truncate(time.Second)

		q := th.Fmt(`
		INSERT INTO %s (id, username, age, active, created_at)
		VALUES (?,?,?,?,?),(?,?,?,?,?),(?,?,?,?,?)`)

		_, err = conn.DB.Exec(q,
			1, "Alice", 18, true, ts,
			2, "Rob", 38, true, ts,
			3, "John", 24, false, ts,
		)
		assert.NoError(t, err)

		type User struct {
			Id       int       `db:"id"`
			Username string    `db:"username"`
			Age      int       `db:"age"`
			Active   bool      `db:"active"`
			Created  time.Time `db:"created_at"`
		}

		t.Run("query row without args should perform a regular query", func(t *testing.T) {
			expected := User{1, "Alice", 18, true, ts}
			var user User
			q := th.Fmt("SELECT * FROM %s LIMIT 1")
			err = QueryRow(ctx, conn.DB, conn.Bind, defaultStructTag, &user, q)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should work with multiple default placeholders", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			q := th.Fmt(`SELECT * FROM %s WHERE id = ? AND active = ?`)
			var user User
			err = QueryRow(ctx, conn.DB, conn.Bind, defaultStructTag, &user, q, 2, true)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should parse IN clause using default placeholder", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			q := th.Fmt(`SELECT * FROM %s WHERE id IN (?)`)
			var user User
			ids := []int{2}
			err = QueryRow(ctx, conn.DB, conn.Bind, defaultStructTag, &user, q, ids)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should work with struct named arg", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			q := th.Fmt(`SELECT * FROM %s WHERE id = :id`)
			var user User
			arg := struct{ Id int }{Id: 2}
			err = QueryRow(ctx, conn.DB, conn.Bind, defaultStructTag, &user, q, arg)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should work with map named arg", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			q := th.Fmt(`SELECT * FROM %s WHERE id = :id`)
			var user User
			arg := map[string]any{"id": 2}
			err = QueryRow(ctx, conn.DB, conn.Bind, defaultStructTag, &user, q, arg)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should parse IN clause using named arg", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			q := th.Fmt(`SELECT * FROM %s WHERE id IN (:ids)`)
			var user User
			arg := map[string]any{"ids": []int{2}}
			err = QueryRow(ctx, conn.DB, conn.Bind, defaultStructTag, &user, q, arg)
			assert.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("query row should return error if no result is found", func(t *testing.T) {
			q := th.Fmt(`SELECT * FROM %s WHERE id = 42`)
			var user User
			err = QueryRow(ctx, conn.DB, conn.Bind, defaultStructTag, &user, q)
			assert.Error(t, err)
			assert.ErrorIs(t, err, sql.ErrNoRows)
		})

		t.Run("query row should return correct error if value is null", func(t *testing.T) {
			q := th.Fmt(`INSERT INTO %s (id, username, age, active, created_at) VALUES (?,?,?,?,?)`)
			_, err = conn.DB.Exec(q, 100, nil, 18, true, ts)
			assert.NoError(t, err)

			q = th.Fmt(`SELECT * FROM %s WHERE id = 100`)
			var user User
			err = QueryRow(ctx, conn.DB, conn.Bind, defaultStructTag, &user, q)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "converting NULL to string is unsupported")
		})
	})
}

func TestCore_ExecArgs(t *testing.T) {
	multi := testutil.NewMultiConn(t)
	multi.Run(t, func(t *testing.T, conn *testutil.Conn) {
		th := testutil.NewTableHelper(t, conn.DB, conn.Bind)

		_, err := conn.DB.Exec(th.Fmt(`
			CREATE TABLE %s (
				id INT PRIMARY KEY,
				name VARCHAR(255),
				age INT,
				created_at TIMESTAMP
			)`,
		))
		assert.NoError(t, err)

		t.Run("multiple args should perform a regular exec", func(t *testing.T) {
			q := th.Fmt(`
			INSERT INTO %s (id, name, age)
			VALUES (?,?,?),(?,?,?),(?,?,?)`)

			re, err := Exec(ctx, conn.DB, conn.Bind, defaultStructTag, q,
				1, "Alice", 18,
				2, "Rob", 38,
				3, "John", 4,
			)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, 3, int(rows))
		})

		t.Run("1 arg struct should perform a named exec", func(t *testing.T) {
			q := th.Fmt("DELETE FROM %s WHERE id = :id")
			arg := struct{ Id int }{Id: 1}
			re, err := Exec(ctx, conn.DB, conn.Bind, defaultStructTag, q, arg)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, 1, int(rows))
		})

		t.Run("1 arg map should perform a named exec", func(t *testing.T) {
			q := th.Fmt("DELETE FROM %s WHERE id = :id")
			arg := map[string]any{"id": 2}
			re, err := Exec(ctx, conn.DB, conn.Bind, defaultStructTag, q, arg)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, 1, int(rows))
		})

		t.Run("1 arg int should perform a regular exec", func(t *testing.T) {
			q := th.Fmt("DELETE FROM %s WHERE id = ?")
			arg := 3
			re, err := Exec(ctx, conn.DB, conn.Bind, defaultStructTag, q, arg)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, 1, int(rows))
		})

		t.Run("1 arg []struct should perform a named batch insert", func(t *testing.T) {
			type Person struct {
				Id        int
				Name      string
				Age       int
				CreatedAt time.Time
			}
			const COUNT = 100
			args := make([]Person, COUNT)
			for i := range COUNT {
				args[i] = Person{i + 1, "Name", 20, time.Now()}
			}
			q := th.Fmt(`INSERT INTO %s (id, name, age, created_at) VALUES (:id, :name, :age, :created_at)`)
			re, err := Exec(ctx, conn.DB, conn.Bind, defaultStructTag, q, args)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, COUNT, int(rows))

			re, err = Exec(ctx, conn.DB, conn.Bind, defaultStructTag, th.Fmt("DELETE FROM %s"))
			assert.NoError(t, err)

			rows, err = re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, COUNT, int(rows))
		})

		t.Run("1 arg []*struct should perform a named batch insert", func(t *testing.T) {
			type Person struct {
				Id        int
				Name      string
				Age       int
				CreatedAt time.Time
			}
			const COUNT = 100
			args := make([]*Person, COUNT)
			for i := range COUNT {
				args[i] = &Person{i + 1, "Name", 20, time.Now()}
			}
			q := th.Fmt(`INSERT INTO %s (id, name, age, created_at) VALUES (:id, :name, :age, :created_at)`)
			re, err := Exec(ctx, conn.DB, conn.Bind, defaultStructTag, q, args)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, COUNT, int(rows))

			re, err = Exec(ctx, conn.DB, conn.Bind, defaultStructTag, th.Fmt("DELETE FROM %s"))
			assert.NoError(t, err)

			rows, err = re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, COUNT, int(rows))
		})

		t.Run("1 arg []map should perform a named batch insert", func(t *testing.T) {
			const COUNT = 100
			args := make([]map[string]any, COUNT)
			for i := range COUNT {
				args[i] = map[string]any{"id": i + 1, "name": "Name", "age": 20}
			}
			q := th.Fmt(`INSERT INTO %s (id, name, age) VALUES (:id, :name, :age)`)
			re, err := Exec(ctx, conn.DB, conn.Bind, defaultStructTag, q, args)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, COUNT, int(rows))
		})

		t.Run("should be able to perform in clause using named args", func(t *testing.T) {
			args := map[string]any{"ids": []int{10, 11, 12}}
			q := th.Fmt(`DELETE FROM %s WHERE id IN (:ids)`)
			re, err := Exec(ctx, conn.DB, conn.Bind, defaultStructTag, q, args)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, 3, int(rows))
		})

		t.Run("should be able to perform in clause using placeholder", func(t *testing.T) {
			args := []int{20, 21, 22}
			q := th.Fmt(`DELETE FROM %s WHERE id IN (?)`)
			re, err := Exec(ctx, conn.DB, conn.Bind, defaultStructTag, q, args)
			assert.NoError(t, err)

			rows, err := re.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, len(args), int(rows))
		})
	})
}

func TestCore_CustomStructTag(t *testing.T) {
	multi := testutil.NewMultiConn(t)
	multi.Run(t, func(t *testing.T, conn *testutil.Conn) {
		th := testutil.NewTableHelper(t, conn.DB, conn.Bind)

		_, err := conn.DB.Exec(th.Fmt(`
			CREATE TABLE %s (
				id INT PRIMARY KEY,
				username VARCHAR(255),
				email VARCHAR(255),
				password VARCHAR(255),
				age INT,
				active BOOL
			)`,
		))
		assert.NoError(t, err)

		q := th.Fmt(`
		INSERT INTO %s (id, username, email, password, age, active)
		VALUES (?,?,?,?,?,?),(?,?,?,?,?,?),(?,?,?,?,?,?)`)

		_, err = conn.DB.Exec(q,
			1, "Alice", "alice@wonderland.com", "123456", 18, true,
			2, "Rob", "rob@google.com", "123456", 38, true,
			3, "John", "john@id.com", "123456", 24, false,
		)
		assert.NoError(t, err)

		type User struct {
			Id     int
			User   string `json:"username"`
			Email  string
			Pw     string `json:"password"`
			Age    int
			Active bool
		}

		expected := User{1, "Alice", "alice@wonderland.com", "123456", 18, true}
		var user User
		err = QueryRow(ctx, conn.DB, conn.Bind, "json", &user, th.Fmt("SELECT * FROM %s LIMIT 1"))
		assert.NoError(t, err)
		assert.Equal(t, expected, user)
	})
}

func TestCore_NonEnglishCharacters(t *testing.T) {
	multi := testutil.NewMultiConn(t)
	multi.Run(t, func(t *testing.T, conn *testutil.Conn) {
		th := testutil.NewTableHelper(t, conn.DB, conn.Bind)

		_, err := conn.DB.Exec(th.Fmt(`
			CREATE TABLE %s (
				id INT PRIMARY KEY,
				名前 VARCHAR(255),
				email VARCHAR(255),
				password VARCHAR(255),
				age INT,
				active BOOL
			)`,
		))
		assert.NoError(t, err)

		type User struct {
			Id       int
			Name     string `db:"名前"`
			Email    string
			Password string
			Age      int
			Active   bool
		}

		q := th.Fmt(`
		INSERT INTO %s (id, 名前, email, password, age, active)
		VALUES (:id,:名前,:email,:password,:age,:active)`)
		args := []User{
			{1, "Alice", "alice@wonderland.com", "123456", 18, true},
			{2, "Rob", "rob@google.com", "123456", 38, true},
			{3, "John", "john@id.com", "123456", 24, false},
		}
		_, err = Exec(ctx, conn.DB, conn.Bind, defaultStructTag, q, args)
		assert.NoError(t, err)

		expected := User{1, "Alice", "alice@wonderland.com", "123456", 18, true}
		var user User
		err = QueryRow(ctx, conn.DB, conn.Bind, defaultStructTag, &user, th.Fmt("SELECT * FROM %s LIMIT 1"))
		assert.NoError(t, err)
		assert.Equal(t, expected, user)
	})
}
