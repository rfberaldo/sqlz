package sqlz

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestBase_basic(t *testing.T) {
	runConn(t, func(t *testing.T, conn *Conn) {
		base := newBase(&config{bind: conn.bind})
		query := "SELECT 'Hello World'"

		t.Run("query", func(t *testing.T) {
			var got []string
			err := base.query(ctx, conn.db, query).Scan(&got)
			require.NoError(t, err)

			expect := []string{"Hello World"}
			assert.Equal(t, expect, got)
		})

		t.Run("queryRow", func(t *testing.T) {
			var got string
			err := base.queryRow(ctx, conn.db, query).Scan(&got)
			require.NoError(t, err)

			expect := "Hello World"
			assert.Equal(t, expect, got)
		})

		t.Run("exec", func(t *testing.T) {
			_, err := base.exec(ctx, conn.db, query)
			require.NoError(t, err)
		})
	})
}

func TestBase_basic_no_stmt_cache(t *testing.T) {
	runConn(t, func(t *testing.T, conn *Conn) {
		base := newBase(&config{bind: conn.bind, stmtCacheCapacity: 0})
		query := "SELECT 'Hello World'"

		t.Run("query", func(t *testing.T) {
			var got []string
			err := base.query(ctx, conn.db, query).Scan(&got)
			require.NoError(t, err)

			expect := []string{"Hello World"}
			assert.Equal(t, expect, got)
		})

		t.Run("queryRow", func(t *testing.T) {
			var got string
			err := base.queryRow(ctx, conn.db, query).Scan(&got)
			require.NoError(t, err)

			expect := "Hello World"
			assert.Equal(t, expect, got)
		})

		t.Run("exec", func(t *testing.T) {
			_, err := base.exec(ctx, conn.db, query)
			require.NoError(t, err)
		})
	})
}

func TestBase_query(t *testing.T) {
	runConn(t, func(t *testing.T, conn *Conn) {
		base := newBase(&config{bind: conn.bind})
		th := newTableHelper(t, conn.db, conn.bind)

		_, err := conn.db.Exec(th.fmt(`
			CREATE TABLE IF NOT EXISTS %s (
				id INT PRIMARY KEY,
				username VARCHAR(255),
				age INT,
				active BOOL,
				created_at TIMESTAMP
			)`,
		))
		require.NoError(t, err)

		ts := time.Now().UTC().Truncate(time.Second)

		q := th.fmt(`
		INSERT INTO %s (id, username, age, active, created_at)
		VALUES (?,?,?,?,?),(?,?,?,?,?),(?,?,?,?,?)`)

		_, err = conn.db.Exec(q,
			1, "Alice", 18, true, ts,
			2, "Rob", 38, true, ts,
			3, "John", 24, false, ts,
		)
		require.NoError(t, err)

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
			q := th.fmt("SELECT * FROM %s")
			err = base.query(ctx, conn.db, q).Scan(&users)
			require.NoError(t, err)
			assert.Equal(t, 3, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should work with multiple default placeholders", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
				{3, "John", 24, false, ts},
			}
			q := th.fmt(`SELECT * FROM %s WHERE id = ? OR id = ?`)
			var users []User
			err = base.query(ctx, conn.db, q, 2, 3).Scan(&users)
			require.NoError(t, err)
			assert.Equal(t, 2, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should parse IN clause using default placeholder", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
				{3, "John", 24, false, ts},
			}
			q := th.fmt(`SELECT * FROM %s WHERE id IN (?)`)
			var users []User
			ids := []int{2, 3}
			err = base.query(ctx, conn.db, q, ids).Scan(&users)
			require.NoError(t, err)
			assert.Equal(t, 2, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should work with struct named arg", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
			}
			q := th.fmt(`SELECT * FROM %s WHERE id = :id`)
			var users []User
			arg := struct{ Id int }{Id: 2}
			err = base.query(ctx, conn.db, q, arg).Scan(&users)
			require.NoError(t, err)
			assert.Equal(t, 1, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should work with map named arg", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
			}
			q := th.fmt(`SELECT * FROM %s WHERE id = :id`)
			var users []User
			arg := map[string]any{"id": 2}
			err = base.query(ctx, conn.db, q, arg).Scan(&users)
			require.NoError(t, err)
			assert.Equal(t, 1, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should parse IN clause using named arg", func(t *testing.T) {
			expected := []User{
				{2, "Rob", 38, true, ts},
				{3, "John", 24, false, ts},
			}
			q := th.fmt(`SELECT * FROM %s WHERE id IN (:ids)`)
			var users []User
			arg := map[string]any{"ids": []int{2, 3}}
			err = base.query(ctx, conn.db, q, arg).Scan(&users)
			require.NoError(t, err)
			assert.Equal(t, 2, len(users))
			assert.Equal(t, expected, users)
		})

		t.Run("query should return length 0 if no result is found", func(t *testing.T) {
			q := th.fmt(`SELECT * FROM %s WHERE id = 42`)
			var users []User
			err = base.query(ctx, conn.db, q).Scan(&users)
			require.NoError(t, err)
			assert.Equal(t, 0, len(users))
		})
	})
}

func TestBase_queryRow(t *testing.T) {
	runConn(t, func(t *testing.T, conn *Conn) {
		base := newBase(&config{bind: conn.bind})
		th := newTableHelper(t, conn.db, conn.bind)

		_, err := conn.db.Exec(th.fmt(`
			CREATE TABLE IF NOT EXISTS %s (
				id INT PRIMARY KEY,
				username VARCHAR(255),
				age INT,
				active BOOL,
				created_at TIMESTAMP
			)`,
		))
		require.NoError(t, err)

		ts := time.Now().UTC().Truncate(time.Second)

		q := th.fmt(`
		INSERT INTO %s (id, username, age, active, created_at)
		VALUES (?,?,?,?,?),(?,?,?,?,?),(?,?,?,?,?)`)

		_, err = conn.db.Exec(q,
			1, "Alice", 18, true, ts,
			2, "Rob", 38, true, ts,
			3, "John", 24, false, ts,
		)
		require.NoError(t, err)

		type User struct {
			Id       int       `db:"id"`
			Username string    `db:"username"`
			Age      int       `db:"age"`
			Active   bool      `db:"active"`
			Created  time.Time `db:"created_at"`
		}

		t.Run("queryRow without args should perform a regular query", func(t *testing.T) {
			expected := User{1, "Alice", 18, true, ts}
			var user User
			q := th.fmt("SELECT * FROM %s LIMIT 1")
			err = base.queryRow(ctx, conn.db, q).Scan(&user)
			require.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("queryRow should work with multiple default placeholders", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			q := th.fmt(`SELECT * FROM %s WHERE id = ? AND active = ?`)
			var user User
			err = base.queryRow(ctx, conn.db, q, 2, true).Scan(&user)
			require.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("queryRow should parse IN clause using default placeholder", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			q := th.fmt(`SELECT * FROM %s WHERE id IN (?)`)
			var user User
			ids := []int{2}
			err = base.queryRow(ctx, conn.db, q, ids).Scan(&user)
			require.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("queryRow should work with struct named arg", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			q := th.fmt(`SELECT * FROM %s WHERE id = :id`)
			var user User
			arg := struct{ Id int }{Id: 2}
			err = base.queryRow(ctx, conn.db, q, arg).Scan(&user)
			require.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("queryRow should work with map named arg", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			q := th.fmt(`SELECT * FROM %s WHERE id = :id`)
			var user User
			arg := map[string]any{"id": 2}
			err = base.queryRow(ctx, conn.db, q, arg).Scan(&user)
			require.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("queryRow should parse IN clause using named arg", func(t *testing.T) {
			expected := User{2, "Rob", 38, true, ts}
			q := th.fmt(`SELECT * FROM %s WHERE id IN (:ids)`)
			var user User
			arg := map[string]any{"ids": []int{2}}
			err = base.queryRow(ctx, conn.db, q, arg).Scan(&user)
			require.NoError(t, err)
			assert.Equal(t, expected, user)
		})

		t.Run("queryRow should return sql.ErrNoRows if no result", func(t *testing.T) {
			q := th.fmt(`SELECT * FROM %s WHERE id = 42`)
			var user User
			err = base.queryRow(ctx, conn.db, q).Scan(&user)
			require.Error(t, err)
			require.ErrorIs(t, err, sql.ErrNoRows)
		})

		t.Run("queryRow should return correct error if value is null", func(t *testing.T) {
			q := th.fmt(`INSERT INTO %s (id, username, age, active, created_at) VALUES (?,?,?,?,?)`)
			_, err = conn.db.Exec(q, 100, nil, 18, true, ts)
			require.NoError(t, err)

			q = th.fmt(`SELECT * FROM %s WHERE id = 100`)
			var user User
			err = base.queryRow(ctx, conn.db, q).Scan(&user)
			require.Error(t, err)
			assert.ErrorContains(t, err, "converting NULL to string is unsupported")
		})
	})
}

func TestBase_exec(t *testing.T) {
	runConn(t, func(t *testing.T, conn *Conn) {
		base := newBase(&config{bind: conn.bind})
		th := newTableHelper(t, conn.db, conn.bind)

		_, err := conn.db.Exec(th.fmt(`
			CREATE TABLE IF NOT EXISTS %s (
				id INT PRIMARY KEY,
				name VARCHAR(255),
				age INT,
				created_at TIMESTAMP
			)`,
		))
		require.NoError(t, err)

		t.Run("multiple args should perform a regular exec", func(t *testing.T) {
			q := th.fmt(`
			INSERT INTO %s (id, name, age)
			VALUES (?,?,?),(?,?,?),(?,?,?)`)

			re, err := base.exec(ctx, conn.db, q,
				1, "Alice", 18,
				2, "Rob", 38,
				3, "John", 4,
			)
			require.NoError(t, err)

			rows, err := re.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, 3, int(rows))
		})

		t.Run("1 arg struct should perform a named exec", func(t *testing.T) {
			q := th.fmt("DELETE FROM %s WHERE id = :id")
			arg := struct{ Id int }{Id: 1}
			re, err := base.exec(ctx, conn.db, q, arg)
			require.NoError(t, err)

			rows, err := re.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, 1, int(rows))
		})

		t.Run("1 arg map should perform a named exec", func(t *testing.T) {
			q := th.fmt("DELETE FROM %s WHERE id = :id")
			arg := map[string]any{"id": 2}
			re, err := base.exec(ctx, conn.db, q, arg)
			require.NoError(t, err)

			rows, err := re.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, 1, int(rows))
		})

		t.Run("1 arg int should perform a regular exec", func(t *testing.T) {
			q := th.fmt("DELETE FROM %s WHERE id = ?")
			arg := 3
			re, err := base.exec(ctx, conn.db, q, arg)
			require.NoError(t, err)

			rows, err := re.RowsAffected()
			require.NoError(t, err)
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
			q := th.fmt(`INSERT INTO %s (id, name, age, created_at) VALUES (:id, :name, :age, :created_at)`)
			re, err := base.exec(ctx, conn.db, q, args)
			require.NoError(t, err)

			rows, err := re.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, COUNT, int(rows))

			re, err = base.exec(ctx, conn.db, th.fmt("DELETE FROM %s"))
			require.NoError(t, err)

			rows, err = re.RowsAffected()
			require.NoError(t, err)
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
			q := th.fmt(`INSERT INTO %s (id, name, age, created_at) VALUES (:id, :name, :age, :created_at)`)
			re, err := base.exec(ctx, conn.db, q, args)
			require.NoError(t, err)

			rows, err := re.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, COUNT, int(rows))

			re, err = base.exec(ctx, conn.db, th.fmt("DELETE FROM %s"))
			require.NoError(t, err)

			rows, err = re.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, COUNT, int(rows))
		})

		t.Run("1 arg []map should perform a named batch insert", func(t *testing.T) {
			const COUNT = 100
			args := make([]map[string]any, COUNT)
			for i := range COUNT {
				args[i] = map[string]any{"id": i + 1, "name": "Name", "age": 20}
			}
			q := th.fmt(`INSERT INTO %s (id, name, age) VALUES (:id, :name, :age)`)
			re, err := base.exec(ctx, conn.db, q, args)
			require.NoError(t, err)

			rows, err := re.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, COUNT, int(rows))
		})

		t.Run("should be able to perform in clause using named args", func(t *testing.T) {
			args := map[string]any{"ids": []int{10, 11, 12}}
			q := th.fmt(`DELETE FROM %s WHERE id IN (:ids)`)
			re, err := base.exec(ctx, conn.db, q, args)
			require.NoError(t, err)

			rows, err := re.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, 3, int(rows))
		})

		t.Run("should be able to perform in clause using placeholder", func(t *testing.T) {
			args := []int{20, 21, 22}
			q := th.fmt(`DELETE FROM %s WHERE id IN (?)`)
			re, err := base.exec(ctx, conn.db, q, args)
			require.NoError(t, err)

			rows, err := re.RowsAffected()
			require.NoError(t, err)
			assert.Equal(t, len(args), int(rows))
		})
	})
}

func TestBase_customStructTag(t *testing.T) {
	runConn(t, func(t *testing.T, conn *Conn) {
		base := newBase(&config{bind: conn.bind, structTag: "json"})
		th := newTableHelper(t, conn.db, conn.bind)

		_, err := conn.db.Exec(th.fmt(`
			CREATE TABLE IF NOT EXISTS %s (
				id INT PRIMARY KEY,
				username VARCHAR(255),
				email VARCHAR(255),
				password VARCHAR(255),
				age INT,
				active BOOL
			)`,
		))
		require.NoError(t, err)

		q := th.fmt(`
		INSERT INTO %s (id, username, email, password, age, active)
		VALUES (?,?,?,?,?,?),(?,?,?,?,?,?),(?,?,?,?,?,?)`)

		_, err = conn.db.Exec(q,
			1, "Alice", "alice@wonderland.com", "123456", 18, true,
			2, "Rob", "rob@google.com", "123456", 38, true,
			3, "John", "john@id.com", "123456", 24, false,
		)
		require.NoError(t, err)

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
		err = base.queryRow(ctx, conn.db, th.fmt("SELECT * FROM %s LIMIT 1")).Scan(&user)
		require.NoError(t, err)
		assert.Equal(t, expected, user)
	})
}

func TestBase_nonEnglishCharacters(t *testing.T) {
	runConn(t, func(t *testing.T, conn *Conn) {
		base := newBase(&config{bind: conn.bind})
		th := newTableHelper(t, conn.db, conn.bind)

		_, err := conn.db.Exec(th.fmt(`
			CREATE TABLE IF NOT EXISTS %s (
				id INT PRIMARY KEY,
				名前 VARCHAR(255),
				email VARCHAR(255),
				password VARCHAR(255),
				age INT,
				active BOOL
			)`,
		))
		require.NoError(t, err)

		type User struct {
			Id       int
			Name     string `db:"名前"`
			Email    string
			Password string
			Age      int
			Active   bool
		}

		q := th.fmt(`
		INSERT INTO %s (id, 名前, email, password, age, active)
		VALUES (:id,:名前,:email,:password,:age,:active)`)
		args := []User{
			{1, "Alice", "alice@wonderland.com", "123456", 18, true},
			{2, "Rob", "rob@google.com", "123456", 38, true},
			{3, "John", "john@id.com", "123456", 24, false},
		}
		_, err = base.exec(ctx, conn.db, q, args)
		require.NoError(t, err)

		expected := User{1, "Alice", "alice@wonderland.com", "123456", 18, true}
		var user User
		err = base.queryRow(ctx, conn.db, th.fmt("SELECT * FROM %s LIMIT 1")).Scan(&user)
		require.NoError(t, err)
		assert.Equal(t, expected, user)
	})
}

type Email string

// Value implements [driver.Valuer].
func (p Email) Value() (driver.Value, error) {
	if !strings.ContainsRune(string(p), '@') {
		return driver.Value(""), fmt.Errorf("'%s' is not a valid email", p)
	}
	return driver.Value(string(p)), nil
}

func TestBase_valuerInterface(t *testing.T) {
	runConn(t, func(t *testing.T, conn *Conn) {
		base := newBase(&config{bind: conn.bind})
		th := newTableHelper(t, conn.db, conn.bind)
		_, err := conn.db.Exec(th.fmt(`
			CREATE TABLE IF NOT EXISTS %s (name VARCHAR(255), email VARCHAR(255))`,
		))
		require.NoError(t, err)

		type T struct {
			Name  string
			Email Email
		}

		query := th.fmt("INSERT INTO %s (name, email) VALUES (:name, :email)")

		data := T{Name: "Alice", Email: "alice@wonderland.com"}
		_, err = base.exec(ctx, conn.db, query, data)
		require.NoError(t, err)

		data.Email = "aliceatwonderland.com"
		_, err = base.exec(ctx, conn.db, query, data)
		require.Error(t, err)
		assert.ErrorContains(t, err, "not a valid email")
	})
}

// BenchmarkBatchInsertStruct-12    	     210	   5568681 ns/op	  389638 B/op	    3042 allocs/op
func BenchmarkBatchInsertStruct(b *testing.B) {
	conn := mysqlConn
	base := newBase(&config{bind: conn.bind})
	th := newTableHelper(b, conn.db, conn.bind)

	_, err := conn.db.Exec(th.fmt(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY AUTO_INCREMENT,
			username VARCHAR(255) NOT NULL,
			email VARCHAR(255),
			password VARCHAR(255),
			age INT
		)`,
	))
	require.NoError(b, err)

	type user struct {
		Username string
		Email    string
		Password string
		Age      int
	}
	var args []user
	for range 1000 {
		args = append(args, user{"john", "john@id.com", "doom", 42})
	}
	input := th.fmt(`INSERT INTO %s (username, email, password, age)
		VALUES (:username, :email, :password, :age)`)

	for b.Loop() {
		_, err := base.exec(ctx, conn.db, input, args)
		require.NoError(b, err)
	}
}
