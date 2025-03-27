package sqlogger

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	t.Run("Without Options", func(t *testing.T) {
		db := New(&sqlite3.SQLiteDriver{}, ":memory:", tSlogger, nil)
		_, ok := any(db).(*sql.DB)
		assert.True(t, ok)

		err := db.Ping()
		assert.NoError(t, err)
		assert.Equal(t, "Ping", output.data.Msg)
		assert.Equal(t, slog.LevelDebug, output.data.Level)
	})

	t.Run("With Options", func(t *testing.T) {
		id := randomId()
		idGenerator := func() string { return id }

		db := New(&sqlite3.SQLiteDriver{}, ":memory:", tSlogger, &Options{IdGenerator: idGenerator})
		_, ok := any(db).(*sql.DB)
		assert.True(t, ok)

		err := db.Ping()
		assert.NoError(t, err)
		assert.Equal(t, "Ping", output.data.Msg)
		assert.Equal(t, slog.LevelDebug, output.data.Level)
		assert.Equal(t, id, output.data.ConnId)
	})
}

func TestOpen(t *testing.T) {
	t.Run("Without Options", func(t *testing.T) {
		db, err := Open("sqlite3", ":memory:", tSlogger, nil)
		assert.NoError(t, err)
		_, ok := any(db).(*sql.DB)
		assert.True(t, ok)

		err = db.Ping()
		assert.NoError(t, err)
		assert.Equal(t, "Ping", output.data.Msg)
		assert.Equal(t, slog.LevelDebug, output.data.Level)
	})

	t.Run("With Options", func(t *testing.T) {
		id := randomId()
		idGenerator := func() string { return id }

		db, err := Open("sqlite3", ":memory:", tSlogger, &Options{IdGenerator: idGenerator})
		assert.NoError(t, err)
		_, ok := any(db).(*sql.DB)
		assert.True(t, ok)

		err = db.Ping()
		assert.NoError(t, err)
		assert.Equal(t, "Ping", output.data.Msg)
		assert.Equal(t, slog.LevelDebug, output.data.Level)
		assert.Equal(t, id, output.data.ConnId)
	})
}

func TestCleanQuery(t *testing.T) {
	input := `
		SELECT * FROM
			user
		WHERE
			name = ?
		AND
			id IN (?,?)
		ORDER BY
			name
	`
	expected := "SELECT * FROM user WHERE name = ? AND id IN (?,?) ORDER BY name"
	assert.Equal(t, expected, cleanQuery(input))
}
