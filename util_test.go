package sqlz

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/rfberaldo/sqlz/sqlogger"
	"github.com/stretchr/testify/assert"

	_ "github.com/mattn/go-sqlite3"
)

func TestNew(t *testing.T) {
	db := New("sqlite3", &sql.DB{}, nil)
	assert.NotNil(t, db)
	assert.IsType(t, &DB{}, db)
	assert.Equal(t, "db", db.scanner.StructTagKey())

	db = New("sqlite3", &sql.DB{}, &Options{})
	assert.NotNil(t, db)
	assert.IsType(t, &DB{}, db)
	assert.Equal(t, "db", db.scanner.StructTagKey())

	db = New("sqlite3", &sql.DB{}, &Options{StructTag: "json"})
	assert.NotNil(t, db)
	assert.IsType(t, &DB{}, db)
	assert.Equal(t, "json", db.scanner.StructTagKey())
}

func TestNew_panic(t *testing.T) {
	defer func() {
		assert.Contains(t, recover(), "unable to find bind")
	}()

	New("wrongdriver", &sql.DB{}, nil)
}

func TestConnect(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		db, err := Connect("sqlite3", ":memory:", nil)
		assert.NoError(t, err)
		assert.NotNil(t, db)
		assert.IsType(t, &DB{}, db)
	})

	t.Run("error", func(t *testing.T) {
		_, err := Connect("wrongdriver", ":memory:", nil)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "unknown driver")
	})

	t.Run("with options", func(t *testing.T) {
		debugLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
		_, err := Connect("sqlite3", ":memory:", &Options{
			Logger:        debugLogger,
			LoggerOptions: sqlogger.Options{},
		})
		assert.NoError(t, err)
	})
}

func TestMustConnect(t *testing.T) {
	db := MustConnect("sqlite3", ":memory:", nil)
	assert.NotNil(t, db)
	assert.IsType(t, &DB{}, db)
}

func TestNotFound(t *testing.T) {
	err := errors.New("some custom error")
	assert.Equal(t, false, IsNotFound(err))

	err = fmt.Errorf("some custom error")
	assert.Equal(t, false, IsNotFound(err))

	err = errors.Join(fmt.Errorf("some custom error"), sql.ErrNoRows)
	assert.Equal(t, true, IsNotFound(err))

	err = fmt.Errorf("a wrapper around sql.ErrNoRows: %w", sql.ErrNoRows)
	assert.Equal(t, true, IsNotFound(err))
}
