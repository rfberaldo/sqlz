package sqlz

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	_ "github.com/mattn/go-sqlite3"
)

func TestNew(t *testing.T) {
	db := New("sqlite3", &sql.DB{})
	assert.NotNil(t, db)
	assert.IsType(t, &DB{}, db)
}

func TestNew_panic(t *testing.T) {
	defer func() {
		assert.Contains(t, recover(), "unable to find bind")
	}()

	New("wrongdriver", &sql.DB{})
}

func TestConnect(t *testing.T) {
	db, err := Connect("sqlite3", ":memory:")
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.IsType(t, &DB{}, db)
}

func TestConnect_wrong_driver(t *testing.T) {
	_, err := Connect("wrongdriver", ":memory:")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unable to find bind")
}

func TestMustConnect(t *testing.T) {
	db := MustConnect("sqlite3", ":memory:")
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
