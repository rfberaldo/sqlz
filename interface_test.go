package sqlz_test

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/rfberaldo/sqlz"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	db := sqlz.New("sqlite3", &sql.DB{}, nil)
	assert.NotNil(t, db)
	assert.IsType(t, &sqlz.DB{}, db)
}

func TestNew_panic(t *testing.T) {
	defer func() {
		err, ok := recover().(error)
		assert.True(t, ok)
		assert.ErrorContains(t, err, "unable to find bind")
	}()

	sqlz.New("wrongdriver", &sql.DB{}, nil)
}

func TestConnect_wrong_driver(t *testing.T) {
	_, err := sqlz.Connect("wrongdriver", ":memory:")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unknown driver")
}

func TestNotFound(t *testing.T) {
	err := errors.New("some custom error")
	assert.Equal(t, false, sqlz.IsNotFound(err))

	err = fmt.Errorf("some custom error")
	assert.Equal(t, false, sqlz.IsNotFound(err))

	err = errors.Join(fmt.Errorf("some custom error"), sql.ErrNoRows)
	assert.Equal(t, true, sqlz.IsNotFound(err))

	err = fmt.Errorf("a wrapper around sql.ErrNoRows: %w", sql.ErrNoRows)
	assert.Equal(t, true, sqlz.IsNotFound(err))
}
