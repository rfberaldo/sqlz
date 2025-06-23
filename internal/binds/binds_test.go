package binds

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBindByDriver(t *testing.T) {
	got := BindByDriver("mysql")
	assert.Equal(t, Question, got)

	got = BindByDriver("pgx")
	assert.Equal(t, Dollar, got)

	got = BindByDriver("sqlserver")
	assert.Equal(t, At, got)

	got = BindByDriver("goracle")
	assert.Equal(t, Colon, got)

	got = BindByDriver("notadriver")
	assert.Equal(t, Unknown, got)
}

func TestRegister(t *testing.T) {
	const driver = "customdriver"
	got := BindByDriver(driver)
	assert.Equal(t, Unknown, got)

	Register(driver, Question)
	got = BindByDriver(driver)
	assert.Equal(t, Question, got)
}
