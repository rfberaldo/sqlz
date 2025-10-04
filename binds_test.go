package sqlz_test

import (
	"testing"

	"github.com/rfberaldo/sqlz"
	"github.com/rfberaldo/sqlz/parser"
	"github.com/stretchr/testify/assert"
)

func TestBindByDriver(t *testing.T) {
	got := sqlz.BindByDriver("mysql")
	assert.Equal(t, parser.BindQuestion, got)

	got = sqlz.BindByDriver("pgx")
	assert.Equal(t, parser.BindDollar, got)

	got = sqlz.BindByDriver("sqlserver")
	assert.Equal(t, parser.BindAt, got)

	got = sqlz.BindByDriver("goracle")
	assert.Equal(t, parser.BindColon, got)

	got = sqlz.BindByDriver("notadriver")
	assert.Equal(t, parser.BindUnknown, got)
}

func TestRegister(t *testing.T) {
	const driver = "customdriver"
	got := sqlz.BindByDriver(driver)
	assert.Equal(t, parser.BindUnknown, got)

	sqlz.Register(driver, parser.BindQuestion)
	got = sqlz.BindByDriver(driver)
	assert.Equal(t, parser.BindQuestion, got)
}
