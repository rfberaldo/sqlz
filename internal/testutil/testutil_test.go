package testutil

import (
	"testing"

	"github.com/rfberaldo/sqlz/internal/parser"
	"github.com/stretchr/testify/assert"
)

func TestQuestionToDollar(t *testing.T) {
	input := "SELECT * FROM user WHERE id = ? and name = ?"
	expected := "SELECT * FROM user WHERE id = $1 and name = $2"
	got := QuestionToDollar(input)
	assert.Equal(t, expected, got)
}

func TestToSnakeCase(t *testing.T) {
	input := "TestOneArgMapShouldPerformABatchInsert/MySQL"
	expect := "test_one_arg_map_should_perform_a_batch_insert"
	got := slugify(input)
	assert.Equal(t, got, expect)
}

func TestRebind(t *testing.T) {
	input := "SELECT * FROM user WHERE id = ? AND age = ?"
	expected := "SELECT * FROM user WHERE id = $1 AND age = $2"
	got := rebind(parser.BindDollar, input)
	assert.Equal(t, expected, got)
}
