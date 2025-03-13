package testutil

import (
	"testing"

	"github.com/rafaberaldo/sqlz/internal/parser"
	"github.com/stretchr/testify/assert"
)

func TestQuestionToDollar(t *testing.T) {
	input := "SELECT * FROM user WHERE id = ? and name = ?"
	expected := "SELECT * FROM user WHERE id = $1 and name = $2"
	got := QuestionToDollar(input)
	assert.Equal(t, expected, got)
}

func TestTableName(t *testing.T) {
	input := "TestCore/MySQL/Core.one_arg_map_should_perform_ABatchInsert"
	expected := "core_one_arg_map_should_perform_abatchinsert"
	got := TableName(input)
	assert.Equal(t, expected, got)
}

func thisFuncName() {}

func TestFuncName(t *testing.T) {
	expected := "this_func_name"
	got := FuncName(thisFuncName)
	assert.Equal(t, expected, got)
}

func TestRebind(t *testing.T) {
	input := "SELECT * FROM user WHERE id = ? AND age = ?"
	expected := "SELECT * FROM user WHERE id = $1 AND age = $2"
	got := Rebind(parser.BindDollar, input)
	assert.Equal(t, expected, got)
}
