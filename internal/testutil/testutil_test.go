package testutil

import (
	"testing"

	"github.com/rfberaldo/sqlz/binds"
	"github.com/stretchr/testify/assert"
)

func TestQuestionToDollar(t *testing.T) {
	input := "SELECT * FROM user WHERE id = ? and name = ?"
	expected := "SELECT * FROM user WHERE id = $1 and name = $2"
	got := QuestionToDollar(input)
	assert.Equal(t, expected, got)
}

func TestTableName(t *testing.T) {
	input := "TestOneArgMapShouldPerformABatchInsert/MySQL"
	contains := "TestOneArgMapShouldPerformABatchInsert"
	got := TableName(input)
	assert.Contains(t, got, contains)
}

func TestRebind(t *testing.T) {
	input := "SELECT * FROM user WHERE id = ? AND age = ?"
	expected := "SELECT * FROM user WHERE id = $1 AND age = $2"
	got := Rebind(binds.Dollar, input)
	assert.Equal(t, expected, got)
}
