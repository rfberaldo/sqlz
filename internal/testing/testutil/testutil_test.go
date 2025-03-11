package testutil

import (
	"testing"

	"github.com/rafaberaldo/sqlz/internal/testing/assert"
)

func TestQuestionToDollar(t *testing.T) {
	input := "SELECT * FROM user WHERE id = ? and name = ?"
	expected := "SELECT * FROM user WHERE id = $1 and name = $2"
	got := QuestionToDollar(input)
	assert.Equal(t, "should replace ? with $", got, expected)
}
