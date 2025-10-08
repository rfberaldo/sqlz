package sqlz

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetMapValue(t *testing.T) {
	data := map[string]any{
		"id":   42,
		"name": "Alice",
		"meta": map[string]any{
			"age":  30,
			"info": map[string]any{"country": "BR"},
		},
		"email": "not a map",
	}

	t.Run("simple key found", func(t *testing.T) {
		v, ok := getMapValue("id", data)
		assert.True(t, ok)
		assert.Equal(t, 42, v)
	})

	t.Run("simple key not found", func(t *testing.T) {
		v, ok := getMapValue("missing", data)
		assert.False(t, ok)
		assert.Nil(t, v)
	})

	t.Run("nested key one level", func(t *testing.T) {
		v, ok := getMapValue("meta.age", data)
		assert.True(t, ok)
		assert.Equal(t, 30, v)
	})

	t.Run("nested key two levels", func(t *testing.T) {
		v, ok := getMapValue("meta.info.country", data)
		assert.True(t, ok)
		assert.Equal(t, "BR", v)
	})

	t.Run("nested key not found", func(t *testing.T) {
		v, ok := getMapValue("meta.unknown", data)
		assert.False(t, ok)
		assert.Nil(t, v)
	})

	t.Run("intermediate not a map", func(t *testing.T) {
		v, ok := getMapValue("email.something", data)
		assert.False(t, ok)
		assert.Nil(t, v)
	})

	t.Run("deeply nested missing branch", func(t *testing.T) {
		v, ok := getMapValue("meta.info.city.name", data)
		assert.False(t, ok)
		assert.Nil(t, v)
	})

	t.Run("nested root missing", func(t *testing.T) {
		v, ok := getMapValue("unknown.key", data)
		assert.False(t, ok)
		assert.Nil(t, v)
	})
}

func TestSnakeCaseMapper(t *testing.T) {
	got := SnakeCaseMapper("Id")
	assert.Equal(t, "id", got)

	got = SnakeCaseMapper("ID")
	assert.Equal(t, "id", got)

	got = SnakeCaseMapper("UserID")
	assert.Equal(t, "user_id", got)

	got = SnakeCaseMapper("CreatedAt")
	assert.Equal(t, "created_at", got)

	got = SnakeCaseMapper("Created_at")
	assert.Equal(t, "created_at", got)

	got = SnakeCaseMapper("Created_At")
	assert.Equal(t, "created_at", got)

	got = SnakeCaseMapper("_createdAt")
	assert.Equal(t, "_created_at", got)

	got = SnakeCaseMapper("createdAt_")
	assert.Equal(t, "created_at_", got)

	got = SnakeCaseMapper("__createdAt")
	assert.Equal(t, "__created_at", got)

	got = SnakeCaseMapper("createdAt__")
	assert.Equal(t, "created_at__", got)

	got = SnakeCaseMapper("Created42At")
	assert.Equal(t, "created42_at", got)

	got = SnakeCaseMapper("あcreated42At")
	assert.Equal(t, "あcreated42_at", got)

	got = SnakeCaseMapper("Createdあ42At")
	assert.Equal(t, "createdあ42_at", got)
}
