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

func TestToSnakeCase(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{name: "empty", input: "", expect: ""},
		{name: "lowercase", input: "user", expect: "user"},
		{name: "single uppercase", input: "A", expect: "a"},
		{name: "two uppercase", input: "ID", expect: "id"},
		{name: "pascal case", input: "UserName", expect: "user_name"},
		{name: "camel case", input: "userName", expect: "user_name"},
		{name: "acronym start", input: "HTTPServer", expect: "http_server"},
		{name: "acronym end", input: "GetHTTP", expect: "get_http"},
		{name: "acronym middle", input: "HTTPStatusCode", expect: "http_status_code"},
		{name: "acronym surrounded", input: "XMLHTTPRequest", expect: "xmlhttp_request"},
		{name: "mixed", input: "UserIDNumber", expect: "user_id_number"},
		{name: "single letter prefix", input: "XValue", expect: "x_value"},
		{name: "multi caps boundary", input: "MyURLParser", expect: "my_url_parser"},
		{name: "digit inside", input: "JSON2XMLData", expect: "json2_xml_data"},
		{name: "digit end", input: "User2", expect: "user2"},
		{name: "digit start", input: "2User", expect: "2_user"},
		{name: "already snake", input: "already_snake_case", expect: "already_snake_case"},
		{name: "mixed with snake", input: "User_Name", expect: "user_name"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ToSnakeCase(tc.input)
			assert.Equal(t, tc.expect, got)
		})
	}
}
