package reflectutil

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStructFieldMap(t *testing.T) {
	type Person struct {
		Id         int    `json:",omitempty"`
		Name       string `json:",omitempty"`
		unexported string
	}

	type User struct {
		*Person
		UserId    int       `json:"user_id,omitempty"`
		Username  string    `json:",omitempty"`
		Active    bool      `json:",omitempty"`
		Parent    *Person   `json:",omitempty"`
		CreatedAt time.Time `json:"created_at,omitzero"`
	}

	expect := map[string][]int{
		"person.id":   {0, 0},
		"person.name": {0, 1},
		"id":          {0, 0},
		"name":        {0, 1},
		"user_id":     {1},
		"username":    {2},
		"active":      {3},
		"parent":      {4},
		"parent.id":   {4, 0},
		"parent.name": {4, 1},
		"created_at":  {5},
	}

	got := StructFieldMap(reflect.TypeFor[User](), "json", strings.ToLower)
	assert.Equal(t, expect, got)
}

func TestStructFieldMap_circular(t *testing.T) {
	type Person struct {
		Parent *Person
	}

	expect := make(map[string][]int)
	for i := range 255 {
		key := "parent"
		idx := []int{0}
		for range i {
			idx = append(idx, 0)
			key += ".parent"
		}
		expect[key] = idx
	}

	got := StructFieldMap(reflect.TypeFor[Person](), "json", strings.ToLower)
	assert.Equal(t, 255, len(got))
	assert.Equal(t, expect, got)
}

func TestFieldByIndex(t *testing.T) {
	type Person struct {
		Id         int
		Name       string
		DeepParent *Person
	}

	type User struct {
		*Person
		UserId    int
		Username  string
		Active    bool
		Parent    Person
		CreatedAt time.Time
	}

	t.Run("top level non nil", func(t *testing.T) {
		var user User
		got := FieldByIndex(reflect.ValueOf(&user), []int{1})
		assert.True(t, got.IsValid())
		assert.True(t, got.CanAddr())

		got.SetInt(69)
		assert.Equal(t, user.UserId, 69)
	})

	t.Run("nested non nil", func(t *testing.T) {
		var user User
		got := FieldByIndex(reflect.ValueOf(&user), []int{4, 0})
		assert.True(t, got.IsValid())
		assert.True(t, got.CanAddr())

		got.SetInt(69)
		assert.Equal(t, user.Parent.Id, 69)
	})

	t.Run("deep nested nil", func(t *testing.T) {
		var user User
		got := FieldByIndex(reflect.ValueOf(&user), []int{0, 2, 1})
		assert.True(t, got.IsValid())
		assert.True(t, got.CanAddr())

		got.SetString("foo")
		assert.Equal(t, user.DeepParent.Name, "foo")
	})
}

func TestFieldTag(t *testing.T) {
	type Sample struct {
		NoTag      string
		WithTag    string `db:"colname"`
		WithOmit   string `db:"omitme,omitempty"`
		WithIgnore string `db:"-"`
		EmptyTag   string `db:""`
	}

	typ := reflect.TypeOf(Sample{})

	t.Run("tag not found", func(t *testing.T) {
		f, _ := typ.FieldByName("NoTag")
		tag, ok := FieldTag(f, "db")
		assert.False(t, ok)
		assert.Empty(t, tag)
	})

	t.Run("tag found", func(t *testing.T) {
		f, _ := typ.FieldByName("WithTag")
		tag, ok := FieldTag(f, "db")
		assert.True(t, ok)
		assert.Equal(t, "colname", tag)
	})

	t.Run("tag with omitempty", func(t *testing.T) {
		f, _ := typ.FieldByName("WithOmit")
		tag, ok := FieldTag(f, "db")
		assert.True(t, ok)
		assert.Equal(t, "omitme", tag)
	})

	t.Run("tag with dash", func(t *testing.T) {
		f, _ := typ.FieldByName("WithIgnore")
		tag, ok := FieldTag(f, "db")
		assert.False(t, ok)
		assert.Empty(t, tag)
	})

	t.Run("tag empty string", func(t *testing.T) {
		f, _ := typ.FieldByName("EmptyTag")
		tag, ok := FieldTag(f, "db")
		assert.False(t, ok)
		assert.Empty(t, tag)
	})
}

// BenchmarkStructFieldMap-12    	  493293	      2143 ns/op	    1928 B/op	      37 allocs/op
func BenchmarkStructFieldMap(b *testing.B) {
	type Person struct {
		Name    string
		Surname string
		Age     int
	}

	type User struct {
		*Person
		Id        int
		Username  string
		Password  string
		Active    bool
		CreatedAt time.Time
	}

	for b.Loop() {
		var user User
		_ = StructFieldMap(reflect.TypeOf(user), "json", strings.ToLower)
	}
}
