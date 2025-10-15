package reflectutil

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStructFieldMap(t *testing.T) {
	type Job struct {
		JobName string
	}

	type Person struct {
		Id         int    `json:",omitempty"`
		Name       string `json:",omitempty"`
		Job        Job
		unexported string
	}

	type User struct {
		*Person
		UserId    int       `json:"user_id,omitempty"`
		Username  string    `json:",omitempty"`
		Active    bool      `json:",omitempty"`
		Parent    *Person   `json:",omitempty"`
		CreatedAt time.Time `json:"created_at,omitzero"`
		Job
	}

	expect := map[string][]int{
		"user_id":            {1},
		"username":           {2},
		"active":             {3},
		"parent":             {4},
		"created_at":         {5},
		"id":                 {0, 0},
		"name":               {0, 1},
		"job":                {0, 2},
		"parent.id":          {4, 0},
		"parent.name":        {4, 1},
		"parent.job":         {4, 2},
		"jobname":            {6, 0},
		"job.jobname":        {0, 2, 0},
		"parent.job.jobname": {4, 2, 0},
	}

	got := StructFieldMap(reflect.TypeFor[User](), "json", ".", strings.ToLower)
	assert.Equal(t, expect, got)
}

func TestStructFieldMap_inline(t *testing.T) {
	type Person struct {
		Name string `json:"person_name"`
	}

	type User struct {
		Id     int
		Parent *Person `json:",inline"`
	}

	expect := map[string][]int{
		"id":          {0},
		"person_name": {1, 0},
	}

	got := StructFieldMap(reflect.TypeFor[User](), "json", "_", strings.ToLower)
	assert.Equal(t, expect, got)
}

func TestStructFieldMap_circular(t *testing.T) {
	type Person struct {
		Parent *Person
	}

	expect := make(map[string][]int)
	for i := range maxCircular {
		key := "parent"
		idx := []int{0}
		for range i {
			idx = append(idx, 0)
			key += ".parent"
		}
		expect[key] = idx
	}

	got := StructFieldMap(reflect.TypeFor[Person](), "json", ".", strings.ToLower)
	assert.Equal(t, maxCircular, len(got))
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
		NoTag          string
		WithTag        string `json:"colname"`
		WithOmit       string `json:"colname2,omitempty"`
		NoTagWithOmit  string `json:",omitempty"`
		WithInline     string `json:",omitempty,inline"`
		InlineEdgeCase string `json:"inline"`
		WithIgnore     string `json:"-"`
		EmptyTag       string `json:""`
	}

	type InlineEdgeCase struct {
		Field string `json:"inline,inline"`
	}

	typ := reflect.TypeOf(Sample{})

	t.Run("tag not found", func(t *testing.T) {
		f, _ := typ.FieldByName("NoTag")
		tag, inline := fieldTag(f, "json")
		assert.False(t, inline)
		assert.Empty(t, tag)
	})

	t.Run("tag found", func(t *testing.T) {
		f, _ := typ.FieldByName("WithTag")
		tag, inline := fieldTag(f, "json")
		assert.False(t, inline)
		assert.Equal(t, "colname", tag)
	})

	t.Run("tag with omitempty", func(t *testing.T) {
		f, _ := typ.FieldByName("WithOmit")
		tag, inline := fieldTag(f, "json")
		assert.False(t, inline)
		assert.Equal(t, "colname2", tag)
	})

	t.Run("tag with omitempty", func(t *testing.T) {
		f, _ := typ.FieldByName("NoTagWithOmit")
		tag, inline := fieldTag(f, "json")
		assert.False(t, inline)
		assert.Empty(t, tag)
	})

	t.Run("tag with inline", func(t *testing.T) {
		f, _ := typ.FieldByName("WithInline")
		tag, inline := fieldTag(f, "json")
		assert.True(t, inline)
		assert.Empty(t, tag)
	})

	t.Run("inline edge case 1", func(t *testing.T) {
		f, _ := typ.FieldByName("InlineEdgeCase")
		tag, inline := fieldTag(f, "json")
		assert.False(t, inline)
		assert.Equal(t, "inline", tag)
	})

	t.Run("inline edge case 2", func(t *testing.T) {
		f, _ := reflect.TypeFor[InlineEdgeCase]().FieldByName("Field")
		tag, inline := fieldTag(f, "json")
		assert.True(t, inline)
		assert.Equal(t, "inline", tag)
	})

	t.Run("tag with dash", func(t *testing.T) {
		f, _ := typ.FieldByName("WithIgnore")
		tag, inline := fieldTag(f, "json")
		assert.False(t, inline)
		assert.Empty(t, tag)
	})

	t.Run("tag empty string", func(t *testing.T) {
		f, _ := typ.FieldByName("EmptyTag")
		tag, inline := fieldTag(f, "json")
		assert.False(t, inline)
		assert.Empty(t, tag)
	})
}

// BenchmarkStructFieldMap-12    	  655912	      1621 ns/op	    1272 B/op	      38 allocs/op
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
		_ = StructFieldMap(reflect.TypeFor[User](), "json", ".", strings.ToLower)
	}
}

// BenchmarkStructFieldMap_circular-8   	   45915	     29806 ns/op	    6992 B/op	      97 allocs/op
func BenchmarkStructFieldMap_circular(b *testing.B) {
	type Person struct {
		Name   string
		Parent *Person
	}

	for b.Loop() {
		_ = StructFieldMap(reflect.TypeFor[Person](), "json", ".", strings.ToLower)
	}
}
