package reflectutil

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tWalkStruct(key string, rval reflect.Value) StructValue {
	dotNotation := strings.ContainsRune(key, '.')
	return walkStruct("json", StructValue{Value: rval}, func(path []string) bool {
		if !dotNotation {
			s := path[len(path)-1]
			return key == s || key == strings.ToLower(s)
		}

		keys := strings.Split(key, ".")
		if len(keys) != len(path) {
			return false
		}
		for i := range len(keys) {
			if keys[i] != path[i] && keys[i] != strings.ToLower(path[i]) {
				return false
			}
		}
		return true
	})
}

func TestWalkStruct(t *testing.T) {
	type Work struct {
		Company  string `json:",omitempty"`
		JobTitle string `json:"job_title,omitempty"`
		Salary   *float64
	}

	type User struct {
		Id        int    `json:",omitempty"`
		Username  string `json:",omitempty"`
		Password  string
		Active    bool      `json:",omitempty"`
		Work      *Work     `json:",omitempty"`
		CreatedAt time.Time `json:"created_at,omitzero"`
	}

	t.Run("without dot notation", func(t *testing.T) {
		columnsIndex := map[string][]int{
			"id":         {0},
			"username":   {1},
			"password":   {2},
			"active":     {3},
			"company":    {4, 0},
			"job_title":  {4, 1},
			"salary":     {4, 2},
			"created_at": {5},
		}

		var user User
		for col, idx := range columnsIndex {
			rval := Deref(reflect.ValueOf(&user))
			v := tWalkStruct(col, rval)

			require.Equal(t, true, v.IsValid(), "Field '%s' not found", col)
			require.Equal(t, idx, v.index)
		}
	})

	t.Run("with dot notation", func(t *testing.T) {
		columnsIndex := map[string][]int{
			"id":             {0},
			"username":       {1},
			"password":       {2},
			"active":         {3},
			"work.company":   {4, 0},
			"work.job_title": {4, 1},
			"work.salary":    {4, 2},
			"created_at":     {5},
		}

		var user User
		for col, idx := range columnsIndex {
			rval := Deref(reflect.ValueOf(&user))
			v := tWalkStruct(col, rval)

			require.Equal(t, true, v.IsValid(), "Field '%s' not found", col)
			require.Equal(t, idx, v.index)
		}
	})
}

func TestWalkStruct_Embed(t *testing.T) {
	type Work struct {
		Company  string
		JobTitle string
		Salary   *float64
	}

	type User struct {
		Id       int
		Username string
		Password string
		Active   bool
		*Work
		CreatedAt time.Time
	}

	t.Run("without dot notation", func(t *testing.T) {
		columnsIndex := map[string][]int{
			"id":        {0},
			"username":  {1},
			"password":  {2},
			"active":    {3},
			"company":   {4, 0},
			"jobtitle":  {4, 1},
			"salary":    {4, 2},
			"createdat": {5},
		}

		var user User
		for col, idx := range columnsIndex {
			rval := Deref(reflect.ValueOf(&user))
			v := tWalkStruct(col, rval)

			require.Equal(t, true, v.IsValid(), "Field '%s' not found", col)
			require.Equal(t, idx, v.index)
		}
	})

	t.Run("with dot notation", func(t *testing.T) {
		columnsIndex := map[string][]int{
			"id":            {0},
			"username":      {1},
			"password":      {2},
			"active":        {3},
			"work.company":  {4, 0},
			"work.jobtitle": {4, 1},
			"work.salary":   {4, 2},
			"createdat":     {5},
		}

		var user User
		for col, idx := range columnsIndex {
			rval := Deref(reflect.ValueOf(&user))
			v := tWalkStruct(col, rval)

			require.Equal(t, true, v.IsValid(), "Field '%s' not found", col)
			require.Equal(t, idx, v.index)
		}
	})
}

func TestStructMapper_FieldByKey(t *testing.T) {
	type Inner struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}

	type Outer struct {
		Inner      Inner
		Ptr        *Inner
		Exported   string
		unexported string
		Tagged     string `db:"custom_tag"`
	}

	sm := NewStructMapper("db", nil)

	t.Run("by struct field name", func(t *testing.T) {
		o := Outer{Exported: "foo"}
		v := sm.FieldByKey("Exported", reflect.ValueOf(&o))
		require.True(t, v.IsValid())
		assert.Equal(t, "foo", v.String())
	})

	t.Run("by tag name", func(t *testing.T) {
		o := Outer{Tagged: "bar"}
		v := sm.FieldByKey("custom_tag", reflect.ValueOf(&o))
		require.True(t, v.IsValid())
		assert.Equal(t, "bar", v.String())
	})

	t.Run("case insensitive mapper", func(t *testing.T) {
		sm2 := NewStructMapper("db", func(s string) string { return strings.ToUpper(s) })
		o := Outer{Exported: "baz"}
		v := sm2.FieldByKey("EXPORTED", reflect.ValueOf(&o))
		require.True(t, v.IsValid())
		assert.Equal(t, "baz", v.String())
	})

	t.Run("nested struct dot notation", func(t *testing.T) {
		o := Outer{Inner: Inner{ID: 42}}
		v := sm.FieldByKey("inner.id", reflect.ValueOf(&o))
		require.True(t, v.IsValid())
		assert.Equal(t, 42, int(v.Int()))
	})

	t.Run("pointer to struct auto init", func(t *testing.T) {
		o := Outer{}
		v := sm.FieldByKey("ptr.name", reflect.ValueOf(&o))
		require.True(t, v.IsValid())
		v.SetString("init-name")
		assert.NotNil(t, o.Ptr)
		assert.Equal(t, "init-name", o.Ptr.Name)
	})

	t.Run("unexported field skipped", func(t *testing.T) {
		o := Outer{unexported: "foo"}
		v := sm.FieldByKey("unexported", reflect.ValueOf(&o))
		assert.False(t, v.IsValid())
	})

	t.Run("not found returns zero reflect.Value", func(t *testing.T) {
		o := Outer{}
		v := sm.FieldByKey("does_not_exist", reflect.ValueOf(&o))
		assert.False(t, v.IsValid())
	})

	t.Run("cache is used", func(t *testing.T) {
		o := Outer{Tagged: "cached"}
		v1 := sm.FieldByKey("custom_tag", reflect.ValueOf(&o))
		v2 := sm.FieldByKey("custom_tag", reflect.ValueOf(&o))
		assert.Equal(t, v1, v2)
	})
}

func TestFieldName(t *testing.T) {
	type Sample struct {
		NoTag      string
		WithTag    string `db:"colname"`
		WithOmit   string `db:"colname,omitempty"`
		WithIgnore string `db:"-"`
	}

	rtype := reflect.TypeOf(Sample{})

	t.Run("no tag falls back to name", func(t *testing.T) {
		f, _ := rtype.FieldByName("NoTag")
		assert.Equal(t, "NoTag", FieldName(f, "db"))
	})

	t.Run("uses tag value", func(t *testing.T) {
		f, _ := rtype.FieldByName("WithTag")
		assert.Equal(t, "colname", FieldName(f, "db"))
	})

	t.Run("tag with omitempty stripped", func(t *testing.T) {
		f, _ := rtype.FieldByName("WithOmit")
		assert.Equal(t, "colname", FieldName(f, "db"))
	})

	t.Run("tag with dash falls back to name", func(t *testing.T) {
		f, _ := rtype.FieldByName("WithIgnore")
		assert.Equal(t, "WithIgnore", FieldName(f, "db"))
	})
}

// BenchmarkStructMapper_FieldByKey-12    	  440548	      2789 ns/op	     264 B/op	       8 allocs/op
func BenchmarkStructMapper_FieldByKey(b *testing.B) {
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

	columns := []string{
		"id",
		"username",
		"password",
		"name",
		"surname",
		"age",
		"active",
		"createdat",
	}
	sm := NewStructMapper("json", nil)

	for b.Loop() {
		var user User
		for _, col := range columns {
			v := sm.FieldByKey(col, reflect.ValueOf(&user))
			require.Equal(b, true, v.IsValid(), "Field: %s", col)
		}
	}
}
