package reflectutil

import (
	"reflect"
	"testing"
	"time"

	"github.com/rfberaldo/sqlz/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestWalkStruct(t *testing.T) {
	type Work struct {
		JobTitle string `json:"job_title,omitempty"`
	}

	type User struct {
		Username string `json:",omitempty"`
		Age      *int   `json:",omitempty"`
		Work     *Work
	}

	user := &User{
		Username: "bob",
		Age:      testutil.PtrTo(42),
		Work: &Work{
			JobTitle: "Worker",
		},
	}
	rval := DerefValue(reflect.ValueOf(&user))

	t.Run("should find top field", func(t *testing.T) {
		v, i := walkStruct("json", &rval, func(s string) bool {
			return s == "Username"
		}, []int{})
		require.Equal(t, true, v.IsValid())
		require.Equal(t, "bob", v.Interface())
		require.Equal(t, []int{0}, i)

		v, i = walkStruct("json", &rval, func(s string) bool {
			return s == "Age"
		}, []int{})
		require.Equal(t, true, v.IsValid())
		require.Equal(t, 42, v.Elem().Interface())
		require.Equal(t, []int{1}, i)
	})

	t.Run("should find nested field", func(t *testing.T) {
		v, i := walkStruct("json", &rval, func(s string) bool {
			return s == "job_title"
		}, []int{})
		require.Equal(t, true, v.IsValid())
		require.Equal(t, "Worker", v.Interface())
		require.Equal(t, []int{2, 0}, i)
	})
}

func TestWalkStruct_Embed(t *testing.T) {
	type Person struct {
		Name string
		Age  int
	}

	type User struct {
		*Person
		Id       *int
		Username string
	}

	t.Run("should find top field", func(t *testing.T) {
		user := &User{Username: "bob"}
		rval := DerefValue(reflect.ValueOf(&user))

		v, i := walkStruct("json", &rval, func(s string) bool {
			return s == "Username"
		}, []int{})
		require.Equal(t, true, v.IsValid())
		require.Equal(t, "bob", v.Interface())
		require.Equal(t, []int{2}, i)
	})

	t.Run("should find nested field", func(t *testing.T) {
		user := &User{
			Person: &Person{
				Age: 42,
			},
		}
		rval := DerefValue(reflect.ValueOf(&user))

		v, i := walkStruct("json", &rval, func(s string) bool {
			return s == "Age"
		}, []int{})
		require.Equal(t, true, v.IsValid())
		require.Equal(t, 42, v.Interface())
		require.Equal(t, []int{0, 1}, i)
	})

	t.Run("should find nil field", func(t *testing.T) {
		var user User
		rval := DerefValue(reflect.ValueOf(&user))
		v, i := walkStruct("json", &rval, func(s string) bool {
			return s == "Id"
		}, []int{})
		require.Equal(t, true, v.IsValid())
		require.Equal(t, []int{1}, i)
	})

	t.Run("should find nil-nested field", func(t *testing.T) {
		var user User
		rval := DerefValue(reflect.ValueOf(&user))
		v, i := walkStruct("json", &rval, func(s string) bool {
			return s == "Name"
		}, []int{})
		require.Equal(t, true, v.IsValid())
		require.Equal(t, []int{0, 0}, i)
	})
}

// BenchmarkFieldByTagName-12    	  237490	      4783 ns/op	    1296 B/op	      57 allocs/op
func BenchmarkFieldByTagName(b *testing.B) {
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

	sv := NewStructValue("json", func(s string) string { return s })
	columns := []string{
		"Id",
		"Username",
		"Password",
		"Name",
		"Surname",
		"Age",
		"Active",
		"CreatedAt",
	}

	for b.Loop() {
		var user User
		rval := DerefValue(reflect.ValueOf(&user))

		for _, col := range columns {
			v := sv.FieldByTagName(col, &rval)
			require.Equal(b, true, v.IsValid())
		}
	}
}
