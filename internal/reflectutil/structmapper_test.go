package reflectutil

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rfberaldo/sqlz/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tWalkStruct(key string, rval reflect.Value) (reflect.Value, []int) {
	return walkStruct("json", rval, func(s string) bool {
		return strings.ToLower(s) == key
	}, []int{})
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

	t.Run("empty struct", func(t *testing.T) {
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
			rval := DerefValue(reflect.ValueOf(&user))
			v, i := tWalkStruct(col, rval)

			require.Equal(t, true, v.IsValid())
			require.Equal(t, idx, i)
		}
	})

	t.Run("filled struct", func(t *testing.T) {
		ts := time.Now()

		fields := []struct {
			key   string
			index []int
			value any
		}{
			{
				key:   "id",
				index: []int{0},
				value: 1,
			},
			{
				key:   "username",
				index: []int{1},
				value: "bob",
			},
			{
				key:   "password",
				index: []int{2},
				value: "123456",
			},
			{
				key:   "active",
				index: []int{3},
				value: true,
			},
			{
				key:   "company",
				index: []int{4, 0},
				value: "Gitgood",
			},
			{
				key:   "job_title",
				index: []int{4, 1},
				value: "Top Worker",
			},
			{
				key:   "salary",
				index: []int{4, 2},
				value: 42069.42,
			},
			{
				key:   "created_at",
				index: []int{5},
				value: ts,
			},
		}

		user := &User{
			Work: &Work{
				Company:  "Gitgood",
				JobTitle: "Top Worker",
				Salary:   testutil.PtrTo(42069.42),
			},
			Id:        1,
			Username:  "bob",
			Password:  "123456",
			Active:    true,
			CreatedAt: ts,
		}

		for _, field := range fields {
			rval := DerefValue(reflect.ValueOf(&user))
			v, i := tWalkStruct(field.key, rval)

			require.Equal(t, true, v.IsValid())
			assert.Equal(t, field.index, i)
		}

		t.Run("should update from reflect.Value", func(t *testing.T) {
			rval := DerefValue(reflect.ValueOf(&user))
			v, _ := tWalkStruct("job_title", rval)
			require.Equal(t, true, v.IsValid())
			v.Set(reflect.ValueOf("Peon"))
			assert.Equal(t, "Peon", user.Work.JobTitle)
		})
	})
}

func TestWalkStruct_Embed(t *testing.T) {
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

	columnsIndex := map[string][]int{
		"id":        {1},
		"username":  {2},
		"password":  {3},
		"name":      {0, 0},
		"surname":   {0, 1},
		"age":       {0, 2},
		"active":    {4},
		"createdat": {5},
	}

	var user User
	for col, idx := range columnsIndex {
		rval := DerefValue(reflect.ValueOf(&user))
		v, i := tWalkStruct(col, rval)

		require.Equal(t, true, v.IsValid())
		require.Equal(t, idx, i)
	}
}

// BenchmarkFieldByTagName-12    	  237490	      4783 ns/op	    1296 B/op	      57 allocs/op
// BenchmarkFieldByTagName-12    	  295118	      3888 ns/op	     240 B/op	      13 allocs/op
// BenchmarkFieldByTagName-12    	  440548	      2789 ns/op	     264 B/op	       8 allocs/op
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
	stv := NewStructMapper("json", nil)

	for b.Loop() {
		var user User
		for _, col := range columns {
			v := stv.FieldByTagName(col, reflect.ValueOf(&user))
			require.Equal(b, true, v.IsValid())
		}
	}
}
