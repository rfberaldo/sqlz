package reflectutil

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTypeOfAny(t *testing.T) {
	t.Run("map", func(t *testing.T) {
		var v map[string]any
		assert.Equal(t, Map, TypeOfAny(v))
	})

	t.Run("pointer to map", func(t *testing.T) {
		var v *map[string]any
		assert.Equal(t, Map, TypeOfAny(v))
	})

	t.Run("struct", func(t *testing.T) {
		var v struct{}
		assert.Equal(t, Struct, TypeOfAny(v))
	})

	t.Run("pointer to struct", func(t *testing.T) {
		var v *struct{}
		assert.Equal(t, Struct, TypeOfAny(v))
	})

	t.Run("primitive", func(t *testing.T) {
		var v string
		assert.Equal(t, Primitive, TypeOfAny(v))
	})

	t.Run("pointer to primitive", func(t *testing.T) {
		var v *string
		assert.Equal(t, Primitive, TypeOfAny(v))
	})

	t.Run("slice struct", func(t *testing.T) {
		var v []struct{}
		assert.Equal(t, SliceStruct, TypeOfAny(v))
	})

	t.Run("slice pointer to struct", func(t *testing.T) {
		var v []*struct{}
		assert.Equal(t, SliceStruct, TypeOfAny(v))
	})

	t.Run("slice map", func(t *testing.T) {
		var v []map[string]any
		assert.Equal(t, SliceMap, TypeOfAny(v))
	})

	t.Run("slice pointer to map", func(t *testing.T) {
		var v []*map[string]any
		assert.Equal(t, SliceMap, TypeOfAny(v))
	})

	t.Run("slice primitive", func(t *testing.T) {
		var v []string
		assert.Equal(t, SlicePrimitive, TypeOfAny(v))
	})

	t.Run("slice pointer to primitive", func(t *testing.T) {
		var v []*string
		assert.Equal(t, SlicePrimitive, TypeOfAny(v))
	})

	t.Run("slice of slice primitive", func(t *testing.T) {
		var v [][]string
		assert.Equal(t, SlicePrimitive, TypeOfAny(v))
	})

	t.Run("slice of slice struct", func(t *testing.T) {
		var v [][]struct{}
		assert.Equal(t, SliceStruct, TypeOfAny(v))
	})

	t.Run("invalid", func(t *testing.T) {
		var v func()
		assert.Equal(t, Invalid, TypeOfAny(v))
	})

	t.Run("slice invalid", func(t *testing.T) {
		var v []func()
		assert.Equal(t, Invalid, TypeOfAny(v))
	})

	t.Run("custom type", func(t *testing.T) {
		type Id int
		var id Id
		assert.Equal(t, Primitive, TypeOfAny(id))
	})

	t.Run("slice of interface", func(t *testing.T) {
		var id []any
		assert.Equal(t, SlicePrimitive, TypeOfAny(id))
	})

	t.Run("interface", func(t *testing.T) {
		var id any
		assert.Equal(t, Primitive, TypeOfAny(id))
	})

	t.Run("nil", func(t *testing.T) {
		assert.Equal(t, Primitive, TypeOfAny(nil))
	})
}

func TestInit(t *testing.T) {
	t.Run("non-pointer value returned as-is", func(t *testing.T) {
		v := reflect.ValueOf(42)
		got := Init(v)
		assert.Equal(t, v, got)
		assert.Equal(t, int64(42), got.Int())
	})

	t.Run("nil pointer gets initialized", func(t *testing.T) {
		var p *int
		v := reflect.ValueOf(&p) // addressable nil pointer
		got := Init(v)
		assert.True(t, got.IsValid())
		assert.Equal(t, reflect.Int, got.Kind())
		assert.NotNil(t, p)
	})

	t.Run("already initialized pointer chain dereferences to value", func(t *testing.T) {
		i := 99
		p := &i
		v := reflect.ValueOf(&p) // **int but already set
		got := Init(v)
		assert.Equal(t, 99, int(got.Int()))
	})

	t.Run("cannot set value remains unchanged", func(t *testing.T) {
		p := new(int)
		v := reflect.ValueOf(p)
		assert.False(t, v.CanSet())
		got := Init(v)
		assert.Equal(t, v.Elem(), got)
	})

	t.Run("nil map", func(t *testing.T) {
		var m map[string]any
		v := reflect.ValueOf(&m)
		got := Init(v)
		assert.False(t, got.IsNil())
		assert.Equal(t, v.Elem(), got)
	})

	t.Run("pointer to struct gets properly initialized", func(t *testing.T) {
		type Inner struct {
			ID   int
			Name string
		}

		type Outer struct {
			Inner *Inner
		}

		var o Outer
		v := reflect.ValueOf(&o).Elem().FieldByName("Inner")
		assert.True(t, v.IsNil())

		got := Init(v)
		assert.True(t, got.IsValid())
		assert.Equal(t, reflect.Struct, got.Kind())
		assert.NotNil(t, o.Inner)

		// Verify that the struct can now be used
		got.FieldByName("ID").SetInt(42)
		got.FieldByName("Name").SetString("Alice")
		assert.Equal(t, 42, o.Inner.ID)
		assert.Equal(t, "Alice", o.Inner.Name)
	})
}
