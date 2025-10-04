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

func TestDeref(t *testing.T) {
	t.Run("basic value", func(t *testing.T) {
		v := reflect.ValueOf(42)
		got := Deref(v)
		assert.Equal(t, v.Interface(), got.Interface())
	})

	t.Run("single pointer", func(t *testing.T) {
		x := 42
		v := reflect.ValueOf(&x)
		got := Deref(v)
		assert.Equal(t, x, got.Interface())
	})

	t.Run("slice", func(t *testing.T) {
		x := []int{42}
		v := reflect.ValueOf(x)
		got := Deref(v)
		assert.Equal(t, x, got.Interface())
	})

	t.Run("pointer to slice", func(t *testing.T) {
		x := []int{42}
		v := reflect.ValueOf(&x)
		got := Deref(v)
		assert.Equal(t, x, got.Interface())
	})

	t.Run("multiple pointers", func(t *testing.T) {
		x := 42
		p1 := &x
		p2 := &p1
		p3 := &p2
		v := reflect.ValueOf(p3)
		got := Deref(v)
		assert.Equal(t, x, got.Interface())
	})

	t.Run("interface with basic value", func(t *testing.T) {
		var i any = 42
		v := reflect.ValueOf(i)
		got := Deref(v)
		assert.Equal(t, 42, got.Interface())
	})

	t.Run("interface with pointer", func(t *testing.T) {
		x := 42
		var i any = &x
		v := reflect.ValueOf(i)
		got := Deref(v)
		assert.Equal(t, x, got.Interface())
	})

	t.Run("nested interface with basic value", func(t *testing.T) {
		var i any = any(42)
		v := reflect.ValueOf(i)
		got := Deref(v)
		assert.Equal(t, 42, got.Interface())
	})

	t.Run("nested interface with pointer", func(t *testing.T) {
		x := 42
		var i any = &x
		v := reflect.ValueOf(i)
		got := Deref(v)
		assert.Equal(t, x, got.Interface())
	})

	t.Run("nil pointer", func(t *testing.T) {
		var p *int
		v := reflect.ValueOf(p)
		got := Deref(v)
		assert.True(t, got.IsNil())
	})

	t.Run("nil pointer interface", func(t *testing.T) {
		var v map[string]any
		var i any = &v
		got := Deref(reflect.ValueOf(i))
		assert.True(t, got.IsNil())
	})

	t.Run("nil interface", func(t *testing.T) {
		var i any
		v := reflect.ValueOf(i)
		got := Deref(v)
		assert.False(t, got.IsValid())
	})

	t.Run("invalid reflect.Value", func(t *testing.T) {
		var v reflect.Value // zero Value
		got := Deref(v)
		assert.False(t, got.IsValid())
	})
}
