package reflectutil

import (
	"reflect"
)

type Type uint8

const (
	Invalid   Type = 0
	Primitive Type = 1 << iota
	Map
	Struct
	Pointer
	Slice

	SlicePrimitive        = Slice | Primitive
	SliceMap              = Slice | Map
	SliceStruct           = Slice | Struct
	PointerPrimitive      = Pointer | Primitive
	PointerMap            = Pointer | Map
	PointerStruct         = Pointer | Struct
	SlicePointerPrimitive = Slice | Pointer | Primitive
	SlicePointerMap       = Slice | Pointer | Map
	SlicePointerStruct    = Slice | Pointer | Struct
)

func TypeOf(v any) Type {
	return typeOf(reflect.TypeOf(v))
}

func typeOf(t reflect.Type) Type {
	if t == nil {
		return PointerPrimitive // interface
	}

	switch t.Kind() {
	case reflect.Map:
		return Map

	case reflect.Struct:
		return Struct

	case reflect.Array, reflect.Slice:
		if et := typeOf(t.Elem()); et > 0 {
			return Slice | et
		}

	case reflect.Pointer:
		if et := typeOf(t.Elem()); et > 0 {
			return Pointer | et
		}

	case
		reflect.Bool,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Uintptr,
		reflect.Float32,
		reflect.Float64,
		reflect.Complex64,
		reflect.Complex128,
		reflect.String,
		reflect.Interface:
		return Primitive
	}

	return Invalid
}

// Deref recursively derefs a [reflect.Value].
// If v is a nil pointer or nil interface, it returns an invalid value.
func Deref(v reflect.Value) reflect.Value {
	kind := v.Kind()
	if kind == reflect.Pointer || kind == reflect.Interface {
		if v.IsNil() {
			return v
		}

		return Deref(v.Elem())
	}

	return v
}
