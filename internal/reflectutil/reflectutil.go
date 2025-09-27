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
	Slice
	SlicePrimitive = Slice | Primitive
	SliceMap       = Slice | Map
	SliceStruct    = Slice | Struct
)

func TypeOf(v any) Type {
	return typeOf(reflect.TypeOf(v))
}

func typeOf(t reflect.Type) Type {
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
		return typeOf(t.Elem())

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
		reflect.String:
		// reflect.Interface:
		return Primitive
	}

	return Invalid
}

func DerefValue(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return v
		}
		return DerefValue(v.Elem())
	}

	return v
}

func DerefType(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Pointer {
		return DerefType(t.Elem())
	}
	return t
}

func Append(s reflect.Value, elem any) {
	s.Set(reflect.Append(s, reflect.ValueOf(elem)))
}
