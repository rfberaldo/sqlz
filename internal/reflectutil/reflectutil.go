package reflectutil

import (
	"reflect"
)

// Type is similar to [reflect.Kind], but adds support for type of slices.
// [reflect.Func], [reflect.Chan], [reflect.Array] and [reflect.UnsafePointer] are considered Invalid.
// Nil is considered Primitive.
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

// TypeOfAny recursively returns the Type of arg, nil is considered Primitive.
func TypeOfAny(arg any) Type {
	return TypeOf(reflect.TypeOf(arg))
}

// TypeOf recursively returns the Type of t, nil is considered Primitive.
func TypeOf(t reflect.Type) Type {
	if t == nil {
		return Primitive
	}

	switch t.Kind() {
	case reflect.Map:
		return Map

	case reflect.Struct:
		return Struct

	case reflect.Slice:
		if et := TypeOf(t.Elem()); et > 0 {
			return Slice | et
		}

	case reflect.Pointer:
		return TypeOf(t.Elem())

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

// Deref follows the pointer from a [reflect.Type].
func Deref(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Pointer {
		return t.Elem()
	}
	return t
}

// Init returns v initialized if it's not.
// If v is nil pointer but is not addressable, it returns the nil pointer.
func Init(v reflect.Value) reflect.Value {
	for v.Kind() == reflect.Pointer && !v.IsNil() {
		v = v.Elem()
	}

	if !v.CanSet() {
		return v
	}

	switch v.Kind() {
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		return v.Elem()

	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
		return v
	}

	return v
}

// TypedValue returns v's value using typed functions,
// like Bool(), String(), etc; fallsback to Interface().
func TypedValue(v reflect.Value) any {
	switch v.Kind() {
	case reflect.Bool:
		return v.Bool()

	case reflect.String:
		return v.String()

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(v.Int())

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return uint(v.Uint())

	case reflect.Float32, reflect.Float64:
		return v.Float()

	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return v.Bytes() // []byte
		}
	}

	return v.Interface()
}
