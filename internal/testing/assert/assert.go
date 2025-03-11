// Package for testing purposes, it's just an abstraction for
// condition checking and printing errors.
//
// See: https://google.github.io/styleguide/go/decisions#assertion-libraries
package assert

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

const (
	tmpl    = "%v\n     got = %+v (type=%T)\nexpected = %+v (type=%T)"
	errTmpl = "%v\n     got = %v\nexpected = %v"
)

// Equal expects got to be equal expected, using [cmp.Equal].
func Equal(t testing.TB, msg string, got, expected any) {
	if !cmp.Equal(got, expected, cmpopts.EquateEmpty()) {
		t.Errorf(tmpl, msg, got, got, expected, expected)
	}
}

// ErrorIs expects got to be equal expected, using [errors.Is].
func ErrorIs(t testing.TB, msg string, got, expected error) {
	if !errors.Is(got, expected) {
		t.Errorf(errTmpl, msg, got, expected)
	}
}

// Error expects error.
func Error(t testing.TB, msg string, err error) {
	if err == nil {
		t.Errorf(errTmpl, msg, nil, "error")
	}
}

// NoError expects no error.
func NoError(t testing.TB, msg string, err error) {
	if err != nil {
		t.Errorf(errTmpl, msg, err, nil)
	}
}

// NoError expects no error, and stop execution on fail.
func MustNoError(t testing.TB, msg string, err error) {
	if err != nil {
		t.Fatalf(errTmpl, msg, err, nil)
	}
}

// ExpectedError reports if the error is expected.
func ExpectedError(t testing.TB, msg string, err error, expected bool) {
	if (err != nil) != expected {
		if expected {
			t.Errorf(errTmpl, msg, err, "error")
		} else {
			t.Errorf(errTmpl, msg, err, nil)
		}
	}
}
