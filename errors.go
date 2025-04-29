package goutils

import (
	"fmt"
	"reflect"
)

// ErrorNoDataAvailable no data available error
type ErrorNoDataAvailable struct{}

// Error implement error interface
func (ErrorNoDataAvailable) Error() string {
	return "no data available"
}

// ErrorUnexpectedType data has unexpected
type ErrorUnexpectedType struct {
	Expected reflect.Type
	Gotten   reflect.Type
}

// Error implement error interface
func (e ErrorUnexpectedType) Error() string {
	return fmt.Sprintf("expected '%s' not '%s'", e.Expected, e.Gotten)
}

// ErrorTimeout operation timed out error
type ErrorTimeout struct{}

// Error implement error interface
func (ErrorTimeout) Error() string {
	return "timeout"
}
