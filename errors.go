package goutils

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
)

// ======================================================================================
// BaseError

// BaseError is the common foundation for the library's custom error types. Concrete
// error types embed BaseError to inherit the Error / Unwrap behaviour, the rendered
// "{name}: {message} [{core}]" format, and optional call-stack capture.
type BaseError struct {
	// Name identifies the concrete error type in the rendered message.
	Name string
	// Message is the human-readable description of the error.
	Message string
	// Core is the wrapped underlying error, if any.
	Core error
	// stack holds the program counters captured at construction, if requested.
	stack []uintptr
}

// Error implements the error interface, rendering "{name}: {message} [{core}]".
func (e BaseError) Error() string {
	var b strings.Builder
	if e.Name != "" {
		b.WriteString(e.Name)
		b.WriteString(": ")
	}
	b.WriteString(e.Message)
	if e.Core != nil {
		fmt.Fprintf(&b, " [%v]", e.Core)
	}
	return b.String()
}

// Unwrap exposes the wrapped error so errors.Is / errors.As walk the chain.
func (e BaseError) Unwrap() error {
	return e.Core
}

// StackTrace returns the captured call stack as formatted text, or "" if no stack
// was captured at construction.
func (e BaseError) StackTrace() string {
	return RenderCallStack(e.stack)
}

// Format supports fmt verbs. "%+v" appends the captured stack trace (when present),
// mimicking a Python traceback; "%v", "%s" and "%q" render just the message.
func (e BaseError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			_, _ = io.WriteString(s, e.Error())
			if st := e.StackTrace(); st != "" {
				_, _ = io.WriteString(s, "\n"+st)
			}
			return
		}
		_, _ = io.WriteString(s, e.Error())
	case 's':
		_, _ = io.WriteString(s, e.Error())
	case 'q':
		_, _ = fmt.Fprintf(s, "%q", e.Error())
	}
}

// stackCarrier is implemented by errors that captured a call stack at construction.
// Because callStack is unexported, only types declared in this package (BaseError and
// the concrete types embedding it) can satisfy it — foreign errors in a chain are
// ignored even if they expose a stack under some other method name.
type stackCarrier interface {
	callStack() []uintptr
}

// callStack exposes the captured program counters to in-package chain walkers.
func (e BaseError) callStack() []uintptr {
	return e.stack
}

// DeepestErrorWithTrace walks err's Unwrap chain and returns the deepest error (nearest
// the root cause) that captured a call stack. It returns nil if no error in the chain
// carried one. The returned error is the original chain link, so callers may inspect
// its type, message, or render its trace via StackTrace / "%+v".
func DeepestErrorWithTrace(err error) error {
	var deepest error
	for ; err != nil; err = errors.Unwrap(err) {
		if c, ok := err.(stackCarrier); ok && len(c.callStack()) > 0 {
			deepest = err
		}
	}
	return deepest
}

// ======================================================================================
// General Errors
//
// These embed BaseError and capture a stack trace at construction, since they signal
// genuine failures where the origin is actionable.

// BadInputError malformed data input error
type BadInputError struct{ BaseError }

// NewBadInputError builds a BadInputError, optionally capturing the call stack.
func NewBadInputError(message string, core error, getCallStack bool) BadInputError {
	base := BaseError{Name: "BadInputError", Message: message, Core: core}
	if getCallStack {
		base.stack = GetCallStack(1)
	}
	return BadInputError{BaseError: base}
}

// ValidationError error when data fails validation
type ValidationError struct{ BaseError }

// NewValidationError builds a ValidationError, optionally capturing the call stack.
func NewValidationError(message string, core error, getCallStack bool) ValidationError {
	base := BaseError{Name: "ValidationError", Message: message, Core: core}
	if getCallStack {
		base.stack = GetCallStack(1)
	}
	return ValidationError{BaseError: base}
}

// ConsistencyError a data consistency error
type ConsistencyError struct{ BaseError }

// NewConsistencyError builds a ConsistencyError, optionally capturing the call stack.
func NewConsistencyError(message string, core error, getCallStack bool) ConsistencyError {
	base := BaseError{Name: "ConsistencyError", Message: message, Core: core}
	if getCallStack {
		base.stack = GetCallStack(1)
	}
	return ConsistencyError{BaseError: base}
}

// RuntimeError general runtime error
type RuntimeError struct{ BaseError }

// NewRuntimeError builds a RuntimeError, optionally capturing the call stack.
func NewRuntimeError(message string, core error, getCallStack bool) RuntimeError {
	base := BaseError{Name: "RuntimeError", Message: message, Core: core}
	if getCallStack {
		base.stack = GetCallStack(1)
	}
	return RuntimeError{BaseError: base}
}

// TimeoutError operation timed out error
type TimeoutError struct{ BaseError }

// NewTimeoutError builds a TimeoutError, optionally capturing the call stack.
func NewTimeoutError(message string, core error, getCallStack bool) TimeoutError {
	base := BaseError{Name: "TimeoutError", Message: message, Core: core}
	if getCallStack {
		base.stack = GetCallStack(1)
	}
	return TimeoutError{BaseError: base}
}

// NotFoundError a requested resource does not exist
type NotFoundError struct{ BaseError }

// NewNotFoundError builds a NotFoundError, optionally capturing the call stack.
func NewNotFoundError(message string, core error, getCallStack bool) NotFoundError {
	base := BaseError{Name: "NotFoundError", Message: message, Core: core}
	if getCallStack {
		base.stack = GetCallStack(1)
	}
	return NotFoundError{BaseError: base}
}

// AlreadyExistsError a resource being created already exists
type AlreadyExistsError struct{ BaseError }

// NewAlreadyExistsError builds an AlreadyExistsError, optionally capturing the call stack.
func NewAlreadyExistsError(message string, core error, getCallStack bool) AlreadyExistsError {
	base := BaseError{Name: "AlreadyExistsError", Message: message, Core: core}
	if getCallStack {
		base.stack = GetCallStack(1)
	}
	return AlreadyExistsError{BaseError: base}
}

// ShutdownError an operation was rejected because the component has been shut down
type ShutdownError struct{ BaseError }

// NewShutdownError builds a ShutdownError, optionally capturing the call stack.
func NewShutdownError(message string, core error, getCallStack bool) ShutdownError {
	base := BaseError{Name: "ShutdownError", Message: message, Core: core}
	if getCallStack {
		base.stack = GetCallStack(1)
	}
	return ShutdownError{BaseError: base}
}

// ======================================================================================
// HTTP Client Errors

// HTTPRequestError an outbound HTTP request returned a non-success status code
type HTTPRequestError struct {
	BaseError
	// StatusCode is the HTTP status code returned by the upstream request.
	StatusCode int
}

// NewHTTPRequestError builds an HTTPRequestError, optionally capturing the call stack.
func NewHTTPRequestError(
	statusCode int, message string, core error, getCallStack bool,
) HTTPRequestError {
	base := BaseError{Name: "HTTPRequestError", Message: message, Core: core}
	if getCallStack {
		base.stack = GetCallStack(1)
	}
	return HTTPRequestError{BaseError: base, StatusCode: statusCode}
}

// ======================================================================================
// GCP PubSub Errors

// PubSubError wraps an error returned by the Google PubSub backend, distinguishing
// it from errors raised by the wrapper's own logic.
type PubSubError struct {
	BaseError
	// Operation is the PubSub API call that failed (e.g. "CreateTopic").
	Operation string
}

// NewPubSubError builds a PubSubError, optionally capturing the call stack.
func NewPubSubError(operation, message string, core error, getCallStack bool) PubSubError {
	base := BaseError{Name: "PubSubError", Message: message, Core: core}
	if getCallStack {
		base.stack = GetCallStack(1)
	}
	return PubSubError{BaseError: base, Operation: operation}
}

// ======================================================================================
// Queue / AsyncQueue Errors
//
// These are lightweight, expected control-flow signals (not failures), so they carry
// no stack trace.

// NoDataAvailableError no data available error
type NoDataAvailableError struct{}

// Error implement error interface
func (NoDataAvailableError) Error() string {
	return "no data available"
}

// UnexpectedTypeError data has unexpected
type UnexpectedTypeError struct {
	Expected reflect.Type
	Gotten   reflect.Type
}

// Error implement error interface
func (e UnexpectedTypeError) Error() string {
	return fmt.Sprintf("expected '%s' not '%s'", e.Expected, e.Gotten)
}

// ======================================================================================
// REDIS Errors

// RedisError wraps an error returned by REDIS client
type RedisError struct{ BaseError }

// NewRedisError builds a RedisError, optionally capturing the call stack.
func NewRedisError(message string, core error, getCallStack bool) RedisError {
	base := BaseError{Name: "RedisError", Message: message, Core: core}
	if getCallStack {
		base.stack = GetCallStack(1)
	}
	return RedisError{BaseError: base}
}
