package goutils

import (
	"errors"
	"fmt"
	"testing"

	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
)

func TestErrorConstruction(t *testing.T) {

	log.SetLevel(log.DebugLevel)

	// Each General Error type is built by a constructor with the same
	// (message, core, getCallStack) signature. Wrap them uniformly so the
	// table below can exercise every type with one body.
	type builder func(message string, core error, getCallStack bool) error
	cases := []struct {
		name string
		new  builder
	}{
		{
			"BadInputError",
			func(m string, c error, s bool) error { return NewBadInputError(m, c, s) },
		},
		{
			"ValidationError",
			func(m string, c error, s bool) error { return NewValidationError(m, c, s) },
		},
		{
			"ConsistencyError",
			func(m string, c error, s bool) error { return NewConsistencyError(m, c, s) },
		},
		{
			"RuntimeError",
			func(m string, c error, s bool) error { return NewRuntimeError(m, c, s) },
		},
		{
			"TimeoutError",
			func(m string, c error, s bool) error { return NewTimeoutError(m, c, s) },
		},
	}

	for _, tc := range cases {
		assert.True(t, t.Run(tc.name, func(lt *testing.T) {
			assert := assert.New(lt)
			// Case 0: no wrapped core, no call stack captured.
			{
				err := tc.new("something went wrong", nil, false)
				assert.Equal(fmt.Sprintf("%s: something went wrong", tc.name), err.Error())
				// Without a core there is nothing to unwrap.
				assert.Nil(errors.Unwrap(err))

				// With no stack requested, the rendered stack is empty and
				// "%+v" degrades to the same output as "%v".
				st, ok := err.(interface{ StackTrace() string })
				assert.True(ok)
				assert.Empty(st.StackTrace())
				assert.Equal(err.Error(), fmt.Sprintf("%+v", err))
			}

			// Case 1: wrapped core, no call stack captured.
			{
				core := errors.New("root cause")
				err := tc.new("wrapping failure", core, false)
				assert.Equal(
					fmt.Sprintf("%s: wrapping failure [root cause]", tc.name),
					err.Error(),
				)
				// errors.Is / errors.As must walk through to the wrapped core.
				assert.ErrorIs(err, core)
				assert.Same(core, errors.Unwrap(err))
				log.Debugf("%+v", err)
			}

			// Case 2: call stack capture must succeed and include this test
			// function, while excluding GetCallStack itself.
			{
				var err error
				assert.NotPanics(func() {
					err = tc.new("captured failure", nil, true)
				})

				st, ok := err.(interface{ StackTrace() string })
				assert.True(ok)
				rendered := st.StackTrace()
				assert.NotEmpty(rendered)
				log.Debugf("%s stack:\n%s", tc.name, rendered)
				assert.Contains(rendered, "TestErrorConstruction")
				assert.NotContains(rendered, "goutils.GetCallStack")

				// "%+v" should append the captured stack after the message.
				verbose := fmt.Sprintf("%+v", err)
				assert.Contains(verbose, err.Error())
				assert.Contains(verbose, rendered)
				log.Debugf("%s", verbose)
			}
		}))
	}
}

// genValidationError is the innermost generator: it produces the root-cause
// ValidationError, capturing its own call stack.
func genValidationError() error {
	return NewValidationError("data failed validation", fmt.Errorf("dummy error 3"), true)
}

// genBadInputError wraps the ValidationError from GenValidationError, capturing its
// own call stack on the way out.
func genBadInputError() error {
	return NewBadInputError(
		"input was rejected", fmt.Errorf("dummy wrap 2 [%w]", genValidationError()), true,
	)
}

// genRuntimeError wraps the BadInputError from GenBadInputError, capturing its own
// call stack on the way out.
func genRuntimeError() error {
	return NewRuntimeError(
		"operation failed", fmt.Errorf("dummy wrap 1 [%w]", genBadInputError()), true,
	)
}

func TestFindDeepestErrorStackTrace(t *testing.T) {

	log.SetLevel(log.DebugLevel)
	assert := assert.New(t)

	// Build a chain RuntimeError -> BadInputError -> ValidationError where every
	// link captured its own stack. The walker must return the deepest one.
	err := genRuntimeError()

	deepest := DeepestErrorWithTrace(err)
	assert.NotNil(deepest)

	// The deepest stack-carrying error is the root-cause ValidationError, and not
	// either of the outer wrappers.
	var validationErr ValidationError
	assert.True(errors.As(deepest, &validationErr))
	var badInputErr BadInputError
	assert.False(errors.As(deepest, &badInputErr))
	var runtimeErr RuntimeError
	assert.False(errors.As(deepest, &runtimeErr))

	// Its captured stack must originate in GenValidationError.
	st, ok := deepest.(interface{ StackTrace() string })
	assert.True(ok)
	rendered := st.StackTrace()
	assert.NotEmpty(rendered)
	assert.Contains(rendered, "genValidationError")
	log.Debugf("deepest stack:\n%s", rendered)
	log.Debugf("full error chain: %v", err)
}
