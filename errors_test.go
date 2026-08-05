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

// genSQLError is the innermost generator of the second ladder, deliberately a different
// type from genValidationError's so a branched tree's results are told apart by type
// rather than by counting.
func genSQLError() error {
	return NewSQLError("statement failed", fmt.Errorf("dummy error 4"), true)
}

// genPersistenceError wraps the SQLError from genSQLError, capturing its own call stack on
// the way out.
func genPersistenceError() error {
	return NewPersistenceError(
		"persistence operation failed", fmt.Errorf("dummy wrap 4 [%w]", genSQLError()), true,
	)
}

// genUntracedError builds a chain that captured no stack anywhere, standing in for a
// failure that arrived from a library rather than from one of this package's constructors.
func genUntracedError() error {
	return fmt.Errorf("outer [%w]", fmt.Errorf("inner"))
}

// stackTraceOf render an error's captured stack, failing the test if it carried none.
func stackTraceOf(t *testing.T, err error) string {
	t.Helper()
	assert := assert.New(t)

	carrier, ok := err.(interface{ StackTrace() string })
	assert.True(ok, "error %T carried no stack", err)
	if !ok {
		return ""
	}

	rendered := carrier.StackTrace()
	assert.NotEmpty(rendered)
	return rendered
}

func TestAllDeepestErrorStackTraces(t *testing.T) {

	log.SetLevel(log.DebugLevel)

	// Case 1: a chain with no branch in it. The tree walker must agree with the chain
	// walker exactly - one error, the root cause - which is what makes the singular form a
	// special case of this one rather than a separate rule.
	t.Run("reports one error for a linear chain", func(t *testing.T) {
		assert := assert.New(t)

		found := AllDeepestErrorsWithTrace(genRuntimeError())

		assert.Len(found, 1)
		var validationErr ValidationError
		assert.True(errors.As(found[0], &validationErr))
		assert.Contains(stackTraceOf(t, found[0]), "genValidationError")
	})

	// Case 2: the shape errors.Join produces. Both origins must survive - reporting one and
	// dropping the other is the whole reason this exists - and they must arrive in the order
	// they were joined, which for a caller aggregating failures is the order they happened.
	t.Run("reports every branch of a joined tree, in join order", func(t *testing.T) {
		assert := assert.New(t)

		err := NewRuntimeError(
			"two things failed", errors.Join(genRuntimeError(), genPersistenceError()), true,
		)

		found := AllDeepestErrorsWithTrace(err)

		assert.Len(found, 2)
		var validationErr ValidationError
		assert.True(errors.As(found[0], &validationErr))
		var sqlErr SQLError
		assert.True(errors.As(found[1], &sqlErr))

		assert.Contains(stackTraceOf(t, found[0]), "genValidationError")
		assert.Contains(stackTraceOf(t, found[1]), "genSQLError")
	})

	// Case 3: a join nested inside a branch of another join. The result is one flat list of
	// origins whatever shape the tree that produced them had, so a caller never walks it.
	t.Run("flattens a join nested inside a branch", func(t *testing.T) {
		assert := assert.New(t)

		err := errors.Join(
			genRuntimeError(),
			NewRuntimeError(
				"and two more", errors.Join(genPersistenceError(), genBadInputError()), true,
			),
		)

		found := AllDeepestErrorsWithTrace(err)

		assert.Len(found, 3)
		var validationErr ValidationError
		assert.True(errors.As(found[0], &validationErr))
		var sqlErr SQLError
		assert.True(errors.As(found[1], &sqlErr))
		// The third branch's own root cause, reached through a second BadInputError ladder.
		assert.True(errors.As(found[2], &validationErr))
		assert.Contains(stackTraceOf(t, found[2]), "genValidationError")
	})

	// Case 4: the case that separates "deepest per branch" from "every traced error in the
	// tree". The wrapper captured a stack of its own, but something nearer the root cause
	// exists below it, so it must not be reported alongside that one.
	t.Run("shadows a traced ancestor of a traced error", func(t *testing.T) {
		assert := assert.New(t)

		err := NewRuntimeError("wrapping", errors.Join(genRuntimeError()), true)

		found := AllDeepestErrorsWithTrace(err)

		assert.Len(found, 1)
		var validationErr ValidationError
		assert.True(errors.As(found[0], &validationErr))
		var runtimeErr RuntimeError
		assert.False(errors.As(found[0], &runtimeErr))
	})

	// Case 5: a branch that carried no stack anywhere contributes nothing rather than a nil
	// entry a caller would have to filter before rendering.
	t.Run("drops a branch that carried no stack", func(t *testing.T) {
		assert := assert.New(t)

		err := errors.Join(genUntracedError(), genPersistenceError(), genUntracedError())

		found := AllDeepestErrorsWithTrace(err)

		assert.Len(found, 1)
		var sqlErr SQLError
		assert.True(errors.As(found[0], &sqlErr))
	})

	// Case 6: nothing anywhere carried a stack. Nil rather than an empty slice, so a caller
	// checks length once and falls back to the plain message.
	t.Run("reports nothing when no error carried a stack", func(t *testing.T) {
		assert := assert.New(t)

		assert.Nil(AllDeepestErrorsWithTrace(genUntracedError()))
		assert.Nil(AllDeepestErrorsWithTrace(errors.Join(genUntracedError(), genUntracedError())))
		assert.Nil(AllDeepestErrorsWithTrace(nil))
	})
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

	// Handed a tree rather than a chain, the first branch's root cause is reported. What
	// matters is that it is a root cause at all: walking with errors.Unwrap would have
	// stopped at the outer wrapper and rendered the wrapping site instead.
	joined := NewRuntimeError(
		"two things failed", errors.Join(genPersistenceError(), genRuntimeError()), true,
	)

	deepest = DeepestErrorWithTrace(joined)
	assert.NotNil(deepest)

	var sqlErr SQLError
	assert.True(errors.As(deepest, &sqlErr))
	assert.False(errors.As(deepest, &runtimeErr))
	assert.Contains(stackTraceOf(t, deepest), "genSQLError")
}
