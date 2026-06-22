package goutils

import (
	"testing"

	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
)

func TestRenderCallStack(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	// Case 0: A -> B -> C, where C captures the call stack
	{
		var captured []uintptr

		funcC := func() {
			captured = GetCallStack(0)
		}
		funcB := func() {
			funcC()
		}
		funcA := func() {
			funcB()
		}

		funcA()

		rendered := RenderCallStack(captured)
		assert.NotEmpty(rendered)
		log.Debugf("Call stack:\n%s", rendered)
		// The capturing closure, its callers, and this test function should
		// all appear in the rendered stack.
		assert.Contains(rendered, "TestRenderCallStack")
		// GetCallStack itself must be skipped from the rendered output.
		assert.NotContains(rendered, "goutils.GetCallStack")
	}

	// Case 1: recurse to a depth of 5, capturing the call stack at the limit
	{
		const recursionLimit = 5
		var captured []uintptr

		var recurse func(depth int)
		recurse = func(depth int) {
			if depth >= recursionLimit {
				captured = GetCallStack(0)
				return
			}
			recurse(depth + 1)
		}

		recurse(0)

		rendered := RenderCallStack(captured)
		assert.NotEmpty(rendered)
		log.Debugf("Call stack:\n%s", rendered)
		assert.Contains(rendered, "TestRenderCallStack")
		assert.NotContains(rendered, "goutils.GetCallStack")
		// The recursive closure should appear once per stack frame. With a
		// limit of 5 there are 6 invocations (depths 0 through 5).
		frames := captured
		assert.GreaterOrEqual(len(frames), recursionLimit+1)
	}
}
