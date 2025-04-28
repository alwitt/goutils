package goutils

import (
	"context"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestAsyncQueueBasic(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uut, err := GetNewAsyncQueue[string](utCtx, "unit-test", log.Fields{})
	assert.Nil(err)

	dataWaitSignal := make(chan bool, 1)

	// Case 0: no data
	{
		assert.Equal(0, uut.Len())
		_, err = uut.Pop(utCtx, false, dataWaitSignal)
		assert.NotNil(err)
		assert.IsType(ErrorNoDataAvailable{}, err)
	}

	// Case 1: pass message
	{
		testMsg := uuid.NewString()
		assert.Nil(uut.Push(utCtx, testMsg))

		lclCtx, lclCancel := context.WithTimeout(utCtx, time.Millisecond*5)
		recv, err := uut.Pop(lclCtx, true, dataWaitSignal)
		assert.Nil(err)
		assert.Equal(testMsg, recv)
		lclCancel()
	}

	// Case 2: pass multiple messages, no readers
	{
		testMsgs1 := []string{
			uuid.NewString(),
			uuid.NewString(),
			uuid.NewString(),
		}

		for _, msg := range testMsgs1 {
			assert.Nil(uut.Push(utCtx, msg))
		}

		lclCtx, lclCancel := context.WithTimeout(utCtx, time.Millisecond*5)
		for _, msg := range testMsgs1 {
			recv, err := uut.Pop(lclCtx, true, dataWaitSignal)
			assert.Nil(err)
			assert.Equal(msg, recv)
		}
		lclCancel()
	}

	// Case 3: write and read message in parallel
	{
		testMsgs2 := []string{}
		for itr := 0; itr < 10; itr++ {
			testMsgs2 = append(testMsgs2, uuid.NewString())
		}

		go func() {
			for _, msg := range testMsgs2 {
				assert.Nil(uut.Push(utCtx, msg))
			}
		}()

		lclCtx, lclCancel := context.WithTimeout(utCtx, time.Millisecond*5)
		for _, msg := range testMsgs2 {
			recv, err := uut.Pop(lclCtx, true, dataWaitSignal)
			assert.Nil(err)
			assert.Equal(msg, recv)
		}
		lclCancel()
	}
}
