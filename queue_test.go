package goutils

import (
	"testing"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestSimpleQueueBasic(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	uut := GetNewSimpleQueue[string]()

	// Case 0: empty queue
	{
		assert.Equal(0, uut.Len())
		_, err := uut.Pop()
		assert.NotNil(err)
		assert.IsType(ErrorNoDataAvailable{}, err)
	}

	// Case 1: enqueue and dequeue
	{
		testData := []string{}
		for itr := 0; itr < 10; itr++ {
			testData = append(testData, uuid.NewString())
		}
		for _, val := range testData {
			uut.Push(val)
		}
		for _, val := range testData {
			read, err := uut.Pop()
			assert.Nil(err)
			assert.Equal(val, read)
		}
	}
}
