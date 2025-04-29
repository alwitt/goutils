package goutils

import (
	"sort"
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
			assert.Nil(uut.Push(val))
		}
		for _, val := range testData {
			read, err := uut.Pop()
			assert.Nil(err)
			assert.Equal(val, read)
		}
	}
}

// testPriorityQueueStringEntry priority queue test struct
type testPriorityQueueStringEntry struct {
	data string
}

// HigherPriorityThan implements PriorityQueueEntry
func (t testPriorityQueueStringEntry) HigherPriorityThan(right PriorityQueueEntry) bool {
	r := right.(testPriorityQueueStringEntry)
	return t.data < r.data
}

func TestPriorityQueueBasic(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	uut := GetNewPriorityQueue[testPriorityQueueStringEntry]()

	// Case 0: empty queue
	{
		assert.Equal(0, uut.Len())
		_, err := uut.Pop()
		assert.NotNil(err)
		assert.IsType(ErrorNoDataAvailable{}, err)
	}

	// Case 1: enqueue data
	{
		testValues := []string{}
		for itr := 0; itr < 10; itr++ {
			testValues = append(testValues, uuid.NewString())
		}
		for _, value := range testValues {
			assert.Nil(uut.Push(testPriorityQueueStringEntry{data: value}))
		}
		sort.Strings(testValues)
		for idx, value := range testValues {
			readBack, err := uut.Pop()
			assert.Nil(err)
			assert.Equalf(value, readBack.data, "%d", idx)
		}
	}
}
