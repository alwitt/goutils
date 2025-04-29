package goutils

import (
	"context"
	"sync"

	"github.com/apex/log"
)

// AsyncQueue asynchronous queue
type AsyncQueue[V any] interface {
	/*
		Len get the current queue length

			@return current queue length
	*/
	Len() int

	/*
		Push enqueue data

			@generic V any - the data type being passed through the queue
			@param ctx context.Context - calling context
			@param data V - data to enqueue
	*/
	Push(ctx context.Context, data V) error

	/*
		Pop dequeue data. If caller choices to wait until data is available, the maximum duration
		for the wait is controlled by the context.

		The queue uses "Condition" to signal to any awaiting caller that data is available. To
		support that, the caller needs to supply wake up chan for use with "Condition".

			@generic V any - the data type being passed through the queue
			@param ctx context.Context - calling context
			@param blocking bool - whether to block until data is available
			@param newDataSignalFlag chan bool - wake up chan for use with "Condition"
			@return data from queue
	*/
	Pop(ctx context.Context, blocking bool, newDataSignalFlag chan bool) (V, error)
}

// asyncQueueImpl implements AsyncQueue
type asyncQueueImpl[V any] struct {
	Component

	name string

	buffer     Queue[V]
	bufferLock sync.Mutex

	newDataSignal Condition
}

/*
GetNewAsyncQueue define new asynchronous queue

	@generic V any - the data type being passed through the queue
	@param ctx context.Context - calling context
	@param instanceName string - queue instance name
	@param logTags log.Fields - metadata fields to include in the logs
	@returns new AsyncQueue instance
*/
func GetNewAsyncQueue[V any](
	ctx context.Context, instanceName string, logTags log.Fields,
) (AsyncQueue[V], error) {
	logTags["queue-type"] = "basic"
	logTags["queue-name"] = instanceName
	instance := &asyncQueueImpl[V]{
		Component:     Component{LogTags: logTags},
		name:          instanceName,
		buffer:        GetNewSimpleQueue[V](),
		bufferLock:    sync.Mutex{},
		newDataSignal: GetNewCondition(),
	}
	return instance, nil
}

/*
GetNewAsyncPriorityQueue define new asynchronous priority queue

	@generic V PriorityQueueEntry - the data type being passed through the queue
	@param ctx context.Context - calling context
	@param instanceName string - queue instance name
	@param logTags log.Fields - metadata fields to include in the logs
	@returns new AsyncQueue instance
*/
func GetNewAsyncPriorityQueue[V PriorityQueueEntry](
	ctx context.Context, instanceName string, logTags log.Fields,
) (AsyncQueue[V], error) {
	logTags["queue-type"] = "priority"
	logTags["queue-name"] = instanceName
	instance := &asyncQueueImpl[V]{
		Component:     Component{LogTags: logTags},
		name:          instanceName,
		buffer:        GetNewPriorityQueue[V](),
		bufferLock:    sync.Mutex{},
		newDataSignal: GetNewCondition(),
	}
	return instance, nil
}

/*
Len get the current queue length

	@return current queue length
*/
func (q *asyncQueueImpl[V]) Len() int {
	q.bufferLock.Lock()
	defer q.bufferLock.Unlock()
	return q.buffer.Len()
}

/*
Push enqueue data

	@generic V any - the data type being passed through the queue
	@param ctx context.Context - calling context
	@param data V - data to enqueue
*/
func (q *asyncQueueImpl[V]) Push(ctx context.Context, data V) error {
	logTags := q.GetLogTagsForContext(ctx)

	// Buffer data
	insert := func(m V) error {
		q.bufferLock.Lock()
		defer q.bufferLock.Unlock()
		return q.buffer.Push(m)
	}
	if err := insert(data); err != nil {
		return err
	}

	// Notify waiting caller
	if err := q.newDataSignal.NotifyOne(); err != nil {
		log.WithError(err).WithFields(logTags).Error("Failed to notify awaiting data reader")
		return err
	}

	return nil
}

/*
Pop dequeue data. If caller choices to wait until data is available, the maximum duration
for the wait is controlled by the context.

The queue uses "Condition" to signal to any awaiting caller that data is available. To
support that, the caller needs to supply wake up chan for use with "Condition".

	@generic V any - the data type being passed through the queue
	@param ctx context.Context - calling context
	@param blocking bool - whether to block until data is available
	@param newDataSignalFlag chan bool - wake up chan for use with "Condition"
	@return data from queue
*/
func (q *asyncQueueImpl[V]) Pop(
	ctx context.Context, blocking bool, newDataSignalFlag chan bool,
) (V, error) {
	logTags := q.GetLogTagsForContext(ctx)

	// Helper function to read from the buffer
	getData := func() (V, error) {
		q.bufferLock.Lock()
		defer q.bufferLock.Unlock()
		return q.buffer.Pop()
	}

	var result V

	for {
		// Attempt to get data
		var err error
		result, err = getData()
		if err == nil {
			break
		}

		// Proceed based on actual error
		switch actualErr := err.(type) {
		case ErrorNoDataAvailable:
			// Queue empty
			if !blocking {
				return result, actualErr
			}

		default:
			// Any other error
			log.WithError(err).WithFields(logTags).Error("Queue read error")
			return result, err
		}

		// Wait until data is available
		if err := q.newDataSignal.Wait(ctx, newDataSignalFlag); err != nil {
			log.WithError(err).WithFields(logTags).Error("Error while awaiting new data")
			return result, err
		}
	}

	return result, nil
}
