package goutils

import (
	"container/list"
	"reflect"
)

// Queue queue
type Queue[V any] interface {
	/*
		Len get the current queue length

			@return current queue length
	*/
	Len() int

	/*
		Push enqueue data

			@param data V - data to enqueue
	*/
	Push(data V)

	/*
		Pop dequeue data

			@return data from queue
	*/
	Pop() (V, error)
}

// simpleQueue implement Queue
type simpleQueue[V any] struct {
	buffer *list.List
}

/*
GetNewSimpleQueue define new simple queue

	@generic V any - the data type being passed through the queue
	@returns new queue instance
*/
func GetNewSimpleQueue[V any]() Queue[V] {
	return &simpleQueue[V]{buffer: list.New()}
}

/*
Len get the current queue length

	@return current queue length
*/
func (q *simpleQueue[V]) Len() int {
	return q.buffer.Len()
}

/*
Push enqueue data

	@param data V - data to enqueue
*/
func (q *simpleQueue[V]) Push(data V) {
	_ = q.buffer.PushBack(data)
}

/*
Pop dequeue data

	@return data from queue
*/
func (q *simpleQueue[V]) Pop() (V, error) {
	var val V

	// No data available
	if q.buffer.Len() == 0 {
		return val, ErrorNoDataAvailable{}
	}

	// Pop first element
	ok := false
	ref := q.buffer.Front()
	raw := q.buffer.Remove(ref)
	val, ok = raw.(V)

	if !ok {
		return val, ErrorUnexpectedType{expected: reflect.TypeOf(val), gotten: reflect.TypeOf(raw)}
	}

	return val, nil
}
