package goutils

import (
	"container/heap"
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
	Push(data V) error

	/*
		Pop dequeue data

			@return data from queue
	*/
	Pop() (V, error)
}

// --------------------------------------------------------------------------------------
// Simple queue

// simpleQueue implement Queue
type simpleQueue[V any] struct {
	buffer     *list.List
	targetType reflect.Type
}

/*
GetNewSimpleQueue define new simple queue

	@generic V any - the data type being passed through the queue
	@returns new queue instance
*/
func GetNewSimpleQueue[V any]() Queue[V] {
	var sample V
	return &simpleQueue[V]{buffer: list.New(), targetType: reflect.TypeOf(sample)}
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
func (q *simpleQueue[V]) Push(data V) error {
	dataType := reflect.TypeOf(data)
	if dataType != q.targetType {
		return ErrorUnexpectedType{Expected: q.targetType, Gotten: dataType}
	}
	_ = q.buffer.PushBack(data)
	return nil
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
		return val, ErrorUnexpectedType{Expected: q.targetType, Gotten: reflect.TypeOf(raw)}
	}

	return val, nil
}

// --------------------------------------------------------------------------------------
// Priority queue

// PriorityQueueEntry a priority queue object
type PriorityQueueEntry interface {
	/*
		HigherPriorityThan check whether priority of this object is higher than "right"

			@param right PriorityQueueEntry - object to compare against
			@returns whether this element has higher priority than "right"
	*/
	HigherPriorityThan(right PriorityQueueEntry) bool
}

// priorityQueueCore core data storage of the priority queue
type priorityQueueCore struct {
	core []any
}

// Len implement sort.Interface::Len
func (q *priorityQueueCore) Len() int {
	return len(q.core)
}

// Less implement sort.Interface::Less
func (q *priorityQueueCore) Less(i, j int) bool {
	// When we Pop, we remove the last element in the slice, so we want the highest
	// priority element at the end
	ithEntry := q.core[i].(PriorityQueueEntry)
	jthEntry := q.core[j].(PriorityQueueEntry)
	return ithEntry.HigherPriorityThan(jthEntry)
}

// Swap implement sort.Interface::Swap
func (q *priorityQueueCore) Swap(i, j int) {
	q.core[i], q.core[j] = q.core[j], q.core[i]
}

// Push implement heap.Interface::Push
func (q *priorityQueueCore) Push(x any) {
	q.core = append(q.core, x)
}

// Pop implement heap.Interface::Pop
func (q *priorityQueueCore) Pop() any {
	currentLen := len(q.core)
	// Get the entry
	entry := q.core[currentLen-1]
	// Trim the buffer
	q.core = q.core[0 : currentLen-1 : currentLen-1]
	return entry
}

// priorityQueue implement Queue
type priorityQueue[V PriorityQueueEntry] struct {
	buffer     *priorityQueueCore
	targetType reflect.Type
}

/*
GetNewPriorityQueue define new priority queue

	@generic V any - the data type being passed through the queue
	@returns new queue instance
*/
func GetNewPriorityQueue[V PriorityQueueEntry]() Queue[V] {
	var sample V
	instance := &priorityQueue[V]{
		buffer: &priorityQueueCore{core: make([]any, 0)}, targetType: reflect.TypeOf(sample),
	}
	heap.Init(instance.buffer)
	return instance
}

/*
Len get the current queue length

	@return current queue length
*/
func (q *priorityQueue[V]) Len() int {
	return q.buffer.Len()
}

/*
Push enqueue data

	@param data V - data to enqueue
*/
func (q *priorityQueue[V]) Push(data V) error {
	dataType := reflect.TypeOf(data)
	if dataType != q.targetType {
		return ErrorUnexpectedType{Expected: q.targetType, Gotten: dataType}
	}
	heap.Push(q.buffer, data)
	return nil
}

/*
Pop dequeue data

	@return data from queue
*/
func (q *priorityQueue[V]) Pop() (V, error) {
	var val V

	// No data available
	if q.buffer.Len() == 0 {
		return val, ErrorNoDataAvailable{}
	}

	// Pop highest priority
	ok := false
	raw := heap.Pop(q.buffer)
	val, ok = raw.(V)

	if !ok {
		return val, ErrorUnexpectedType{Expected: reflect.TypeOf(val), Gotten: reflect.TypeOf(raw)}
	}

	return val, nil
}
