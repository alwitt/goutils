package goutils

import (
	"context"
	"sync"
	"sync/atomic"
)

// Condition variable which has similar behavior to the C++11 std::condition_variable
type Condition interface {
	// NotifyOne notify one waiter
	NotifyOne() error

	// NotifyAll notify all waiters
	NotifyAll() error

	/*
		Wait caller will block and wait to be notified. The maximum duration for the wait is
		controlled by the context.

		The signaling channel is provided by the caller, and allows the same channel to be reused
		for subsequent calls.

		IMPORTANT: the signaling channel must be buffered

			@param ctx context.Context - calling context
			@param wakeUp chan bool - signaling channel
			@error ErrorTimeout - timed out waiting for signal
	*/
	Wait(ctx context.Context, wakeUp chan bool) error
}

// conditionImpl implement Condition
type conditionImpl struct {
	waitersLock   sync.Mutex
	waiters       map[uint64]chan bool
	waiterIDTrack atomic.Uint64
}

/*
GetNewCondition get new condition variable.

This condition has similar behavior to the C++11 std::condition_variable.

	@return new condition variable
*/
func GetNewCondition() Condition {
	return &conditionImpl{
		waitersLock:   sync.Mutex{},
		waiters:       make(map[uint64]chan bool),
		waiterIDTrack: atomic.Uint64{},
	}
}

// notify core notify function
func (c *conditionImpl) notify(allWaiters bool) error {
	c.waitersLock.Lock()
	defer c.waitersLock.Unlock()

	for waiterID, signalChan := range c.waiters {
		// Notify waiter
		signalChan <- true
		// Remove waiter from list
		delete(c.waiters, waiterID)

		// Channel remains open, which allows it to be re-used again

		if !allWaiters {
			break
		}
	}

	return nil
}

// NotifyOne notify one waiter
func (c *conditionImpl) NotifyOne() error {
	return c.notify(false)
}

// NotifyAll notify all waiters
func (c *conditionImpl) NotifyAll() error {
	return c.notify(true)
}

/*
Wait caller will block and wait to be notified. The maximum duration for the wait is
controlled by the context.

The signaling channel is provided by the caller, and allows the same channel to be reused
for subsequent calls.

IMPORTANT: the signaling channel must be buffered
*/
func (c *conditionImpl) Wait(ctx context.Context, wakeUp chan bool) error {
	waiterID := c.waiterIDTrack.Add(1)

	// Helper function to insert caller into waiting pool
	register := func() {
		c.waitersLock.Lock()
		defer c.waitersLock.Unlock()
		c.waiters[waiterID] = wakeUp
	}

	// Helper function to remove caller from waiting pool
	deregister := func() {
		c.waitersLock.Lock()
		defer c.waitersLock.Unlock()
		delete(c.waiters, waiterID)
	}

	// register for signal
	register()

	// wait
	select {
	case <-ctx.Done():
		deregister()
		return ErrorTimeout{}
	case <-wakeUp:
	}

	return nil
}
