package goutils

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
)

func TestConditionNotifyOne(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uut := GetNewCondition()

	// Case 0: no waiters
	{
		assert.NotPanics(func() { assert.Nil(uut.NotifyOne()) })
		assert.NotPanics(func() { assert.Nil(uut.NotifyAll()) })
	}

	testSignalChan := make(chan bool, 1)

	// Case 1: one waiter
	{
		complete := make(chan bool, 1)

		lclCtx, lclCancel := context.WithTimeout(utCtx, time.Millisecond*20)

		go func() {
			assert.Nil(uut.Wait(lclCtx, testSignalChan))
			complete <- true
		}()

		time.Sleep(time.Millisecond * 5)
		assert.Nil(uut.NotifyOne())

		select {
		case <-lclCtx.Done():
			assert.False(true, "wait timed out")
		case <-complete:
		}
		lclCancel()
	}

	// Case 2: one waiter timeout
	{
		complete := make(chan bool, 1)

		lclCtx, lclCancel := context.WithTimeout(utCtx, time.Millisecond*20)

		go func() {
			waitCtx, waitCancel := context.WithTimeout(utCtx, time.Millisecond*10)
			defer waitCancel()
			err := uut.Wait(waitCtx, testSignalChan)
			assert.NotNil(err)
			assert.IsType(ErrorTimeout{}, err)
			complete <- true
		}()

		select {
		case <-lclCtx.Done():
			assert.False(true, "wait timed out")
		case <-complete:
		}
		lclCancel()
	}

	// Case 3: sequentially notify multiple waiters
	{
		waiters := 3
		complete := make(chan int, waiters)

		lclCtx, lclCancel := context.WithTimeout(utCtx, time.Millisecond*20)

		// Helper function to run the wait
		wg := sync.WaitGroup{}
		wait := func(id int) {
			waitCtx, waitCancel := context.WithTimeout(utCtx, time.Millisecond*10)
			defer waitCancel()
			defer wg.Done()
			signal := make(chan bool, 1)
			assert.Nil(uut.Wait(waitCtx, signal))
			complete <- id
		}

		// Wait
		for itr := 0; itr < waiters; itr++ {
			wg.Add(1)
			go wait(itr)
		}

		time.Sleep(time.Millisecond * 5)
		// notify one at a time
		for itr := 0; itr < waiters; itr++ {
			assert.Nil(uut.NotifyOne())
		}

		assert.Nil(TimeBoundedWaitGroupWait(lclCtx, &wg, time.Millisecond*20))

		waited := map[int]bool{}
		for itr := 0; itr < waiters; itr++ {
			select {
			case <-lclCtx.Done():
				assert.False(true, "wait timed out")
			case id, ok := <-complete:
				assert.True(ok)
				waited[id] = true
			}
		}
		assert.Len(waited, waiters)

		lclCancel()
	}
}

func TestConditionNotifyAll(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uut := GetNewCondition()

	waiters := 5
	complete := make(chan int, waiters)

	lclCtx, lclCancel := context.WithTimeout(utCtx, time.Millisecond*20)

	// Helper function to run the wait
	wg := sync.WaitGroup{}
	wait := func(id int) {
		waitCtx, waitCancel := context.WithTimeout(utCtx, time.Millisecond*10)
		defer waitCancel()
		defer wg.Done()
		signal := make(chan bool, 1)
		assert.Nil(uut.Wait(waitCtx, signal))
		complete <- id
	}

	// Wait
	for itr := 0; itr < waiters; itr++ {
		wg.Add(1)
		go wait(itr)
	}

	// Notify waiters
	time.Sleep(time.Millisecond * 5)
	assert.Nil(uut.NotifyAll())

	assert.Nil(TimeBoundedWaitGroupWait(lclCtx, &wg, time.Millisecond*20))

	waited := map[int]bool{}
	for itr := 0; itr < waiters; itr++ {
		select {
		case <-lclCtx.Done():
			assert.False(true, "wait timed out")
		case id, ok := <-complete:
			assert.True(ok)
			waited[id] = true
		}
	}
	assert.Len(waited, waiters)

	lclCancel()
}
