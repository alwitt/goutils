package goutils

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apex/log"
)

// TimeoutHandler callback function signature called timer timeout
type TimeoutHandler func() error

// IntervalTimer is a support interface for triggering events at specific intervals
type IntervalTimer interface {
	/*
		Start starts timer with a specific timeout interval, and the callback to trigger on timeout.

		If oneShort, cancel after first timeout.

		 @param interval time.Duration - timeout interval
		 @param handler TimeoutHandler - handler to trigger on timeout
		 @param oneShort bool - if true, timer stop after first activation
	*/
	Start(interval time.Duration, handler TimeoutHandler, oneShort bool) error

	/*
		Stop stops the timer
	*/
	Stop() error
}

// intervalTimerImpl implements IntervalTimer
type intervalTimerImpl struct {
	Component
	running          bool
	rootContext      context.Context
	operationContext context.Context
	contextCancel    context.CancelFunc
	wg               *sync.WaitGroup
}

/*
GetIntervalTimerInstance get an implementation instance of IntervalTimer

	@param rootCtxt context.Context - the base Context the timer will derive new runtime context from
	  each time Start is called.
	@param wg *sync.WaitGroup - WaitGroup use by timer
	@param logTags log.Fields - log metadata fields
	@return an IntervalTimer instance
*/
func GetIntervalTimerInstance(
	rootCtxt context.Context, wg *sync.WaitGroup, logTags log.Fields,
) (IntervalTimer, error) {
	return &intervalTimerImpl{
		Component:        Component{LogTags: logTags},
		running:          false,
		rootContext:      rootCtxt,
		operationContext: nil,
		contextCancel:    nil,
		wg:               wg,
	}, nil
}

/*
Start starts timer with a specific timeout interval, and the callback to trigger on timeout.

If oneShort, cancel after first timeout.

	@param interval time.Duration - timeout interval
	@param handler TimeoutHandler - handler to trigger on timeout
	@param oneShort bool - if true, timer stop after first activation
*/
func (t *intervalTimerImpl) Start(
	interval time.Duration, handler TimeoutHandler, oneShot bool,
) error {
	if t.running {
		return fmt.Errorf("already running")
	}
	log.WithFields(t.LogTags).Infof("Starting with int %s", interval)
	t.wg.Add(1)
	t.running = true
	ctxt, cancel := context.WithCancel(t.rootContext)
	t.operationContext = ctxt
	t.contextCancel = cancel
	go func() {
		defer t.wg.Done()
		defer log.WithFields(t.LogTags).Info("Timer loop exiting")
		defer func() {
			t.running = false
		}()
		finished := false
		for !finished {
			select {
			case <-t.operationContext.Done():
				finished = true
			case <-time.After(interval):
				log.WithFields(t.LogTags).Debug("Calling handler")
				if err := handler(); err != nil {
					log.WithError(err).WithFields(t.LogTags).Error("Handler failed")
				}
				if oneShot {
					return
				}
			}
		}
	}()
	return nil
}

/*
Stop stops the timer
*/
func (t *intervalTimerImpl) Stop() error {
	if t.contextCancel != nil {
		log.WithFields(t.LogTags).Info("Stopping timer loop")
		t.contextCancel()
	}
	return nil
}

// ========================================================================================

// Sequencer is a helper interface for returning a sequence of numbers
type Sequencer interface {
	/*
		NextValue returns the next value in the sequence
	*/
	NextValue() float64
}

// exponentialSequence is a helper interface to get an exponential sequence from a
// starting value
type exponentialSequence struct {
	current    float64
	growthRate float64
}

/*
NextValue returns the next value in the sequence
*/
func (s *exponentialSequence) NextValue() float64 {
	nextValue := s.current * s.growthRate
	s.current = nextValue
	return nextValue
}

/*
GetExponentialSeq define an exponential sequencer

	@param initial float64 - initial value
	@param growthRate float64 - EXP change rate
	@return an Sequencer instance
*/
func GetExponentialSeq(initial float64, growthRate float64) (Sequencer, error) {
	if growthRate < 1.0 {
		return nil, fmt.Errorf("growth rate of exponential sequence must be > 1.0")
	}
	return &exponentialSequence{current: initial, growthRate: growthRate}, nil
}
