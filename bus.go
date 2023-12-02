package goutils

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apex/log"
)

// MessageBus an application scoped local message bus
type MessageBus interface{}

// MessageTopic a message bus topic, responsible for managing its child subscriptions.
type MessageTopic interface {
	/*
		Publish publish a message on the topic in parallel.

			@param ctxt context.Context - execution context
			@param message interface{} - the message to send
			@param blockFor time.Duration - how long to block for the publish to complete. If >0,
			    this is a non-blocking call; blocking call otherwise.
	*/
	Publish(ctxt context.Context, message interface{}, blockFor time.Duration) error

	/*
		CreateSubscription create a new topic subscription

			@param ctxt context.Context - execution context
			@param subscriber string - name of the subscription
			@param bufferLen int - length of message buffer
			@returns the channel to receive messages on
	*/
	CreateSubscription(
		ctxt context.Context, subscriber string, bufferLen int,
	) (chan interface{}, error)

	/*
		DeleteSubscription delete an existing topic subscription

			@param ctxt context.Context - execution context
			@param subscriber string - subscription to delete
	*/
	DeleteSubscription(ctxt context.Context, subscriber string) error
}

// messageTopicImpl implements MessageTopic
type messageTopicImpl struct {
	Component
	topic string

	// manage access to subscriptions
	lock sync.RWMutex

	// collection of subscription of this topic
	subscriptions map[string]chan interface{}

	operationContext context.Context
	contextCancel    context.CancelFunc
}

/*
GetNewMessageTopicInstance get message topic instance

	@param parentCtxt context.Context - parent execution context
	@param topic string - topic name
	@param logTags log.Fields - metadata fields to include in the logs
	@return new MessageTopic instance
*/
func GetNewMessageTopicInstance(
	parentCtxt context.Context, topic string, logTags log.Fields,
) (MessageTopic, error) {
	optCtxt, cancel := context.WithCancel(parentCtxt)

	instance := &messageTopicImpl{
		Component:        Component{LogTags: logTags},
		topic:            topic,
		lock:             sync.RWMutex{},
		subscriptions:    make(map[string]chan interface{}),
		operationContext: optCtxt,
		contextCancel:    cancel,
	}

	return instance, nil
}

/*
Publish publish a message on the topic in parallel.

	@param ctxt context.Context - execution context
	@param message interface{} - the message to send
	@param blockFor time.Duration - how long to block for the publish to complete. If >0,
	    this is a non-blocking call; blocking call otherwise.
*/
func (t *messageTopicImpl) Publish(
	ctxt context.Context, message interface{}, blockFor time.Duration,
) error {
	logTags := t.GetLogTagsForContext(ctxt)

	blocking := blockFor <= 0

	var lclCtxt context.Context
	var lclCancel context.CancelFunc

	if blocking {
		lclCtxt, lclCancel = context.WithCancel(ctxt)
	} else {
		lclCtxt, lclCancel = context.WithTimeout(ctxt, blockFor)
	}

	defer lclCancel()

	// Duplicate the message to all subscribers in parallel
	wg := sync.WaitGroup{}

	// Function to write the message to the subscriber
	tx := func(subscriber string, buffer chan interface{}) {
		wg.Done()
		// Write message
		select {
		case buffer <- message:
			break
		case <-lclCtxt.Done():
			// write timed out
			log.
				WithFields(logTags).
				WithField("subscriber", subscriber).
				Error("Timed out sending message to subscriber")
		}

		log.
			WithFields(logTags).
			WithField("subscriber", subscriber).
			Debug("Published message to subscriber")
	}

	t.lock.RLock()
	defer t.lock.RUnlock()

	// Start all the message sending helpers
	wg.Add(len(t.subscriptions))
	for subscriber, channel := range t.subscriptions {
		go tx(subscriber, channel)
	}

	if blocking {
		wg.Wait()
		return nil
	}
	return TimeBoundedWaitGroupWait(lclCtxt, &wg, blockFor)
}

/*
CreateSubscription create a new topic subscription

	@param ctxt context.Context - execution context
	@param subscriber string - name of the subscription
	@param bufferLen int - length of message buffer
	@returns the channel to receive messages on
*/
func (t *messageTopicImpl) CreateSubscription(
	ctxt context.Context, subscriber string, bufferLen int,
) (chan interface{}, error) {
	logTags := t.GetLogTagsForContext(ctxt)

	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.subscriptions[subscriber]; ok {
		return nil, fmt.Errorf("subscription '%s' already exist", subscriber)
	}

	msgBuffer := make(chan interface{}, bufferLen)

	t.subscriptions[subscriber] = msgBuffer

	log.
		WithFields(logTags).
		WithField("buffer-len", bufferLen).
		Infof("Created new subscription '%s'", subscriber)

	return msgBuffer, nil
}

/*
DeleteSubscription delete an existing topic subscription

	@param ctxt context.Context - execution context
	@param subscriber string - subscription to delete
*/
func (t *messageTopicImpl) DeleteSubscription(ctxt context.Context, subscriber string) error {
	logTags := t.GetLogTagsForContext(ctxt)

	t.lock.Lock()
	defer t.lock.Unlock()

	existing, ok := t.subscriptions[subscriber]
	if !ok {
		return fmt.Errorf("unknown subscription '%s'", subscriber)
	}
	log.
		WithFields(logTags).
		Debugf("Closing subscription '%s' channel", subscriber)
	close(existing)
	log.
		WithFields(logTags).
		Infof("Closed subscription '%s' channel", subscriber)
	delete(t.subscriptions, subscriber)

	log.
		WithFields(logTags).
		Infof("Deleted subscription '%s'", subscriber)

	return nil
}
