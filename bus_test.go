package goutils

import (
	"context"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestMessageTopicOnePubOneSub(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	ctxt, cancel := context.WithCancel(context.Background())
	defer cancel()

	testTopic, err := GetNewMessageTopicInstance(
		ctxt, uuid.NewString(), log.Fields{"unit-testiung": "testing"},
	)
	assert.Nil(err)

	type testMsg struct {
		msg string
	}

	// Case 0: create subscription
	subscriber := uuid.NewString()
	subChan, err := testTopic.CreateSubscription(ctxt, subscriber, 0)
	assert.Nil(err)

	// Case 1: blocking publish
	{
		test := testMsg{msg: uuid.NewString()}
		complete := make(chan bool, 1)

		lclCtxt, lclCancel := context.WithTimeout(ctxt, time.Millisecond*100)

		go func() {
			assert.Nil(testTopic.Publish(lclCtxt, &test, 0))
			complete <- true
		}()

		select {
		case <-lclCtxt.Done():
			assert.False(true, "msg timed out")
		case msg, ok := <-subChan:
			assert.True(ok)
			asString, ok := msg.(*testMsg)
			assert.True(ok)
			assert.Equal(test.msg, asString.msg)
		}

		select {
		case <-lclCtxt.Done():
			assert.False(true, "test timed out")
		case <-complete:
			break
		}

		lclCancel()
	}

	// Case 2: non-blocking publish, but not receiving
	{
		test := testMsg{msg: uuid.NewString()}

		lclCtxt, lclCancel := context.WithTimeout(ctxt, time.Millisecond*100)

		assert.Nil(testTopic.Publish(lclCtxt, &test, time.Millisecond*10))

		lclCancel()
	}

	// Case 3: non-blocking publish, prepared to receive
	{
		test := testMsg{msg: uuid.NewString()}
		complete := make(chan bool, 1)

		lclCtxt, lclCancel := context.WithTimeout(ctxt, time.Millisecond*100)

		go func() {
			assert.Nil(testTopic.Publish(lclCtxt, &test, time.Millisecond*10))
			complete <- true
		}()

		select {
		case <-lclCtxt.Done():
			assert.False(true, "msg timed out")
		case msg, ok := <-subChan:
			assert.True(ok)
			asString, ok := msg.(*testMsg)
			assert.True(ok)
			assert.Equal(test.msg, asString.msg)
		}

		select {
		case <-lclCtxt.Done():
			assert.False(true, "test timed out")
		case <-complete:
			break
		}

		lclCancel()
	}

	// Delete subscription
	assert.Nil(testTopic.DeleteSubscription(ctxt, subscriber))
}

func TestMessageTopicOnePubMultiSub(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	ctxt, cancel := context.WithCancel(context.Background())
	defer cancel()

	testTopic, err := GetNewMessageTopicInstance(
		ctxt, uuid.NewString(), log.Fields{"unit-testiung": "testing"},
	)
	assert.Nil(err)

	type testMsg struct {
		msg string
	}

	// Case 0: create subscriptions
	subNames := []string{}
	subs := map[string]chan interface{}{}
	for itr := 0; itr < 2; itr++ {
		subscriber := uuid.NewString()
		subChan, err := testTopic.CreateSubscription(ctxt, subscriber, 0)
		assert.Nil(err)
		subs[subscriber] = subChan
		subNames = append(subNames, subscriber)
	}

	// Case 1: blocking publish
	{
		test := testMsg{msg: uuid.NewString()}
		complete := make(chan bool, 3)

		lclCtxt, lclCancel := context.WithTimeout(ctxt, time.Millisecond*100)

		go func() {
			assert.Nil(testTopic.Publish(lclCtxt, &test, 0))
			complete <- true
		}()

		h := func(channel chan interface{}) {
			select {
			case <-lclCtxt.Done():
				assert.False(true, "msg timed out")
			case msg, ok := <-channel:
				assert.True(ok)
				asString, ok := msg.(*testMsg)
				assert.True(ok)
				assert.Equal(test.msg, asString.msg)
				complete <- true
			}
		}

		for _, channel := range subs {
			go h(channel)
		}

		for itr := 0; itr < 3; itr++ {
			select {
			case <-lclCtxt.Done():
				assert.False(true, "test timed out")
			case <-complete:
			}
		}

		lclCancel()
	}

	// Case 2: non-blocking publish, but one not receiving
	{
		test := testMsg{msg: uuid.NewString()}
		complete := make(chan bool, 1)

		lclCtxt, lclCancel := context.WithTimeout(ctxt, time.Millisecond*100)

		go func() {
			assert.Nil(testTopic.Publish(lclCtxt, &test, time.Millisecond*10))
			complete <- true
		}()

		select {
		case <-lclCtxt.Done():
			assert.False(true, "msg timed out")
		case msg, ok := <-subs[subNames[0]]:
			assert.True(ok)
			asString, ok := msg.(*testMsg)
			assert.True(ok)
			assert.Equal(test.msg, asString.msg)
		}

		select {
		case <-lclCtxt.Done():
			assert.False(true, "test timed out")
		case <-complete:
		}

		select {
		case <-lclCtxt.Done():
			break
		case <-subs[subNames[1]]:
			assert.False(true, "should not have received a message")
		}

		lclCancel()
	}

	// Case 3: non-blocking publish, prepared to receive
	{
		test := testMsg{msg: uuid.NewString()}
		complete := make(chan bool, 3)

		lclCtxt, lclCancel := context.WithTimeout(ctxt, time.Millisecond*100)

		go func() {
			assert.Nil(testTopic.Publish(lclCtxt, &test, time.Millisecond*10))
			complete <- true
		}()

		h := func(channel chan interface{}) {
			select {
			case <-lclCtxt.Done():
				assert.False(true, "msg timed out")
			case msg, ok := <-channel:
				assert.True(ok)
				asString, ok := msg.(*testMsg)
				assert.True(ok)
				assert.Equal(test.msg, asString.msg)
				complete <- true
			}
		}

		for _, channel := range subs {
			go h(channel)
		}

		for itr := 0; itr < 3; itr++ {
			select {
			case <-lclCtxt.Done():
				assert.False(true, "test timed out")
			case <-complete:
			}
		}

		lclCancel()
	}

	// Delete subscription
	for subName := range subs {
		assert.Nil(testTopic.DeleteSubscription(ctxt, subName))
	}
}
