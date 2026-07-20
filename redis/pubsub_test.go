package redis_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alwitt/goutils/redis"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	mocktest "github.com/alwitt/goutils/mocks/test"
)

// testPubSubMessage a minimal `redis.QueueMessageEnvelope` implementation used to drive the
// PubSub publish path.
type testPubSubMessage struct {
	payload string
}

// StringPayload return its payload as a string
func (m testPubSubMessage) StringPayload() (string, error) {
	return m.payload, nil
}

// subReady is how long to wait after Start before publishing. Redis only delivers to
// subscribers that were already subscribed when the message was published, and Start
// establishes the subscription asynchronously, so publishers must let it settle first.
const subReady = time.Millisecond * 100

func TestRedisPubSubBasic(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	testTopic := uuid.NewString()

	uut, err := client.Subscribe(utCtx, uuid.NewString(), []string{testTopic})
	assert.Nil(err)

	// Collect delivered messages through the mock handler. Because delivery is async, a
	// Run hook records each received payload and signals a channel the test can wait on.
	const messageCount = 5
	collector := mocktest.NewUnitTestCallbackCollector(t)
	received := make([]string, 0, messageCount)
	gotOne := make(chan struct{}, messageCount)
	collector.EXPECT().
		CollectRedisSubscribeMsgs(mock.Anything, mock.Anything).
		Run(func(_ context.Context, msg redis.PubSubMessage) {
			assert.Equal(testTopic, msg.Topic)
			payload, err := msg.Message.StringPayload()
			assert.Nil(err)
			received = append(received, payload)
			gotOne <- struct{}{}
		}).
		Return().
		Times(messageCount)

	assert.Nil(uut.Start(utCtx, collector.CollectRedisSubscribeMsgs))
	defer func() {
		assert.Nil(uut.Stop(utCtx))
	}()

	// Give the subscription time to register before publishing
	time.Sleep(subReady)

	// Publish a sequence of messages
	expected := make([]string, 0, messageCount)
	for i := 0; i < messageCount; i++ {
		msg := testPubSubMessage{payload: uuid.NewString()}
		expected = append(expected, msg.payload)
		assert.Nil(client.Publish(utCtx, redis.PubSubMessage{Topic: testTopic, Message: msg}))
	}

	// Wait for every published message to be delivered to the handler
	for i := 0; i < messageCount; i++ {
		select {
		case <-gotOne:
		case <-time.After(time.Second * 2):
			assert.Fail("timed out waiting for delivered message")
		}
	}

	// The handler was invoked, serially, once per message in publish order
	assert.Equal(expected, received)
}

func TestRedisPubSubMultipleTopics(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	// A single subscriber listening on two distinct topics
	topicA := uuid.NewString()
	topicB := uuid.NewString()
	// A third topic the subscriber does NOT listen on, to prove isolation
	topicUnsub := uuid.NewString()

	uut, err := client.Subscribe(utCtx, uuid.NewString(), []string{topicA, topicB})
	assert.Nil(err)

	// Delivery order across topics is not guaranteed, so collect into a topic->payload map.
	collector := mocktest.NewUnitTestCallbackCollector(t)
	var lock sync.Mutex
	byTopic := map[string]string{}
	gotOne := make(chan struct{}, 4)
	collector.EXPECT().
		CollectRedisSubscribeMsgs(mock.Anything, mock.Anything).
		Run(func(_ context.Context, msg redis.PubSubMessage) {
			payload, err := msg.Message.StringPayload()
			assert.Nil(err)
			lock.Lock()
			byTopic[msg.Topic] = payload
			lock.Unlock()
			gotOne <- struct{}{}
		}).
		Return()

	assert.Nil(uut.Start(utCtx, collector.CollectRedisSubscribeMsgs))
	defer func() {
		assert.Nil(uut.Stop(utCtx))
	}()

	time.Sleep(subReady)

	msgA := testPubSubMessage{payload: uuid.NewString()}
	msgB := testPubSubMessage{payload: uuid.NewString()}
	msgUnsub := testPubSubMessage{payload: uuid.NewString()}

	// Publish one message per topic, including the un-subscribed topic
	assert.Nil(client.Publish(utCtx, redis.PubSubMessage{Topic: topicA, Message: msgA}))
	assert.Nil(client.Publish(utCtx, redis.PubSubMessage{Topic: topicB, Message: msgB}))
	assert.Nil(client.Publish(utCtx, redis.PubSubMessage{Topic: topicUnsub, Message: msgUnsub}))

	// The handler is invoked exactly for the two messages on the subscribed topics
	for i := 0; i < 2; i++ {
		select {
		case <-gotOne:
		case <-time.After(time.Second * 2):
			assert.Fail("timed out waiting for delivered message")
		}
	}

	lock.Lock()
	assert.Equal(msgA.payload, byTopic[topicA])
	assert.Equal(msgB.payload, byTopic[topicB])
	lock.Unlock()

	// The message on the un-subscribed topic must never be delivered
	select {
	case <-gotOne:
		assert.Fail("received message on un-subscribed topic")
	case <-time.After(time.Millisecond * 200):
		// expected: nothing else delivered
	}
}

func TestRedisPubSubFanOut(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	// Fan-out: several independent subscribers on the same topic each receive every message
	const subscriberCount = 3
	testTopic := uuid.NewString()

	msg := testPubSubMessage{payload: uuid.NewString()}

	type sub struct {
		runner redis.Subscriber
		gotOne chan struct{}
	}
	subs := make([]sub, subscriberCount)
	for i := range subs {
		runner, err := client.Subscribe(utCtx, uuid.NewString(), []string{testTopic})
		assert.Nil(err)

		gotOne := make(chan struct{}, 1)
		collector := mocktest.NewUnitTestCallbackCollector(t)
		collector.EXPECT().
			CollectRedisSubscribeMsgs(mock.Anything, mock.Anything).
			Run(func(_ context.Context, got redis.PubSubMessage) {
				assert.Equal(testTopic, got.Topic)
				payload, err := got.Message.StringPayload()
				assert.Nil(err)
				assert.Equal(msg.payload, payload)
				gotOne <- struct{}{}
			}).
			Return().
			Once()

		assert.Nil(runner.Start(utCtx, collector.CollectRedisSubscribeMsgs))
		subs[i] = sub{runner: runner, gotOne: gotOne}
	}
	defer func() {
		for _, s := range subs {
			assert.Nil(s.runner.Stop(utCtx))
		}
	}()

	time.Sleep(subReady)

	// Publish a single message; every subscriber's handler receives it
	assert.Nil(client.Publish(utCtx, redis.PubSubMessage{Topic: testTopic, Message: msg}))

	for i := range subs {
		select {
		case <-subs[i].gotOne:
		case <-time.After(time.Second * 2):
			assert.Fail("subscriber timed out waiting for the fan-out message")
		}
	}
}

func TestRedisPubSubNoSubscriber(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	// Publishing to a topic with no subscribers is a best-effort no-op: the message is
	// silently dropped and the publish still succeeds (Redis reports zero receivers).
	testTopic := uuid.NewString()
	msg := testPubSubMessage{payload: uuid.NewString()}
	assert.Nil(client.Publish(utCtx, redis.PubSubMessage{Topic: testTopic, Message: msg}))
}

func TestRedisPubSubStartValidation(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	uut, err := client.Subscribe(utCtx, uuid.NewString(), []string{uuid.NewString()})
	assert.Nil(err)

	// Starting with no handler is a bad-input error
	assert.Error(uut.Start(utCtx, nil))
}

func TestRedisPubSubStartStopLifecycle(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	testTopic := uuid.NewString()
	uut, err := client.Subscribe(utCtx, uuid.NewString(), []string{testTopic})
	assert.Nil(err)

	// A no-op handler: this test never publishes, so it is never invoked
	noop := func(_ context.Context, _ redis.PubSubMessage) {}

	// Stopping before starting is an error (nothing is running)
	assert.Error(uut.Stop(utCtx))

	assert.Nil(uut.Start(utCtx, noop))

	// Starting an already-running subscriber is an error
	assert.Error(uut.Start(utCtx, noop))

	// A clean Stop returns in time
	assert.Nil(uut.Stop(utCtx))

	// Stopping a second time is an error (already stopped)
	assert.Error(uut.Stop(utCtx))

	// The subscriber can be started again after being stopped
	assert.Nil(uut.Start(utCtx, noop))
	assert.Nil(uut.Stop(utCtx))
}

func TestRedisPubSubStopUnblocksSlowHandler(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	testTopic := uuid.NewString()
	uut, err := client.Subscribe(utCtx, uuid.NewString(), []string{testTopic})
	assert.Nil(err)

	// A handler that observes the provided (working) context and returns when it is
	// cancelled. When Stop is called mid-handler, the context cancels and the handler
	// unblocks, so the reader goroutine can exit and Stop returns in time.
	inHandler := make(chan struct{}, 1)
	handler := func(ctx context.Context, _ redis.PubSubMessage) {
		inHandler <- struct{}{}
		<-ctx.Done()
	}

	assert.Nil(uut.Start(utCtx, handler))

	time.Sleep(subReady)

	// Publish a message the handler will pick up and then park on until Stop cancels the ctx
	msg := testPubSubMessage{payload: uuid.NewString()}
	assert.Nil(client.Publish(utCtx, redis.PubSubMessage{Topic: testTopic, Message: msg}))

	// Ensure the handler is actually executing (and blocked) before stopping
	select {
	case <-inHandler:
	case <-time.After(time.Second * 2):
		assert.Fail("handler was never invoked")
	}

	// Stop must not deadlock on the in-flight handler; it returns within its own bound
	stopCtx, cancel := context.WithTimeout(utCtx, time.Second*10)
	defer cancel()
	assert.Nil(uut.Stop(stopCtx))
}

func TestRedisPubSubHandlerPanicIsolated(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtx := context.Background()

	redisConnect := getRedisConnectParamForTest(assert)
	client, err := redis.NewClient(utCtx, redisConnect)
	assert.Nil(err)

	testTopic := uuid.NewString()
	uut, err := client.Subscribe(utCtx, uuid.NewString(), []string{testTopic})
	assert.Nil(err)

	// The first message panics the handler; the subscriber must survive and still deliver
	// the second message.
	var count int
	delivered := make(chan string, 2)
	handler := func(_ context.Context, msg redis.PubSubMessage) {
		count++
		if count == 1 {
			panic("boom")
		}
		payload, err := msg.Message.StringPayload()
		assert.Nil(err)
		delivered <- payload
	}

	assert.Nil(uut.Start(utCtx, handler))
	defer func() {
		assert.Nil(uut.Stop(utCtx))
	}()

	time.Sleep(subReady)

	// First publish triggers the panic (swallowed by the subscriber)
	assert.Nil(client.Publish(utCtx, redis.PubSubMessage{Topic: testTopic, Message: testPubSubMessage{payload: uuid.NewString()}}))

	// Second publish must still be delivered, proving the reader loop survived the panic
	second := testPubSubMessage{payload: uuid.NewString()}
	assert.Nil(client.Publish(utCtx, redis.PubSubMessage{Topic: testTopic, Message: second}))

	select {
	case got := <-delivered:
		assert.Equal(second.payload, got)
	case <-time.After(time.Second * 2):
		assert.Fail("subscriber did not survive a panicking handler")
	}
}
