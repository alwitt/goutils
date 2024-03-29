package goutils_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/alwitt/goutils"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
)

func createPubSubClientUsingAPIToken(
	ctxt context.Context, projectID string, token string,
) (*pubsub.Client, error) {
	return pubsub.NewClient(ctxt, projectID, option.WithAPIKey(token))
}

func createTestPubSubClient(ctxt context.Context) (*pubsub.Client, error) {
	testGCPProjectID := os.Getenv("UNITTEST_GCP_PROJECT_ID")

	testToken := os.Getenv("UNITTEST_GCP_API_TOKEN")
	if testToken != "" {
		return createPubSubClientUsingAPIToken(ctxt, testGCPProjectID, testToken)
	}
	return goutils.CreateBasicGCPPubSubClient(ctxt, testGCPProjectID)
}

func TestPubSubTopicCRUD(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	coreClient, err := createTestPubSubClient(utCtxt)
	assert.Nil(err)

	uut, err := goutils.GetNewPubSubClientInstance(
		coreClient, log.Fields{"instance": "unit-tester"}, nil,
	)
	assert.Nil(err)

	assert.Nil(uut.UpdateLocalTopicCache(utCtxt))

	// Case 0: unknown topic
	{
		_, err = uut.GetTopic(utCtxt, uuid.NewString())
		assert.NotNil(err)
	}

	// Case 1: create new topic
	topic0 := fmt.Sprintf("goutil-ut-topic-0-%s", uuid.NewString())
	assert.Nil(uut.CreateTopic(
		utCtxt, topic0, &pubsub.TopicConfig{RetentionDuration: time.Minute * 10},
	))
	{
		config, err := uut.GetTopic(utCtxt, topic0)
		assert.Nil(err)
		assert.Equal(topic0, config.ID())
		assert.Equal(time.Minute*10, config.RetentionDuration)
	}

	// Case 2: create same topic again
	assert.Nil(uut.CreateTopic(utCtxt, topic0, nil))

	// Case 3: update the topic
	assert.Nil(uut.UpdateTopic(
		utCtxt, topic0, pubsub.TopicConfigToUpdate{RetentionDuration: time.Minute * 30},
	))
	{
		config, err := uut.GetTopic(utCtxt, topic0)
		assert.Nil(err)
		assert.Equal(topic0, config.ID())
		assert.Equal(time.Minute*30, config.RetentionDuration)
	}

	// Case 4: create another topic
	topic1 := fmt.Sprintf("goutil-ut-topic-0-%s", uuid.NewString())
	assert.Nil(uut.CreateTopic(
		utCtxt, topic1, &pubsub.TopicConfig{RetentionDuration: time.Minute * 15},
	))
	{
		config, err := uut.GetTopic(utCtxt, topic1)
		assert.Nil(err)
		assert.Equal(topic1, config.ID())
		assert.Equal(time.Minute*15, config.RetentionDuration)
		config, err = uut.GetTopic(utCtxt, topic0)
		assert.Nil(err)
		assert.Equal(topic0, config.ID())
		assert.Equal(time.Minute*30, config.RetentionDuration)
	}

	// Clean up
	assert.Nil(uut.DeleteTopic(utCtxt, topic0))
	assert.Nil(uut.DeleteTopic(utCtxt, topic1))
}

func TestPubSubTopicSync(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	coreClient0, err := createTestPubSubClient(utCtxt)
	assert.Nil(err)

	coreClient1, err := createTestPubSubClient(utCtxt)
	assert.Nil(err)

	uut0, err := goutils.GetNewPubSubClientInstance(
		coreClient0, log.Fields{"instance": "unit-tester-0"}, nil,
	)
	assert.Nil(err)

	uut1, err := goutils.GetNewPubSubClientInstance(
		coreClient1, log.Fields{"instance": "unit-tester-1"}, nil,
	)
	assert.Nil(err)

	// Create new topic
	topic0 := fmt.Sprintf("goutil-ut-topic-1-%s", uuid.NewString())
	assert.Nil(uut0.CreateTopic(
		utCtxt, topic0, &pubsub.TopicConfig{RetentionDuration: time.Minute * 33},
	))

	// Sync from the other client
	assert.Nil(uut1.UpdateLocalTopicCache(utCtxt))
	{
		config, err := uut1.GetTopic(utCtxt, topic0)
		assert.Nil(err)
		assert.Equal(topic0, config.ID())
		assert.Equal(time.Minute*33, config.RetentionDuration)
	}

	// Clean up
	assert.Nil(uut0.DeleteTopic(utCtxt, topic0))
}

func TestPubSubSubscriptionCRUD(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	coreClient, err := createTestPubSubClient(utCtxt)
	assert.Nil(err)

	uut, err := goutils.GetNewPubSubClientInstance(
		coreClient, log.Fields{"instance": "unit-tester"}, nil,
	)
	assert.Nil(err)

	assert.Nil(uut.UpdateLocalTopicCache(utCtxt))
	assert.Nil(uut.UpdateLocalSubscriptionCache(utCtxt))

	// Case 0: unknown subscription
	{
		_, err := uut.GetSubscription(utCtxt, uuid.NewString())
		assert.NotNil(err)
	}

	// Create test topic
	testTopic := fmt.Sprintf("goutil-ut-topic-2-%s", uuid.NewString())
	assert.Nil(uut.CreateTopic(
		utCtxt, testTopic, &pubsub.TopicConfig{RetentionDuration: time.Minute * 10},
	))

	// Case 1: create subscription
	subscribe0 := fmt.Sprintf("goutil-ut-sub-2-%s", uuid.NewString())
	assert.Nil(uut.CreateSubscription(utCtxt, testTopic, subscribe0, pubsub.SubscriptionConfig{
		AckDeadline:       time.Second * 60,
		RetentionDuration: time.Hour * 4,
	}))
	{
		config, err := uut.GetSubscription(utCtxt, subscribe0)
		assert.Nil(err)
		assert.Equal(testTopic, config.Topic.ID())
		assert.Equal(subscribe0, config.ID())
		assert.Equal(time.Second*60, config.AckDeadline)
		assert.Equal(time.Hour*4, config.RetentionDuration)
	}

	// Case 2: create subscription against unknown topic
	assert.NotNil(uut.CreateSubscription(
		utCtxt,
		uuid.NewString(),
		fmt.Sprintf("goutil-ut-sub-2-%s", uuid.NewString()),
		pubsub.SubscriptionConfig{},
	))

	// Case 3: create using the same parameters
	assert.Nil(uut.CreateSubscription(utCtxt, testTopic, subscribe0, pubsub.SubscriptionConfig{
		AckDeadline:       time.Second * 60,
		RetentionDuration: time.Hour * 4,
	}))

	// Case 4: update subscription config
	assert.Nil(uut.UpdateSubscription(utCtxt, subscribe0, pubsub.SubscriptionConfigToUpdate{
		RetentionDuration: time.Hour * 72,
	}))
	{
		config, err := uut.GetSubscription(utCtxt, subscribe0)
		assert.Nil(err)
		assert.Equal(testTopic, config.Topic.ID())
		assert.Equal(subscribe0, config.ID())
		assert.Equal(time.Second*60, config.AckDeadline)
		assert.Equal(time.Hour*72, config.RetentionDuration)
	}

	// Clean up
	assert.Nil(uut.DeleteSubscription(utCtxt, subscribe0))
	assert.Nil(uut.DeleteTopic(utCtxt, testTopic))
}

func TestPubSubDataPassing(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	coreClient, err := createTestPubSubClient(utCtxt)
	assert.Nil(err)

	uut, err := goutils.GetNewPubSubClientInstance(
		coreClient, log.Fields{"instance": "unit-tester"}, nil,
	)
	assert.Nil(err)

	assert.Nil(uut.UpdateLocalTopicCache(utCtxt))
	assert.Nil(uut.UpdateLocalSubscriptionCache(utCtxt))

	// Create test topic
	testTopic := fmt.Sprintf("goutil-ut-topic-3-%s", uuid.NewString())
	assert.Nil(uut.CreateTopic(
		utCtxt, testTopic, &pubsub.TopicConfig{RetentionDuration: time.Minute * 10},
	))

	// Create subscription
	testSubscription := fmt.Sprintf("goutil-ut-sub-3-%s", uuid.NewString())
	assert.Nil(uut.CreateSubscription(utCtxt, testTopic, testSubscription, pubsub.SubscriptionConfig{
		AckDeadline:       time.Second * 10,
		RetentionDuration: time.Minute * 10,
	}))

	// support for receiving messages
	type msgPkg struct {
		msg  []byte
		meta map[string]string
	}
	rxMsg := make(chan msgPkg)
	receiveMsg := func(ctxt context.Context, ts time.Time, msg []byte, meta map[string]string) error {
		rxMsg <- msgPkg{msg: msg, meta: meta}
		return nil
	}

	// Start receiving on subscription
	log.Debug("Starting message subscription receive")
	wg := sync.WaitGroup{}
	wg.Add(1)
	rxCtxt, rxCancel := context.WithCancel(utCtxt)
	go func() {
		defer wg.Done()
		assert.Nil(uut.Subscribe(rxCtxt, testSubscription, receiveMsg))
	}()
	log.Debug("Started message subscription receive")

	// Send messages
	log.Debug("Publishing test messages")
	testMsgs := map[string]map[string]string{}
	for itr := 0; itr < 3; itr++ {
		msg := uuid.NewString()
		meta := map[string]string{
			uuid.NewString(): uuid.NewString(), uuid.NewString(): uuid.NewString(),
		}
		_, err := uut.Publish(utCtxt, testTopic, []byte(msg), meta, true)
		assert.Nil(err)
		testMsgs[msg] = meta
	}
	log.Debug("Published test messages")

	// Wait for message to come back
	{
		lclCtxt, cancel := context.WithTimeout(utCtxt, time.Second*5)
		defer cancel()

		processedMsgs := map[string]map[string]string{}
		for itr := 0; itr < 3; itr++ {
			select {
			case <-lclCtxt.Done():
				assert.False(true, "PubSub receive timeout")
			case msg, ok := <-rxMsg:
				assert.True(ok)
				processedMsgs[string(msg.msg)] = msg.meta
			}
		}

		assert.EqualValues(testMsgs, processedMsgs)
	}

	// Clean up
	rxCancel()
	wg.Wait()
	assert.Nil(uut.DeleteSubscription(utCtxt, testSubscription))
	assert.Nil(uut.DeleteTopic(utCtxt, testTopic))
}

func TestPubSubMultiReaderOneSubcription(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	coreClient, err := createTestPubSubClient(utCtxt)
	assert.Nil(err)

	uut0, err := goutils.GetNewPubSubClientInstance(
		coreClient, log.Fields{"instance": "unit-tester-0"}, nil,
	)
	assert.Nil(err)

	assert.Nil(uut0.UpdateLocalTopicCache(utCtxt))
	assert.Nil(uut0.UpdateLocalSubscriptionCache(utCtxt))

	// Create test topic
	testTopic := fmt.Sprintf("goutil-ut-topic-4-%s", uuid.NewString())
	assert.Nil(uut0.CreateTopic(
		utCtxt, testTopic, &pubsub.TopicConfig{RetentionDuration: time.Minute * 10},
	))

	// Create subscription
	testSubscription := fmt.Sprintf("goutil-ut-sub-4-%s", uuid.NewString())
	assert.Nil(uut0.CreateSubscription(utCtxt, testTopic, testSubscription, pubsub.SubscriptionConfig{
		AckDeadline:       time.Second * 10,
		RetentionDuration: time.Minute * 10,
	}))

	// Create second client
	uut1, err := goutils.GetNewPubSubClientInstance(
		coreClient, log.Fields{"instance": "unit-tester-1"}, nil,
	)
	assert.Nil(err)
	assert.Nil(uut1.UpdateLocalTopicCache(utCtxt))
	assert.Nil(uut1.UpdateLocalSubscriptionCache(utCtxt))

	type msgWrap struct {
		rxIdx int
		msg   []byte
		meta  map[string]string
	}

	// Support for receiving messages
	rxMsg := make(chan msgWrap)
	receiveMsg0 := func(ctxt context.Context, ts time.Time, msg []byte, meta map[string]string) error {
		rxMsg <- msgWrap{rxIdx: 0, msg: msg, meta: meta}
		return nil
	}
	receiveMsg1 := func(ctxt context.Context, ts time.Time, msg []byte, meta map[string]string) error {
		rxMsg <- msgWrap{rxIdx: 1, msg: msg, meta: meta}
		return nil
	}

	// Start receiving on subscription
	log.Debug("Starting message subscription receive")
	wg := sync.WaitGroup{}
	wg.Add(2)
	rxCtxt, rxCancel := context.WithCancel(utCtxt)
	receiver := func(
		idx int,
		client goutils.PubSubClient,
		handler func(ctxt context.Context, ts time.Time, msg []byte, meta map[string]string) error,
	) {
		defer wg.Done()
		log.Debugf("Start MSG Receiver %d", idx)
		assert.Nil(client.Subscribe(rxCtxt, testSubscription, handler))
	}
	go receiver(0, uut0, receiveMsg0)
	go receiver(1, uut1, receiveMsg1)
	log.Debug("Started message subscription receive")

	// Send messages
	log.Debug("Publishing test messages")
	testMsgs := map[string]map[string]string{}
	for itr := 0; itr < 3; itr++ {
		msg := uuid.NewString()
		meta := map[string]string{
			uuid.NewString(): uuid.NewString(), uuid.NewString(): uuid.NewString(),
		}
		_, err := uut0.Publish(utCtxt, testTopic, []byte(msg), meta, true)
		assert.Nil(err)
		testMsgs[msg] = meta
	}
	log.Debug("Published test messages")

	// Wait for message to come back
	{
		lclCtxt, cancel := context.WithTimeout(utCtxt, time.Second*5)
		defer cancel()

		processedMsgs := map[string]map[string]string{}
		for itr := 0; itr < 3; itr++ {
			select {
			case <-lclCtxt.Done():
				assert.False(true, "PubSub receive timeout")
			case msg, ok := <-rxMsg:
				assert.True(ok)
				processedMsgs[string(msg.msg)] = msg.meta
			}
		}

		assert.EqualValues(testMsgs, processedMsgs)
	}
	{
		// Verify no more messages show up
		lclCtxt, cancel := context.WithTimeout(utCtxt, time.Second*2)
		defer cancel()

		select {
		case <-lclCtxt.Done():
			break
		case <-rxMsg:
			assert.False(true, "Received unexpected message")
		}
	}

	// Clean up
	rxCancel()
	wg.Wait()
	assert.Nil(uut0.DeleteSubscription(utCtxt, testSubscription))
	assert.Nil(uut0.DeleteTopic(utCtxt, testTopic))
}

func TestPubSubMultiSubscriptionOneTopic(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	coreClient, err := createTestPubSubClient(utCtxt)
	assert.Nil(err)

	uut, err := goutils.GetNewPubSubClientInstance(
		coreClient, log.Fields{"instance": "unit-tester"}, nil,
	)
	assert.Nil(err)

	assert.Nil(uut.UpdateLocalTopicCache(utCtxt))
	assert.Nil(uut.UpdateLocalSubscriptionCache(utCtxt))

	// Create test topic
	testTopic := fmt.Sprintf("goutil-ut-topic-5-%s", uuid.NewString())
	assert.Nil(uut.CreateTopic(
		utCtxt, testTopic, &pubsub.TopicConfig{RetentionDuration: time.Minute * 10},
	))

	// Create subscription
	testSubscription0 := fmt.Sprintf("goutil-ut-sub-5-%s", uuid.NewString())
	assert.Nil(uut.CreateSubscription(utCtxt, testTopic, testSubscription0, pubsub.SubscriptionConfig{
		AckDeadline:       time.Second * 10,
		RetentionDuration: time.Minute * 10,
	}))
	testSubscription1 := fmt.Sprintf("goutil-ut-sub-5-%s", uuid.NewString())
	assert.Nil(uut.CreateSubscription(utCtxt, testTopic, testSubscription1, pubsub.SubscriptionConfig{
		AckDeadline:       time.Second * 10,
		RetentionDuration: time.Minute * 10,
	}))

	type msgWrap struct {
		rxIdx int
		msg   []byte
		meta  map[string]string
	}

	// Support for receiving messages
	rxMsg := make(chan msgWrap)
	receiveMsg0 := func(ctxt context.Context, ts time.Time, msg []byte, meta map[string]string) error {
		rxMsg <- msgWrap{rxIdx: 0, msg: msg, meta: meta}
		return nil
	}
	receiveMsg1 := func(ctxt context.Context, ts time.Time, msg []byte, meta map[string]string) error {
		rxMsg <- msgWrap{rxIdx: 1, msg: msg, meta: meta}
		return nil
	}

	// Start receiving on subscription
	log.Debug("Starting message subscription receive")
	wg := sync.WaitGroup{}
	wg.Add(2)
	rxCtxt, rxCancel := context.WithCancel(utCtxt)
	receiver := func(
		idx int,
		subscription string,
		handler func(ctxt context.Context, ts time.Time, msg []byte, meta map[string]string) error,
	) {
		defer wg.Done()
		log.Debugf("Start MSG Receiver %d", idx)
		assert.Nil(uut.Subscribe(rxCtxt, subscription, handler))
	}
	go receiver(0, testSubscription0, receiveMsg0)
	go receiver(1, testSubscription1, receiveMsg1)
	log.Debug("Started message subscription receive")

	// Send messages
	log.Debug("Publishing test messages")
	testMsgs := map[string]map[string]string{}
	for itr := 0; itr < 3; itr++ {
		msg := uuid.NewString()
		meta := map[string]string{
			uuid.NewString(): uuid.NewString(), uuid.NewString(): uuid.NewString(),
		}
		_, err := uut.Publish(utCtxt, testTopic, []byte(msg), meta, true)
		assert.Nil(err)
		testMsgs[msg] = meta
	}
	log.Debug("Published test messages")

	// Wait for message to come back
	{
		lclCtxt, cancel := context.WithTimeout(utCtxt, time.Second*5)
		defer cancel()

		perSubRXMsgs := map[int]map[string]map[string]string{}

		for itr := 0; itr < 6; itr++ {
			select {
			case <-lclCtxt.Done():
				assert.False(true, "PubSub receive timeout")
			case msg, ok := <-rxMsg:
				assert.True(ok)
				if _, ok := perSubRXMsgs[msg.rxIdx]; !ok {
					perSubRXMsgs[msg.rxIdx] = make(map[string]map[string]string)
				}
				perSubRXMsgs[msg.rxIdx][string(msg.msg)] = msg.meta
			}
		}

		assert.Len(perSubRXMsgs, 2)
		assert.EqualValues(testMsgs, perSubRXMsgs[0])
		assert.EqualValues(testMsgs, perSubRXMsgs[1])
	}

	// Clean up
	rxCancel()
	wg.Wait()
	assert.Nil(uut.DeleteSubscription(utCtxt, testSubscription0))
	assert.Nil(uut.DeleteSubscription(utCtxt, testSubscription1))
	assert.Nil(uut.DeleteTopic(utCtxt, testTopic))
}

func TestPubSubDynamicTopicCacheUpdate(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	coreClient, err := createTestPubSubClient(utCtxt)
	assert.Nil(err)

	uut0, err := goutils.GetNewPubSubClientInstance(
		coreClient, log.Fields{"instance": "unit-tester-0"}, nil,
	)
	assert.Nil(err)

	// Create test topic
	testTopic1 := fmt.Sprintf("goutil-ut-topic-6-%s", uuid.NewString())
	assert.Nil(uut0.CreateTopic(
		utCtxt, testTopic1, &pubsub.TopicConfig{RetentionDuration: time.Minute * 10},
	))

	// Define a different client
	uut1, err := goutils.GetNewPubSubClientInstance(
		coreClient, log.Fields{"instance": "unit-tester-1"}, nil,
	)
	assert.Nil(err)

	// Publish on topic created by first client
	_, err = uut1.Publish(utCtxt, testTopic1, []byte(uuid.NewString()), nil, true)
	assert.Nil(err)

	// Publish on completely unknown topic
	testTopic2 := fmt.Sprintf("goutil-ut-topic-6-%s", uuid.NewString())
	_, err = uut1.Publish(utCtxt, testTopic2, []byte(uuid.NewString()), nil, true)
	assert.NotNil(err)

	// Clean up
	assert.Nil(uut0.DeleteTopic(utCtxt, testTopic1))
}
