package goutils

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestTopicCRUD(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	testGCPProjectID := os.Getenv("UNITTEST_GCP_PROJECT_ID")
	assert.NotEqual("", testGCPProjectID)

	coreClient, err := CreateBasicGCPPubSubClient(utCtxt, testGCPProjectID)
	assert.Nil(err)

	uut, err := GetNewPubSubClientInstance(coreClient, log.Fields{"instance": "unit-tester"})
	assert.Nil(err)

	assert.Nil(uut.SyncWithExisting(utCtxt))

	// Case 0: unknown topic
	{
		_, err = uut.GetTopic(utCtxt, uuid.NewString())
		assert.NotNil(err)
	}

	// Case 1: create new topic
	topic0 := fmt.Sprintf("goutil-ut-topic-%s", uuid.NewString())
	{
		topicConfig := pubsub.TopicConfig{RetentionDuration: time.Minute * 10}
		assert.Nil(uut.CreateTopic(utCtxt, topic0, &topicConfig))
	}
	{
		config, err := uut.GetTopic(utCtxt, topic0)
		assert.Nil(err)
		assert.Equal(topic0, config.ID())
		assert.Equal(time.Minute*10, config.RetentionDuration)
	}

	// Case 2: create same topic again
	assert.Nil(uut.CreateTopic(utCtxt, topic0, nil))

	// Case 3: update the topic
	{
		newConfig := pubsub.TopicConfigToUpdate{RetentionDuration: time.Minute * 30}
		assert.Nil(uut.UpdateTopic(utCtxt, topic0, newConfig))
	}
	{
		config, err := uut.GetTopic(utCtxt, topic0)
		assert.Nil(err)
		assert.Equal(topic0, config.ID())
		assert.Equal(time.Minute*30, config.RetentionDuration)
	}

	// Case 4: create another topic
	topic1 := fmt.Sprintf("goutil-ut-topic-%s", uuid.NewString())
	{
		topicConfig := pubsub.TopicConfig{RetentionDuration: time.Minute * 15}
		assert.Nil(uut.CreateTopic(utCtxt, topic1, &topicConfig))
	}
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
}

func TestTopicSync(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	testGCPProjectID := os.Getenv("UNITTEST_GCP_PROJECT_ID")
	assert.NotEqual("", testGCPProjectID)

	coreClient0, err := CreateBasicGCPPubSubClient(utCtxt, testGCPProjectID)
	assert.Nil(err)

	coreClient1, err := CreateBasicGCPPubSubClient(utCtxt, testGCPProjectID)
	assert.Nil(err)

	uut0, err := GetNewPubSubClientInstance(coreClient0, log.Fields{"instance": "unit-tester-0"})
	assert.Nil(err)

	uut1, err := GetNewPubSubClientInstance(coreClient1, log.Fields{"instance": "unit-tester-1"})
	assert.Nil(err)

	// Create new topic
	topic0 := fmt.Sprintf("goutil-ut-topic-%s", uuid.NewString())
	{
		topicConfig := pubsub.TopicConfig{RetentionDuration: time.Minute * 33}
		assert.Nil(uut0.CreateTopic(utCtxt, topic0, &topicConfig))
	}

	// Sync from the other client
	assert.Nil(uut1.SyncWithExisting(utCtxt))
	{
		config, err := uut1.GetTopic(utCtxt, topic0)
		assert.Nil(err)
		assert.Equal(topic0, config.ID())
		assert.Equal(time.Minute*33, config.RetentionDuration)
	}

	// Clean up
	assert.Nil(uut0.DeleteTopic(utCtxt, topic0))
}
