package goutils

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/apex/log"
	"google.golang.org/api/iterator"
)

/*
CreateBasicGCPPubSubClient define a basic GCP PubSub client

	@param ctxt context.Context - execution context
	@param projectID string - GCP project ID
	@returns new client
*/
func CreateBasicGCPPubSubClient(ctxt context.Context, projectID string) (*pubsub.Client, error) {
	return pubsub.NewClient(ctxt, projectID)
}

// ==========================================================================================
// Client Interface

// PubSubMessageHandler callback to trigger when PubSub message received
type PubSubMessageHandler func(
	ctxt context.Context, pubTimestamp time.Time, msg []byte, metadata map[string]string,
) error

// PubSubClient is a wrapper interface around the PubSub API with some ease-of-use features
type PubSubClient interface {
	/*
		SyncWithExisting sync the current set of topics and subscriptions the client can find

		 @param ctxt context.Context - execution context
	*/
	SyncWithExisting(ctxt context.Context) error

	/*
		CreateTopic create PubSub topic

		 @param ctxt context.Context - execution context
		 @param topic string - topic name
		 @param config *pubsub.TopicConfig - optionally, provide config on the topic
	*/
	CreateTopic(ctxt context.Context, topic string, config *pubsub.TopicConfig) error

	/*
		DeleteTopic delete PubSub topic

		 @param ctxt context.Context - execution context
		 @param topic string - topic name
	*/
	DeleteTopic(ctxt context.Context, topic string) error

	/*
		GetTopic get the topic config for a topic

		 @param ctxt context.Context - execution context
		 @param topic string - topic name
		 @returns if topic is known, the topic config
	*/
	GetTopic(ctxt context.Context, topic string) (pubsub.TopicConfig, error)

	/*
		UpdateTopic update the topic config

		 @param ctxt context.Context - execution context
		 @param topic string - topic name
		 @param newConfig pubsub.TopicConfigToUpdate - the new config
	*/
	UpdateTopic(ctxt context.Context, topic string, newConfig pubsub.TopicConfigToUpdate) error

	/*
		CreateSubscription create PubSub subscription to attach to topic

		 @param ctxt context.Context - execution context
		 @param targetTopic string - target topic
		 @param subscription string - subscription name
		 @param config pubsub.SubscriptionConfig - subscription config
	*/
	CreateSubscription(
		ctxt context.Context, targetTopic, subscription string, config pubsub.SubscriptionConfig,
	) error

	/*
		DeleteSubscription delete PubSub subscription

		 @param ctxt context.Context - execution context
		 @param subscription string - subscription name
	*/
	DeleteSubscription(ctxt context.Context, subscription string) error

	/*
		GetSubscription get the subscription config for a subscription

		 @param ctxt context.Context - execution context
		 @param subscription string - subscription name
		 @returns if subscription is known, the subscription config
	*/
	GetSubscription(ctxt context.Context, subscription string) (pubsub.SubscriptionConfig, error)

	/*
		UpdateSubscription update the subscription config

		 @param ctxt context.Context - execution context
		 @param subscription string - subscription name
		 @param newConfig pubsub.SubscriptionConfigToUpdate - the new config
	*/
	UpdateSubscription(
		ctxt context.Context, subscription string, newConfig pubsub.SubscriptionConfigToUpdate,
	) error

	/*
		Publish publish a message to a topic

		 @param ctxt context.Context - execution context
		 @param topic string - topic name
		 @param message []byte - message content
		 @param metadata map[string]string - message metadata, which will be sent using attributes
		 @param blocking bool - whether the call is blocking until publish is complete
		 @returns when non-blocking, the async result object to check on publish status
	*/
	Publish(
		ctxt context.Context, topic string, message []byte, metadata map[string]string, blocking bool,
	) (*pubsub.PublishResult, error)

	/*
		Subscribe subscribe for message on a subscription

		This call is blocking.

		 @param ctxt context.Context - execution context
		 @param subscription string - subscription name
		 @param handler PubSubMessageHandler - RX message callback
	*/
	Subscribe(ctxt context.Context, subscription string, handler PubSubMessageHandler) error

	/*
		Close close and clean up the client

		 @param ctxt context.Context - execution context
	*/
	Close(ctxt context.Context) error
}

// pubsubClientImpl implements PubSubClient
type pubsubClientImpl struct {
	Component
	client        *pubsub.Client
	topicLock     sync.RWMutex
	topics        map[string]*pubsub.Topic
	subLock       sync.RWMutex
	subscriptions map[string]*pubsub.Subscription
}

/*
GetNewPubSubClientInstance get PubSub wrapper client

	@param client *pubsub.Client - core PubSub client
	@param logTags log.Fields - metadata fields to include in the logs
	@returns new PubSubClient instance
*/
func GetNewPubSubClientInstance(client *pubsub.Client, logTags log.Fields) (PubSubClient, error) {
	return &pubsubClientImpl{
		Component:     Component{LogTags: logTags},
		client:        client,
		topicLock:     sync.RWMutex{},
		topics:        make(map[string]*pubsub.Topic),
		subLock:       sync.RWMutex{},
		subscriptions: make(map[string]*pubsub.Subscription),
	}, nil
}

// ==========================================================================================
// Maintenance

/*
SyncWithExisting sync the current set of topics and subscriptions the client can find

	@param ctxt context.Context - execution context
*/
func (p *pubsubClientImpl) SyncWithExisting(ctxt context.Context) error {
	logTag := p.GetLogTagsForContext(ctxt)

	// Read all known topics
	{
		p.topicLock.Lock()
		defer p.topicLock.Unlock()
		topicItr := p.client.Topics(ctxt)
		for {
			topicEntry, err := topicItr.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.WithError(err).WithFields(logTag).Error("Topic iterator query failure")
				return err
			}
			p.topics[topicEntry.ID()] = topicEntry
			log.WithFields(logTag).Debugf("Found topic '%s'", topicEntry.ID())
		}
	}

	// Read all known subscriptions
	{
		p.subLock.Lock()
		defer p.subLock.Unlock()
		subItr := p.client.Subscriptions(ctxt)
		for {
			subEntry, err := subItr.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.WithError(err).WithFields(logTag).Error("Topic iterator query failure")
				return err
			}
			p.subscriptions[subEntry.ID()] = subEntry
			log.WithFields(logTag).Debugf("Found subscription '%s'", subEntry.ID())
		}
	}

	return nil
}

/*
Close close and clean up the client

	@param ctxt context.Context - execution context
*/
func (p *pubsubClientImpl) Close(ctxt context.Context) error {
	logTag := p.GetLogTagsForContext(ctxt)
	{
		// Stop all the topics
		p.topicLock.Lock()
		defer p.topicLock.Unlock()

		for topicName, topic := range p.topics {
			log.WithFields(logTag).Debugf("Stopping topic '%s'", topicName)
			topic.Stop()
			log.WithFields(logTag).Infof("Stopped topic '%s'", topicName)
		}
	}
	return nil
}

// ==========================================================================================
// Topics

/*
CreateTopic create PubSub topic

	@param ctxt context.Context - execution context
	@param topic string - topic name
	@param config *pubsub.TopicConfig - optionally, provide config on the topic
*/
func (p *pubsubClientImpl) CreateTopic(
	ctxt context.Context, topic string, config *pubsub.TopicConfig,
) error {
	p.topicLock.Lock()
	defer p.topicLock.Unlock()

	logTag := p.GetLogTagsForContext(ctxt)

	// If this instance has created this topic before
	if _, ok := p.topics[topic]; ok {
		log.WithFields(logTag).Infof("Topic '%s' already exist", topic)
		return nil
	}

	log.WithFields(logTag).Debugf("Creating topic '%s'", topic)
	topicHandle, err := p.client.CreateTopicWithConfig(ctxt, topic, config)
	if err != nil {
		log.WithError(err).WithFields(logTag).Errorf("Topic '%s' creation failed", topic)
		return err
	}
	log.WithFields(logTag).Infof("Created topic '%s'", topic)

	p.topics[topic] = topicHandle

	return nil
}

/*
DeleteTopic delete PubSub topic

	@param ctxt context.Context - execution context
	@param topic string - topic name
*/
func (p *pubsubClientImpl) DeleteTopic(ctxt context.Context, topic string) error {
	p.topicLock.Lock()
	defer p.topicLock.Unlock()

	logTag := p.GetLogTagsForContext(ctxt)

	topicHandle, ok := p.topics[topic]
	if !ok {
		err := fmt.Errorf("this instance does know of topic '%s'", topic)
		log.WithError(err).WithFields(logTag).Errorf("Unable to delete topic '%s'", topic)
		return err
	}

	// This instance has initialized this topic
	log.WithFields(logTag).Infof("Stopping handler for topic '%s'", topic)
	topicHandle.Stop()
	log.WithFields(logTag).Debugf("Deleting topic '%s'", topic)
	if err := topicHandle.Delete(ctxt); err != nil {
		log.WithError(err).WithFields(logTag).Errorf("Unable to delete topic '%s'", topic)
		return err
	}
	log.WithFields(logTag).Infof("Deleted topic '%s'", topic)

	// Forgot the handle
	delete(p.topics, topic)

	return nil
}

/*
GetTopic get the topic config for a topic

	@param ctxt context.Context - execution context
	@param topic string - topic name
	@returns if topic is known, the topic config
*/
func (p *pubsubClientImpl) GetTopic(ctxt context.Context, topic string) (pubsub.TopicConfig, error) {
	p.topicLock.Lock()
	defer p.topicLock.Unlock()

	logTag := p.GetLogTagsForContext(ctxt)

	topicHandle, ok := p.topics[topic]
	if !ok {
		err := fmt.Errorf("this instance does know of topic '%s'", topic)
		log.WithError(err).WithFields(logTag).Errorf("Unable to find topic '%s'", topic)
		return pubsub.TopicConfig{}, err
	}

	log.WithFields(logTag).Debugf("Reading topic '%s' config", topic)
	config, err := topicHandle.Config(ctxt)
	if err != nil {
		log.WithError(err).WithFields(logTag).Errorf("Unable to read topic '%s' config", topic)
		return pubsub.TopicConfig{}, err
	}
	log.WithFields(logTag).Infof("Read topic '%s' config", topic)

	return config, nil
}

/*
UpdateTopic update the topic config

	@param ctxt context.Context - execution context
	@param topic string - topic name
	@param newConfig pubsub.TopicConfigToUpdate - the new config
*/
func (p *pubsubClientImpl) UpdateTopic(
	ctxt context.Context, topic string, newConfig pubsub.TopicConfigToUpdate,
) error {
	p.topicLock.Lock()
	defer p.topicLock.Unlock()

	logTag := p.GetLogTagsForContext(ctxt)

	topicHandle, ok := p.topics[topic]
	if !ok {
		err := fmt.Errorf("this instance does know of topic '%s'", topic)
		log.WithError(err).WithFields(logTag).Errorf("Unable to find topic '%s'", topic)
		return err
	}

	log.WithFields(logTag).Debugf("Updating topic '%s' config", topic)
	if _, err := topicHandle.Update(ctxt, newConfig); err != nil {
		log.WithError(err).WithFields(logTag).Errorf("Unable to update topic '%s'", topic)
		return err
	}
	log.WithFields(logTag).Infof("Updated topic '%s' config", topic)

	return nil
}

// ==========================================================================================
// Subscriptions

/*
CreateSubscription create PubSub subscription to attach to topic

	@param ctxt context.Context - execution context
	@param targetTopic string - target topic
	@param subscription string - subscription name
	@param config pubsub.SubscriptionConfig - subscription config
*/
func (p *pubsubClientImpl) CreateSubscription(
	ctxt context.Context, targetTopic, subscription string, config pubsub.SubscriptionConfig,
) error {
	logTag := p.GetLogTagsForContext(ctxt)

	var topic *pubsub.Topic

	// Grab the topic object
	{
		p.topicLock.Lock()
		defer p.topicLock.Unlock()

		t, ok := p.topics[targetTopic]
		if !ok {
			err := fmt.Errorf("topic '%s' is unknown", targetTopic)
			log.
				WithError(err).
				WithFields(logTag).
				Errorf("Unable to create subscription '%s'", subscription)
			return err
		}

		topic = t
	}

	// Create the subscription
	config.Topic = topic
	{
		p.subLock.Lock()
		defer p.subLock.Unlock()

		// If this instance has created this topic before
		if _, ok := p.subscriptions[subscription]; ok {
			log.WithFields(logTag).Infof("Subscription '%s' already exist", subscription)
			return nil
		}

		log.WithFields(logTag).Debugf("Creating subscription '%s'", subscription)
		subHandle, err := p.client.CreateSubscription(ctxt, subscription, config)
		if err != nil {
			log.WithError(err).WithFields(logTag).Errorf("Unable to create subscription '%s'", subscription)
			return err
		}
		log.WithFields(logTag).Infof("Created subscription '%s'", subscription)

		p.subscriptions[subscription] = subHandle
	}

	return nil
}

/*
DeleteSubscription delete PubSub subscription

	@param ctxt context.Context - execution context
	@param subscription string - subscription name
*/
func (p *pubsubClientImpl) DeleteSubscription(ctxt context.Context, subscription string) error {
	p.subLock.Lock()
	defer p.subLock.Unlock()

	logTag := p.GetLogTagsForContext(ctxt)

	subHandle, ok := p.subscriptions[subscription]
	if !ok {
		err := fmt.Errorf("this instance does not know of subscription '%s'", subscription)
		log.WithError(err).WithFields(logTag).Errorf("Unable to delete subscription '%s'", subscription)
		return err
	}

	log.WithFields(logTag).Debugf("Deleting subscription '%s'", subscription)
	if err := subHandle.Delete(ctxt); err != nil {
		log.WithError(err).WithFields(logTag).Errorf("Unable to delete subscription '%s'", subscription)
		return err
	}
	log.WithFields(logTag).Infof("Deleted subscription '%s'", subscription)

	// Forgot the handle
	delete(p.subscriptions, subscription)

	return nil
}

/*
GetSubscription get the subscription config for a subscription

	@param ctxt context.Context - execution context
	@param subscription string - subscription name
	@returns if subscription is known, the subscription config
*/
func (p *pubsubClientImpl) GetSubscription(
	ctxt context.Context, subscription string,
) (pubsub.SubscriptionConfig, error) {
	p.subLock.Lock()
	defer p.subLock.Unlock()

	logTag := p.GetLogTagsForContext(ctxt)

	subHandle, ok := p.subscriptions[subscription]
	if !ok {
		err := fmt.Errorf("this instance does not know of subscription '%s'", subscription)
		log.WithError(err).WithFields(logTag).Errorf("Unable to find subscription '%s'", subscription)
		return pubsub.SubscriptionConfig{}, err
	}

	log.WithFields(logTag).Debugf("Reading subscription '%s' config", subscription)
	config, err := subHandle.Config(ctxt)
	if err != nil {
		log.
			WithError(err).
			WithFields(logTag).
			Errorf("Unable to read subscription '%s' config", subscription)
		return pubsub.SubscriptionConfig{}, err
	}
	log.WithFields(logTag).Infof("Read subscription '%s' config", subscription)

	return config, nil
}

/*
UpdateSubscription update the subscription config

	@param ctxt context.Context - execution context
	@param subscription string - subscription name
	@param newConfig pubsub.SubscriptionConfigToUpdate - the new config
*/
func (p *pubsubClientImpl) UpdateSubscription(
	ctxt context.Context, subscription string, newConfig pubsub.SubscriptionConfigToUpdate,
) error {
	p.subLock.Lock()
	defer p.subLock.Unlock()

	logTag := p.GetLogTagsForContext(ctxt)

	subHandle, ok := p.subscriptions[subscription]
	if !ok {
		err := fmt.Errorf("this instance does not know of subscription '%s'", subscription)
		log.WithError(err).WithFields(logTag).Errorf("Unable to find subscription '%s'", subscription)
		return err
	}

	log.WithFields(logTag).Debugf("Updating subscription '%s' config", subscription)
	if _, err := subHandle.Update(ctxt, newConfig); err != nil {
		log.WithError(err).WithFields(logTag).Errorf("Unable to update subscription '%s'", subscription)
		return err
	}
	log.WithFields(logTag).Infof("Updated subscription '%s' config", subscription)

	return nil
}

// ==========================================================================================
// Message Passing

/*
Publish publish a message to a topic

	@param ctxt context.Context - execution context
	@param topic string - topic name
	@param message []byte - message content
	@param metadata map[string]string - message metadata, which will be sent using attributes
	@param blocking bool - whether the call is blocking until publish is complete
	@returns when non-blocking, the async result object to check on publish status
*/
func (p *pubsubClientImpl) Publish(
	ctxt context.Context, topic string, message []byte, metadata map[string]string, blocking bool,
) (*pubsub.PublishResult, error) {
	logTag := p.GetLogTagsForContext(ctxt)

	// Get the topic handle
	var topicHandle *pubsub.Topic
	{
		p.topicLock.RLock()
		defer p.topicLock.RUnlock()

		t, ok := p.topics[topic]
		if !ok {
			err := fmt.Errorf("topic '%s' is unknown", topic)
			log.WithError(err).WithFields(logTag).Errorf("Publish on topic '%s' failed", topic)
			return nil, err
		}

		topicHandle = t
	}

	log.WithFields(logTag).Debugf("Publishing message on topic '%s'", topic)
	txHandle := topicHandle.Publish(ctxt, &pubsub.Message{Data: message, Attributes: metadata})

	if blocking {
		// Wait for publish to finish
		txID, err := txHandle.Get(ctxt)
		if err != nil {
			log.WithError(err).WithFields(logTag).Errorf("Publish on topic '%s' failed", topic)
			return nil, err
		}
		log.WithFields(logTag).Debugf("Published message [%s] on topic '%s'", txID, topic)
	} else {
		log.WithFields(logTag).Debugf("Published message on topic '%s'", topic)
	}
	return txHandle, nil
}

/*
Subscribe subscribe for message on a subscription.

This call is blocking.

	@param ctxt context.Context - execution context
	@param subscription string - subscription name
	@param handler PubSubMessageHandler - RX message callback
*/
func (p *pubsubClientImpl) Subscribe(
	ctxt context.Context, subscription string, handler PubSubMessageHandler,
) error {
	logTag := p.GetLogTagsForContext(ctxt)

	// Get the subscription handle
	var subscriptionHandle *pubsub.Subscription
	{
		p.subLock.RLock()
		defer p.subLock.RUnlock()

		s, ok := p.subscriptions[subscription]
		if !ok {
			err := fmt.Errorf("subscription '%s' is unknown", subscription)
			log.
				WithError(err).
				WithFields(logTag).
				Errorf("Listen on subscription '%s' failed", subscription)
			return err
		}

		subscriptionHandle = s
	}

	// Install subscription receive
	if err := subscriptionHandle.Receive(ctxt, func(ctx context.Context, m *pubsub.Message) {
		if err := handler(ctx, m.PublishTime, m.Data, m.Attributes); err != nil {
			log.
				WithError(err).
				WithFields(logTag).
				WithField("subscription", subscription).
				Errorf("Failed to process message [%s]", m.ID)
			m.Nack()
		} else {
			log.
				WithError(err).
				WithFields(logTag).
				WithField("subscription", subscription).
				Debugf("Processed message [%s]", m.ID)
			m.Ack()
		}
	}); err != nil {
		log.WithError(err).WithFields(logTag).Errorf("Listen on subscription '%s' failed", subscription)
		return err
	}

	return nil
}
