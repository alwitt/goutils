package redis

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/alwitt/goutils"
	"github.com/apex/log"
	"github.com/redis/go-redis/v9"
)

// PubSubMessage tuple for a PubSub message
type PubSubMessage struct {
	// Topic the PubSub topic
	Topic string
	// Message to send or received
	Message QueueMessageEnvelope
}

// PubSubMessageHandler processes one received PubSub message.
//
// The handler is invoked serially from the subscriber's single reader goroutine, so
// implementations need no internal locking to preserve ordering. It MUST return promptly:
// a slow handler stalls consumption of messages from REDIS and can cause messages to be
// dropped. Offload any heavy work onto the caller's own goroutine or queue.
//
// The provided context is the subscriber's working context; it is cancelled when the
// subscriber is stopped, so a long-running handler can observe it and bail out early.
type PubSubMessageHandler func(ctx context.Context, msg PubSubMessage)

/*
Publish a message a onto a REDIS PubSub topic

	@param ctx context.Context - execution context
	@param topic string - topic to publish on
	@param msg QueueMessageEnvelope - the message to publish
	@returns `BadInputError` in case message is not serializing
	@returns `REDISError` in case of failure
*/
func (c *clientImpl) Publish(ctx context.Context, msg PubSubMessage) error {
	payload, err := msg.Message.StringPayload()
	if err != nil {
		return goutils.NewBadInputError("failed to serialize message for publishing", err, true)
	}
	resp := c.core.Publish(ctx, msg.Topic, payload)
	if resp.Err() != nil {
		return goutils.NewRedisError("failed to publish message on topic "+msg.Topic, resp.Err(), true)
	}
	return nil
}

/*
Subscribe for messages on a set of REDIS PubSub topics

	@param ctx context.Context - execution context
	@param subName string - subscriber name
	@param topics []string - the PubSub topics to subscribe on
	@return the subscription runner
*/
func (c *clientImpl) Subscribe(
	_ context.Context, subName string, topics []string,
) (Subscriber, error) {
	logTags := log.Fields{
		"module":        "redis",
		"component":     "redis-client",
		"server":        c.serverAddress,
		"sub-component": "subscriber",
		"subscriber":    subName,
		"topics":        topics,
	}

	return &subscriberImpl{
		Component: goutils.Component{
			LogTags: logTags,
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		subscriber:       subName,
		lock:             sync.RWMutex{},
		wg:               sync.WaitGroup{},
		workingCtx:       nil,
		workingCtxCancel: nil,
		topics:           topics,
		core:             c.core,
	}, nil
}

// Subscriber REDIS PubSub subscription runner
//
// This runner will spawn a support task to read messages from REDIS and deliver each one
// to a caller provided handler callback.
type Subscriber interface {
	/*
		Start the subscription message processing support task.

		The caller must provide a handler callback. It is invoked, serially, for each
		received message from the subscriber's single reader goroutine.

			@param parentCtx context.Context - parent execution context for the support process
			@param handler PubSubMessageHandler - callback invoked for each received message
	*/
	Start(parentCtx context.Context, handler PubSubMessageHandler) error

	/*
		Stop the subscription message processing support task

			@param ctx context.Context - execution context
	*/
	Stop(ctx context.Context) error
}

// subscriberImpl implements Subscription
type subscriberImpl struct {
	goutils.Component

	subscriber string

	lock sync.RWMutex

	wg               sync.WaitGroup
	workingCtx       context.Context
	workingCtxCancel context.CancelFunc

	// topics to subscribe to
	topics []string

	core *redis.Client
}

// running helper function to determine whether subscription reader already running
func (s *subscriberImpl) running() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.workingCtx != nil && s.workingCtxCancel != nil
}

/*
Start the subscription message processing support task.

The caller must provide a handler callback. It is invoked, serially, for each received
message from the subscriber's single reader goroutine.

	@param parentCtx context.Context - parent execution context for the support process
	@param handler PubSubMessageHandler - callback invoked for each received message
*/
func (s *subscriberImpl) Start(parentCtx context.Context, handler PubSubMessageHandler) error {
	if handler == nil {
		return goutils.NewBadInputError("subscriber '"+s.subscriber+"' requires a handler", nil, true)
	}
	if s.running() {
		return goutils.NewConsistencyError("subscriber '"+s.subscriber+"' already running", nil, true)
	}

	// Establish new context
	setupNewContext := func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		s.workingCtx, s.workingCtxCancel = context.WithCancel(parentCtx)
	}
	setupNewContext()

	// Start the processing loop
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		logTags := s.GetLogTagsForContext(s.workingCtx)
		log.WithFields(goutils.UpdateCodePositionInTags(logTags)).Info("Processing messages")
		defer log.WithFields(goutils.UpdateCodePositionInTags(logTags)).Info("Stop processing messages")

		// Start the subscription
		subscription := s.core.Subscribe(s.workingCtx, s.topics...)
		defer func() {
			if err := subscription.Close(); err != nil {
				log.
					WithError(err).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Error("Error when closing subscription")
			}
		}()
		// Get the *redis.Message channel
		msgChan := subscription.Channel()

		// handleDone log any non-cancellation context error before the loop exits
		handleDone := func() {
			err := s.workingCtx.Err()
			if !errors.Is(err, context.Canceled) {
				log.
					WithError(err).
					WithFields(goutils.UpdateCodePositionInTags(logTags)).
					Error("Subscription processing loop ended with error")
			}
		}

		// deliver invoke the caller's handler for one message, isolating the loop from a
		// panicking handler so one bad callback does not tear down the subscriber.
		deliver := func(msg PubSubMessage) {
			defer func() {
				if r := recover(); r != nil {
					log.
						WithField("panic", r).
						WithFields(goutils.UpdateCodePositionInTags(logTags)).
						Error("PubSub message handler panicked")
				}
			}()
			handler(s.workingCtx, msg)
		}

		for {
			select {
			// Context cancelled
			case <-s.workingCtx.Done():
				handleDone()
				return

			// Receive message
			case msg, ok := <-msgChan:
				if !ok {
					// REDIS SDK closed channel
					return
				}
				// Hand the message to the caller's callback
				deliver(PubSubMessage{Topic: msg.Channel, Message: queueMessage(msg.Payload)})
			}
		}
	}()

	return nil
}

/*
Stop the subscription message processing support task

	@param ctx context.Context - execution context
*/
func (s *subscriberImpl) Stop(ctx context.Context) error {
	if !s.running() {
		return goutils.NewConsistencyError("subscriber '"+s.subscriber+"' not running", nil, true)
	}

	// Clean up
	defer func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		s.workingCtx = nil
		s.workingCtxCancel = nil
	}()

	// Stop the process
	s.workingCtxCancel()

	if err := goutils.TimeBoundedWaitGroupWait(ctx, &s.wg, time.Second*5); err != nil {
		return goutils.NewRedisError("REDIS subscription reader process didn't stop in time", err, true)
	}

	return nil
}
