package goutils

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/apex/log"
	"github.com/google/uuid"
)

// ReqRespMessage message structure passing through a request-response system
type ReqRespMessage struct {
	// RequestID request ID associated with this message
	RequestID string `json:"request" validate:"required"`
	// SenderID message sender ID
	SenderID string `json:"sender" validate:"required"`
	// TargetID message target ID
	TargetID string `json:"target" validate:"required"`
	// IsRequest whether the message is a request message
	IsRequest bool `json:"is_request"`
	// Timestamp message timestamp
	Timestamp time.Time `json:"timestamp"`
	// Metadata message metadata
	Metadata map[string]string `json:"meta,omitempty"`
	// Payload message payload
	Payload []byte `json:"payload,omitempty"`
}

// ReqRespMessageHandler callback called when request-response message is available for processing
type ReqRespMessageHandler func(ctxt context.Context, msg ReqRespMessage) error

// ReqRespTimeoutHandler callback called when request timed out waiting for all responses
type ReqRespTimeoutHandler func(ctxt context.Context) error

// RequestCallParam contains the parameters of a request
type RequestCallParam struct {
	// RespHandler response message handler callback
	RespHandler ReqRespMessageHandler
	// ExpectedResponsesCount the expected number of responses to receive. Once this many responses
	// are received, the request is considered to be complete.
	ExpectedResponsesCount int
	// Blocking whether the request call is blocking
	Blocking bool
	// Timeout the request timeout if it has not received all responses after this duration
	Timeout time.Duration
	// TimeoutHandler request timeout handler callback
	// TODO FIXME: implement request timeout handling
	TimeoutHandler ReqRespTimeoutHandler
}

// RequestResponseClient is a request-response client built on provided messaging transport
//
// Each client instance will respond to requests for a single request target ID
type RequestResponseClient interface {
	/*
		SetInboundRequestHandler set the inbound request handler

		 @param ctxt context.Context - execution context
		 @param handler ReqRespMessageHandler - the handler to use
	*/
	SetInboundRequestHandler(ctxt context.Context, handler ReqRespMessageHandler) error

	/*
		Request make a new request

		 @param ctxt context.Context - execution context
		 @param targetID string - target ID this request is destined for
		 @param message []byte - request message payload
		 @param metadata map[string]string - request metadata
		 @param callParam RequestCallParam - request call parameters
		 @return request ID
	*/
	Request(
		ctxt context.Context,
		targetID string,
		message []byte,
		metadata map[string]string,
		callParam RequestCallParam,
	) (string, error)

	/*
		Respond respond to an inbound request

		 @param ctxt context.Context - execution context
		 @param originalReq ReqRespMessage - original request
		 @param message []byte - response message payload
		 @param metadata map[string]string - request metadata
		 @param blocking bool - whether the call is blocking
	*/
	Respond(
		ctxt context.Context,
		originalReq ReqRespMessage,
		message []byte,
		metadata map[string]string,
		blocking bool,
	) error

	/*
		Stop stop any support background tasks which were started

		 @param ctxt context.Context - execution context
	*/
	Stop(ctxt context.Context) error
}

// requestContextKey associated key for requestContext when storing in context
type requestContextKey struct{}

// requestContext represents a request made through the system
type requestContext struct {
	requestID         string
	senderID          string
	targetID          string
	createdAt         time.Time
	deadLine          time.Time
	expectedRespCount int
	receivedRespCount int
	completeCallback  func()
	callParam         RequestCallParam
}

// updateLogTags updates Apex log.Fields map with values the requests's parameters
func (c *requestContext) updateLogTags(tags log.Fields) {
	tags["req_request_id"] = c.requestID
	tags["sender_id"] = c.senderID
	tags["target_id"] = c.targetID
}

// String toString function
func (c *requestContext) String() string {
	return fmt.Sprintf(
		"(REQ-RESP[%s] (%s ==> %s) DEADLINE %s)",
		c.requestID,
		c.senderID,
		c.targetID,
		c.deadLine.Format(time.RFC3339),
	)
}

// modifyLogMetadataByRRRequestParam update log metadata with info from requestContext
func modifyLogMetadataByRRRequestParam(ctxt context.Context, theTags log.Fields) {
	if ctxt.Value(requestContextKey{}) != nil {
		v, ok := ctxt.Value(requestContextKey{}).(requestContext)
		if ok {
			v.updateLogTags(theTags)
		}
	}
}

// pubsubReqRespClient implements RequestResponseClient using PubSub.
type pubsubReqRespClient struct {
	Component
	targetID               string
	psClient               PubSubClient
	inboundProcessor       TaskProcessor
	inboundRequestHandler  ReqRespMessageHandler
	outboundProcessor      TaskProcessor
	outboundRequests       map[string]*requestContext
	parentContext          context.Context
	processorContext       context.Context
	processorContextCancel context.CancelFunc
	checkTimer             IntervalTimer
	wg                     sync.WaitGroup
}

// PubSubRequestResponseClientParam configuration parameters of PubSub based RequestResponseClient
type PubSubRequestResponseClientParam struct {
	// TargetID the ID to target to send a request (or response) to this client
	TargetID string
	// Name client instance name
	Name string
	// PSClient base pubsub client
	PSClient PubSubClient
	// MsgRetentionTTL PubSub message TTL, after which the message is purged.
	MsgRetentionTTL time.Duration
	// LogTags metadata fields to include in the logs
	LogTags log.Fields
	// CustomLogModifiers additional log metadata modifiers to use
	CustomLogModifiers []LogMetadataModifier
	// SupportWorkerCount number of support workers to spawn to process incoming messages
	SupportWorkerCount int
	// TimeoutEnforceInt interval between request timeout checks
	TimeoutEnforceInt time.Duration
}

/*
GetNewPubSubRequestResponseClientInstance get PubSub based RequestResponseClient

	@param parentCtxt context.Context - parent context
	@param params PubSubRequestResponseClientParam - client config parameters
	@return new RequestResponseClient instance
*/
func GetNewPubSubRequestResponseClientInstance(
	parentCtxt context.Context,
	params PubSubRequestResponseClientParam,
) (RequestResponseClient, error) {
	params.LogTags["rr-instance"] = params.Name
	params.LogTags["rr-target-id"] = params.TargetID

	workerContext, workerCtxtCancel := context.WithCancel(parentCtxt)

	// -----------------------------------------------------------------------------------------
	// Prepare PubSub topic and subscription for inbound messages

	// Prepare the topic
	if existingTopic, err := params.PSClient.GetTopic(parentCtxt, params.TargetID); err != nil {
		// Create new topic
		log.WithFields(params.LogTags).Warnf("Topic '%s' is not known. Creating...", params.TargetID)
		if err := params.PSClient.CreateTopic(
			parentCtxt, params.TargetID, &pubsub.TopicConfig{RetentionDuration: params.MsgRetentionTTL},
		); err != nil {
			log.
				WithError(err).
				WithFields(params.LogTags).
				Errorf("Failed when creating PubSub topic '%s'", params.TargetID)
			workerCtxtCancel()
			return nil, err
		}
	} else {
		// Use existing topic
		log.WithFields(params.LogTags).Infof("Reusing existing topic '%s'", params.TargetID)
		updatedTopic := pubsub.TopicConfigToUpdate{
			Labels:               existingTopic.Labels,
			MessageStoragePolicy: &existingTopic.MessageStoragePolicy,
			RetentionDuration:    params.MsgRetentionTTL,
			SchemaSettings:       existingTopic.SchemaSettings,
		}
		if err := params.PSClient.UpdateTopic(parentCtxt, params.TargetID, updatedTopic); err != nil {
			log.
				WithError(err).
				WithFields(params.LogTags).
				Errorf("Failed when updating PubSub topic '%s'", params.TargetID)
			workerCtxtCancel()
			return nil, err
		}
	}

	// Prepare the subscription
	receiveSubscription := fmt.Sprintf("%s.%s", params.Name, params.TargetID)
	if _, err := params.PSClient.GetSubscription(
		parentCtxt, receiveSubscription,
	); err != nil {
		// Create new subscription
		log.WithFields(params.LogTags).Warnf("Subscription '%s' is not known. Creating...", receiveSubscription)
		if err := params.PSClient.CreateSubscription(
			parentCtxt,
			params.TargetID,
			receiveSubscription,
			pubsub.SubscriptionConfig{
				ExpirationPolicy:  time.Hour * 24,
				RetentionDuration: params.MsgRetentionTTL,
			},
		); err != nil {
			log.
				WithError(err).
				WithFields(params.LogTags).
				Errorf("Failed when creating PubSub subscription '%s'", receiveSubscription)
			workerCtxtCancel()
			return nil, err
		}
	} else {
		// Use existing subscription
		log.WithFields(params.LogTags).Infof("Reusing existing subscription '%s'", receiveSubscription)
		updatedSubscription := pubsub.SubscriptionConfigToUpdate{
			RetentionDuration: params.MsgRetentionTTL,
			ExpirationPolicy:  time.Hour * 24,
		}
		if err := params.PSClient.UpdateSubscription(
			parentCtxt, receiveSubscription, updatedSubscription,
		); err != nil {
			log.
				WithError(err).
				WithFields(params.LogTags).
				Errorf("Failed when updating PubSub subscription '%s'", receiveSubscription)
			workerCtxtCancel()
			return nil, err
		}
	}

	// -----------------------------------------------------------------------------------------
	// Setup support daemon for processing inbound requests
	procInboundInstanceName := fmt.Sprintf("%s-inbound-process", params.Name)
	procInboundLogTags := log.Fields{}
	for lKey, lVal := range params.LogTags {
		procInboundLogTags[lKey] = lVal
	}
	procInboundLogTags["sub-module"] = procInboundInstanceName
	inboundProcessor, err := GetNewTaskDemuxProcessorInstance(
		workerContext,
		procInboundInstanceName,
		params.SupportWorkerCount*2,
		params.SupportWorkerCount,
		procInboundLogTags,
	)
	if err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Unable to define inbound request processor")
		workerCtxtCancel()
		return nil, err
	}

	// Setup support daemon for processing outbound requests
	procOutboundInstanceName := fmt.Sprintf("%s-outbound-process", params.Name)
	procOutboundLogTags := log.Fields{}
	for lKey, lVal := range params.LogTags {
		procOutboundLogTags[lKey] = lVal
	}
	procOutboundLogTags["sub-module"] = procOutboundInstanceName
	outboundProcessor, err := GetNewTaskProcessorInstance(
		workerContext, procOutboundInstanceName, params.SupportWorkerCount*2, procOutboundLogTags,
	)
	if err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Unable to define outbound request processor")
		workerCtxtCancel()
		return nil, err
	}

	// -----------------------------------------------------------------------------------------
	// Build the component
	instance := &pubsubReqRespClient{
		Component: Component{
			LogTags:         params.LogTags,
			LogTagModifiers: []LogMetadataModifier{modifyLogMetadataByRRRequestParam},
		},
		targetID:               params.TargetID,
		psClient:               params.PSClient,
		inboundProcessor:       inboundProcessor,
		inboundRequestHandler:  nil,
		outboundProcessor:      outboundProcessor,
		outboundRequests:       make(map[string]*requestContext),
		parentContext:          parentCtxt,
		processorContext:       workerContext,
		processorContextCancel: workerCtxtCancel,
		wg:                     sync.WaitGroup{},
	}
	timerLogTags := log.Fields{}
	for lKey, lVal := range params.LogTags {
		timerLogTags[lKey] = lVal
	}
	timerLogTags["sub-module"] = "timeout-enforce-timer"
	instance.checkTimer, err = GetIntervalTimerInstance(workerContext, &instance.wg, timerLogTags)
	if err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Unable to define request timeout check timer")
		workerCtxtCancel()
		return nil, err
	}

	// Add additional log tag modifiers
	instance.LogTagModifiers = append(instance.LogTagModifiers, params.CustomLogModifiers...)

	// -----------------------------------------------------------------------------------------
	// Define support tasks for processing outgoing requests

	if err := outboundProcessor.AddToTaskExecutionMap(
		reflect.TypeOf(rrRequestPayload{}), instance.request,
	); err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Unable to install task definition")
		return nil, err
	}

	// Incoming responses correspond with outgoing requests
	if err := outboundProcessor.AddToTaskExecutionMap(
		reflect.TypeOf(rrInboundResponsePayload{}), instance.receiveInboundResponseMsg,
	); err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Unable to install task definition")
		return nil, err
	}

	// Trigger request timeout enforcement
	if err := outboundProcessor.AddToTaskExecutionMap(
		reflect.TypeOf(rrRequestTimeoutCheckPayload{}), instance.requestTimeoutCheck,
	); err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Unable to install task definition")
		return nil, err
	}

	// -----------------------------------------------------------------------------------------
	// Define support tasks for processing incoming requests

	if err := inboundProcessor.AddToTaskExecutionMap(
		reflect.TypeOf(rrInboundRequestPayload{}), instance.receiveInboundRequestMsg,
	); err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Unable to install task definition")
		return nil, err
	}

	// Outgoing responses correspond with incoming requests
	if err := inboundProcessor.AddToTaskExecutionMap(
		reflect.TypeOf(rrResponsePayload{}), instance.respond,
	); err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Unable to install task definition")
		return nil, err
	}

	// -----------------------------------------------------------------------------------------
	// Listen for messages on previously created subscription

	instance.wg.Add(1)
	go func() {
		defer instance.wg.Done()
		err := params.PSClient.Subscribe(workerContext, receiveSubscription, instance.ReceivePubSubMsg)
		logTags := instance.GetLogTagsForContext(workerContext)
		if err != nil {
			log.
				WithError(err).
				WithFields(logTags).
				Errorf("Receive failure on subscription '%s'", receiveSubscription)
		}
	}()

	// -----------------------------------------------------------------------------------------
	// Start the daemon processors

	if err := outboundProcessor.StartEventLoop(&instance.wg); err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Unable to outbound processing task pool")
		return nil, err
	}
	if err := inboundProcessor.StartEventLoop(&instance.wg); err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Unable to inbound processing task pool")
		return nil, err
	}

	// -----------------------------------------------------------------------------------------
	// Start the timeout check timer

	if err := instance.checkTimer.Start(params.TimeoutEnforceInt, func() error {
		return instance.RequestTimeoutCheck(workerContext)
	}, false); err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Unable to start request timeout check timer")
		return nil, err
	}

	return instance, nil
}

/*
SetInboundRequestHandler set the inbound request handler

	@param ctxt context.Context - execution context
	@param handler ReqRespMessageHandler - the handler to use
*/
func (c *pubsubReqRespClient) SetInboundRequestHandler(
	ctxt context.Context, handler ReqRespMessageHandler,
) error {
	logTags := c.GetLogTagsForContext(ctxt)
	c.inboundRequestHandler = handler
	log.WithFields(logTags).Info("Changed inbound request handler")
	return nil
}

/*
Stop stop any support background tasks which were started

	@param ctxt context.Context - execution context
*/
func (c *pubsubReqRespClient) Stop(ctxt context.Context) error {
	c.processorContextCancel()

	if err := c.inboundProcessor.StopEventLoop(); err != nil {
		return err
	}

	if err := c.outboundProcessor.StopEventLoop(); err != nil {
		return err
	}

	if err := c.checkTimer.Stop(); err != nil {
		return err
	}

	return TimeBoundedWaitGroupWait(c.parentContext, &c.wg, time.Second*10)
}

// ===========================================================================================
// Make Outbound Request

type rrRequestPayload struct {
	requestID string
	request   requestContext
	message   []byte
	metadata  map[string]string
}

const (
	rrMsgAttributeNameRequestID = "__request_id__"
	rrMsgAttributeNameSenderID  = "__request_sender_id__"
	rrMsgAttributeNameTargetID  = "__request_target_id__"
	rrMsgAttributeNameIsRequest = "__is_request__"
)

/*
Request make a new request

	@param ctxt context.Context - execution context
	@param targetID string - target ID this request is destined for
	@param message []byte - request message payload
	@param metadata map[string]string - request metadata
	@param callParam RequestCallParam - request call parameters
	@return request ID
*/
func (c *pubsubReqRespClient) Request(
	ctxt context.Context,
	targetID string,
	message []byte,
	metadata map[string]string,
	callParam RequestCallParam,
) (string, error) {
	logTags := c.GetLogTagsForContext(ctxt)

	requestID := uuid.NewString()
	currentTime := time.Now().UTC()

	// Sanity check the request parameters
	if callParam.ExpectedResponsesCount < 1 {
		return "", fmt.Errorf("request which require no response does not fit supported usage")
	}
	if callParam.RespHandler == nil || callParam.TimeoutHandler == nil {
		return "", fmt.Errorf("request must define all handler callbacks")
	}

	// Define new outbound request
	requestEntry := requestContext{
		requestID:         requestID,
		senderID:          c.targetID,
		targetID:          targetID,
		createdAt:         currentTime,
		deadLine:          currentTime.Add(callParam.Timeout),
		expectedRespCount: callParam.ExpectedResponsesCount,
		receivedRespCount: 0,
		completeCallback:  nil,
		callParam:         callParam,
	}

	// Define back channel, if call is blocking
	finishSignal := make(chan bool)
	if callParam.Blocking {
		requestEntry.completeCallback = func() {
			finishSignal <- true
		}
	}

	log.
		WithFields(logTags).
		WithField("target-id", targetID).
		WithField("request-id", requestID).
		Info("Submitting new outbound REQUEST")

	// Make the request
	taskParams := rrRequestPayload{
		requestID: requestID,
		request:   requestEntry,
		message:   message,
		metadata:  metadata,
	}
	if err := c.outboundProcessor.Submit(ctxt, taskParams); err != nil {
		log.
			WithError(err).
			WithFields(logTags).
			WithField("target-id", targetID).
			WithField("request-id", requestID).
			Error("Failed to submit 'Request' job")
		return requestID, err
	}

	// Wait for all responses to be processed, if blocking
	if callParam.Blocking {
		log.
			WithFields(logTags).
			WithField("target-id", targetID).
			WithField("request-id", requestID).
			Info("Awaiting all responses")

		select {
		case <-ctxt.Done():
			err := fmt.Errorf("request timed out waiting for all responses")
			log.
				WithError(err).
				WithFields(logTags).
				WithField("target-id", targetID).
				WithField("request-id", requestID).
				Error("Outbound request failed")
			return requestID, err
		case <-finishSignal:
			log.
				WithFields(logTags).
				WithField("target-id", targetID).
				WithField("request-id", requestID).
				Info("Received all responses")
		}
	}
	return requestID, nil
}

func (c *pubsubReqRespClient) request(params interface{}) error {
	// Convert params into expected data type
	if requestParams, ok := params.(rrRequestPayload); ok {
		return c.handleRequest(requestParams)
	}
	err := fmt.Errorf("received unexpected call parameters: %s", reflect.TypeOf(params))
	logTags := c.GetLogTagsForContext(c.processorContext)
	log.WithError(err).WithFields(logTags).Error("'Request' processing failure")
	return err
}

// handleRequest contains the actual logic for `Request`
func (c *pubsubReqRespClient) handleRequest(params rrRequestPayload) error {
	lclCtxt := context.WithValue(c.processorContext, requestContextKey{}, params.request)
	logTags := c.GetLogTagsForContext(lclCtxt)

	if params.metadata == nil {
		params.metadata = make(map[string]string)
	}

	// Add additional information to message metadata
	params.metadata[rrMsgAttributeNameRequestID] = params.requestID
	params.metadata[rrMsgAttributeNameSenderID] = c.targetID
	params.metadata[rrMsgAttributeNameTargetID] = params.request.targetID
	params.metadata[rrMsgAttributeNameIsRequest] = "true"

	// Publish the message
	log.
		WithFields(logTags).
		Debug("Publishing new request")
	if _, err := c.psClient.Publish(
		c.processorContext, params.request.targetID, params.message, params.metadata, true,
	); err != nil {
		log.
			WithError(err).
			WithFields(logTags).
			Error("PubSub publish failed")
		return err
	}

	// Record the message context
	c.outboundRequests[params.requestID] = &params.request

	log.
		WithFields(logTags).
		Debug("Published new request")

	return nil
}

// ===========================================================================================
// Send Outbound Response

type rrResponsePayload struct {
	request  requestContext
	message  []byte
	metadata map[string]string
}

/*
Respond respond to an inbound request

	@param ctxt context.Context - execution context
	@param originalReq ReqRespMessage - original request
	@param message []byte - response message payload
	@param metadata map[string]string - request metadata
	@param blocking bool - whether the call is blocking
*/
func (c *pubsubReqRespClient) Respond(
	ctxt context.Context,
	originalReq ReqRespMessage,
	message []byte,
	metadata map[string]string,
	blocking bool,
) error {
	logTags := c.GetLogTagsForContext(ctxt)

	currentTime := time.Now().UTC()

	// Define inbound response context
	response := requestContext{
		requestID:        originalReq.RequestID,
		senderID:         originalReq.TargetID,
		targetID:         originalReq.SenderID,
		createdAt:        currentTime,
		completeCallback: nil,
	}

	// Define back channel, if call is blocking
	finishSignal := make(chan bool)
	if blocking {
		response.completeCallback = func() {
			finishSignal <- true
		}
	}

	log.
		WithFields(logTags).
		WithField("orig-target-id", originalReq.TargetID).
		WithField("orig-request-id", originalReq.RequestID).
		Info("Submitting new outbound RESPONSE")

	// Make the request
	taskParams := rrResponsePayload{
		request:  response,
		message:  message,
		metadata: metadata,
	}
	if err := c.inboundProcessor.Submit(ctxt, taskParams); err != nil {
		log.
			WithError(err).
			WithFields(logTags).
			WithField("orig-target-id", originalReq.TargetID).
			WithField("orig-request-id", originalReq.RequestID).
			Error("Failed to submit 'Respond' job")
		return err
	}

	// Wait for response to be sent
	if blocking {
		log.
			WithFields(logTags).
			WithField("orig-target-id", originalReq.TargetID).
			WithField("orig-request-id", originalReq.RequestID).
			Info("Wait until response sent")

		select {
		case <-ctxt.Done():
			err := fmt.Errorf("request timed out waiting response transmit")
			log.
				WithError(err).
				WithFields(logTags).
				WithField("orig-target-id", originalReq.TargetID).
				WithField("orig-request-id", originalReq.RequestID).
				Error("Outbound response failed")
			return err
		case <-finishSignal:
			log.
				WithFields(logTags).
				WithField("orig-target-id", originalReq.TargetID).
				WithField("orig-request-id", originalReq.RequestID).
				Info("Transmitted response")
		}
	}
	return nil
}

func (c *pubsubReqRespClient) respond(params interface{}) error {
	// Convert params into expected data type
	if responseParams, ok := params.(rrResponsePayload); ok {
		return c.handleRespond(responseParams)
	}
	err := fmt.Errorf("received unexpected call parameters: %s", reflect.TypeOf(params))
	logTags := c.GetLogTagsForContext(c.processorContext)
	log.WithError(err).WithFields(logTags).Error("'Respond' processing failure")
	return err
}

// handleRespond contains the actual logic for `Respond`
func (c *pubsubReqRespClient) handleRespond(params rrResponsePayload) error {
	lclCtxt := context.WithValue(c.processorContext, requestContextKey{}, params.request)
	logTags := c.GetLogTagsForContext(lclCtxt)

	if params.metadata == nil {
		params.metadata = make(map[string]string)
	}

	// Add additional information to message metadata
	params.metadata[rrMsgAttributeNameRequestID] = params.request.requestID
	params.metadata[rrMsgAttributeNameSenderID] = c.targetID
	params.metadata[rrMsgAttributeNameTargetID] = params.request.targetID
	params.metadata[rrMsgAttributeNameIsRequest] = "false"

	// Publish the message
	log.
		WithFields(logTags).
		Debug("Publishing new response")
	if _, err := c.psClient.Publish(
		c.processorContext, params.request.targetID, params.message, params.metadata, true,
	); err != nil {
		log.
			WithError(err).
			WithFields(logTags).
			Error("PubSub publish failed")
		return err
	}

	log.
		WithFields(logTags).
		Debug("Published new request")

	// Unblock the original caller
	if params.request.completeCallback != nil {
		params.request.completeCallback()
	}

	return nil
}

// ===========================================================================================
// Receive inbound messages

type rrInboundRequestPayload struct {
	requestID string
	entry     requestContext
	message   []byte
	metadata  map[string]string
}

type rrInboundResponsePayload struct {
	senderID  string
	requestID string
	timestamp time.Time
	message   []byte
	metadata  map[string]string
}

/*
ReceivePubSubMsg receive PubSub message on topic `c.targetID`

	@param ctxt context.Context - execution context
	@param pubTimestamp time.Time - timestamp when the message received by the PubSub system
	@param msg []byte - message payload
	@param metadata map[string]string - message metadata
*/
func (c *pubsubReqRespClient) ReceivePubSubMsg(
	ctxt context.Context, pubTimestamp time.Time, msg []byte, metadata map[string]string,
) error {
	logTags := c.GetLogTagsForContext(ctxt)

	requestID, ok := metadata[rrMsgAttributeNameRequestID]
	if !ok {
		err := fmt.Errorf("message did not come with request ID")
		log.WithError(err).WithFields(logTags).Error("Unable to process inbound message")
		return err
	}

	senderID, ok := metadata[rrMsgAttributeNameSenderID]
	if !ok {
		err := fmt.Errorf("message did not come with sender ID")
		log.WithError(err).WithFields(logTags).Error("Unable to process inbound message")
		return err
	}

	if msgTargetID, ok := metadata[rrMsgAttributeNameTargetID]; !ok {
		err := fmt.Errorf("message did not come with target ID")
		log.WithError(err).WithFields(logTags).Error("Unable to process inbound message")
		return err
	} else if msgTargetID != c.targetID {
		err := fmt.Errorf("message target ID does not match client: %s =/= %s", msgTargetID, c.targetID)
		log.WithError(err).WithFields(logTags).Error("Unable to process inbound message")
		return err
	}

	// Process based on whether the message is an inbound request or response
	msgType, ok := metadata[rrMsgAttributeNameIsRequest]
	if ok && msgType == "true" {
		// An inbound request

		// Define inbound request
		requestEntry := requestContext{
			requestID: requestID,
			senderID:  senderID,
			targetID:  c.targetID,
			createdAt: pubTimestamp,
		}
		// Define task param
		taskParams := rrInboundRequestPayload{
			requestID: requestID,
			entry:     requestEntry,
			message:   msg,
			metadata:  metadata,
		}

		log.
			WithFields(logTags).
			WithField("sender-id", senderID).
			WithField("request-id", requestID).
			Debug("Submitting inbound request for processing")

		// Make the request
		if err := c.inboundProcessor.Submit(ctxt, taskParams); err != nil {
			log.
				WithError(err).
				WithFields(logTags).
				WithField("sender-id", senderID).
				WithField("request-id", requestID).
				Error("Failed to submit 'ReceivePubSubMsg' job for inbound request")
			return err
		}
	} else {
		// An inbound response

		// Define task param
		taskParams := rrInboundResponsePayload{
			senderID:  senderID,
			requestID: requestID,
			timestamp: pubTimestamp,
			message:   msg,
			metadata:  metadata,
		}

		log.
			WithFields(logTags).
			WithField("sender-id", senderID).
			WithField("request-id", requestID).
			Debug("Submitting inbound response for processing")

		// Make the request
		if err := c.outboundProcessor.Submit(ctxt, taskParams); err != nil {
			log.
				WithError(err).
				WithFields(logTags).
				WithField("sender-id", senderID).
				WithField("request-id", requestID).
				Error("Failed to submit 'ReceivePubSubMsg' job for inbound response")
			return err
		}
	}

	return nil
}

// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
// Receive inbound requests

func (c *pubsubReqRespClient) receiveInboundRequestMsg(params interface{}) error {
	// Convert params into expected data type
	if inboundReqParams, ok := params.(rrInboundRequestPayload); ok {
		return c.handleInboundRequest(inboundReqParams)
	}
	err := fmt.Errorf("received unexpected call parameters: %s", reflect.TypeOf(params))
	logTags := c.GetLogTagsForContext(c.processorContext)
	log.
		WithError(err).
		WithFields(logTags).
		Error("'ReceivePubSubMsg' inbound request processing failure")
	return err
}

func (c *pubsubReqRespClient) handleInboundRequest(params rrInboundRequestPayload) error {
	// Build new context for local logging
	lclCtxt := context.WithValue(c.processorContext, requestContextKey{}, params.entry)
	logTags := c.GetLogTagsForContext(lclCtxt)

	if c.inboundRequestHandler == nil {
		err := fmt.Errorf("no handler installed for inbound requests")
		log.WithError(err).WithFields(logTags).Error("Unable to process inbound request")
		return err
	}

	// Package the message up for forwarding
	forwardMsg := ReqRespMessage{
		RequestID: params.requestID,
		SenderID:  params.entry.senderID,
		TargetID:  params.entry.targetID,
		IsRequest: true,
		Timestamp: params.entry.createdAt,
		Metadata:  params.metadata,
		Payload:   params.message,
	}

	log.WithFields(logTags).Debug("Forwarding request for processing")
	// Call the request handler
	if err := c.inboundRequestHandler(lclCtxt, forwardMsg); err != nil {
		// TODO: in the future, sort out how to handle failure in upstream processing.
		// Dead letter queue, retry, return to sender, etc.
		log.WithError(err).WithFields(logTags).Error("Upstream failed to process inbound request")
	}
	log.WithFields(logTags).Debug("Request processed")

	return nil
}

// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
// Receive inbound responses

func (c *pubsubReqRespClient) receiveInboundResponseMsg(params interface{}) error {
	// Convert params into expected data type
	if inboundRespParams, ok := params.(rrInboundResponsePayload); ok {
		return c.handleInboundResponse(inboundRespParams)
	}
	err := fmt.Errorf("received unexpected call parameters: %s", reflect.TypeOf(params))
	logTags := c.GetLogTagsForContext(c.processorContext)
	log.
		WithError(err).
		WithFields(logTags).
		Error("'ReceivePubSubMsg' inbound response processing failure")
	return err
}

func (c *pubsubReqRespClient) handleInboundResponse(params rrInboundResponsePayload) error {
	// Find the associated outbound request
	logTags := c.GetLogTagsForContext(c.processorContext)

	originalReq, ok := c.outboundRequests[params.requestID]
	if !ok {
		err := fmt.Errorf("request ID '%s' did not originate from this client", params.requestID)
		log.WithError(err).WithFields(logTags).Error("Unable to process inbound response")
		return err
	}

	// Build new context for local logging
	lclCtxt := context.WithValue(c.processorContext, requestContextKey{}, originalReq)
	logTags = c.GetLogTagsForContext(lclCtxt)

	// Package the message up for forwarding
	forwardMsg := ReqRespMessage{
		RequestID: params.requestID,
		SenderID:  params.senderID,
		TargetID:  originalReq.senderID,
		IsRequest: false,
		Timestamp: params.timestamp,
		Metadata:  params.metadata,
		Payload:   params.message,
	}

	log.WithFields(logTags).Debug("Forwarding response for processing")
	// Forward the response to the original caller
	if err := originalReq.callParam.RespHandler(lclCtxt, forwardMsg); err != nil {
		// TODO: in the future, sort out how to handle failure in upstream processing.
		// Dead letter queue, retry, return to sender, etc.
		log.WithError(err).WithFields(logTags).Error("Upstream failed to process inbound response")
	}
	log.WithFields(logTags).Debug("Response processed")

	// Record one more response collected
	originalReq.receivedRespCount++

	// Clean up if all responses received
	if originalReq.receivedRespCount == originalReq.expectedRespCount {
		log.WithFields(logTags).Debug("All expected responses received")

		// Forget the request entry
		delete(c.outboundRequests, params.requestID)

		if originalReq.completeCallback != nil {
			// Notify anyone waiting
			originalReq.completeCallback()
		}
	}

	return nil
}

// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
// Trigger request timeout enforcement

type rrRequestTimeoutCheckPayload struct {
	timestamp time.Time
}

// RequestTimeoutCheck execute to enforce request timeout
func (c *pubsubReqRespClient) RequestTimeoutCheck(ctxt context.Context) error {
	logTags := c.GetLogTagsForContext(ctxt)

	currentTime := time.Now().UTC()

	log.
		WithFields(logTags).
		Debug("Submitting request timeout check trigger")

	if err := c.outboundProcessor.Submit(
		ctxt, rrRequestTimeoutCheckPayload{timestamp: currentTime},
	); err != nil {
		log.
			WithError(err).
			WithFields(logTags).
			Error("Failed to submit 'RequestTimeoutCheck' job")
		return err
	}

	return nil
}

func (c *pubsubReqRespClient) requestTimeoutCheck(params interface{}) error {
	// Convert params into expected data type
	if requestParams, ok := params.(rrRequestTimeoutCheckPayload); ok {
		return c.handleRequestTimeoutCheck(requestParams)
	}
	err := fmt.Errorf("received unexpected call parameters: %s", reflect.TypeOf(params))
	logTags := c.GetLogTagsForContext(c.processorContext)
	log.WithError(err).WithFields(logTags).Error("'RequestTimeoutCheck' processing failure")
	return err
}

// handleRequestTimeoutCheck contains the actual logic for `RequestTimeoutCheck`
func (c *pubsubReqRespClient) handleRequestTimeoutCheck(params rrRequestTimeoutCheckPayload) error {
	logTags := c.GetLogTagsForContext(c.parentContext)

	// Go through all the request, and find the ones which have timed out
	removeRequests := []string{}
	for requestID, request := range c.outboundRequests {
		log.WithFields(logTags).Debugf("Checking request %s for timeout", request.String())
		if request.deadLine.Before(params.timestamp) && request.receivedRespCount < request.expectedRespCount {
			log.WithFields(logTags).Infof("Request '%s' has timed out", requestID)
			removeRequests = append(removeRequests, requestID)

			// Notify the original caller that the request timed out
			if err := request.callParam.TimeoutHandler(c.parentContext); err != nil {
				log.
					WithError(err).
					WithFields(logTags).
					Errorf("Errored when calling timeout handler of request '%s'", requestID)
			}
		}
	}

	// Remove the request entries
	for _, requestID := range removeRequests {
		delete(c.outboundRequests, requestID)
	}

	return nil
}
