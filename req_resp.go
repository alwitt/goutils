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
	tags["req_sender_id"] = c.senderID
	tags["req_target_id"] = c.targetID
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
	outboundRequests       map[string]requestContext
	processorContext       context.Context
	processorContextCancel context.CancelFunc
	wg                     sync.WaitGroup
}

/*
GetNewPubSubRequestResponseClientInstance get PubSub based RequestResponseClient

	@param parentCtxt context.Context - parent context
	@param clientTargetID string - the ID to target to send a request (or response) to this client
	@param clientName string - client instance name
	@param client PubSubClient - base PubSub client
	@param msgRetentionTTL time.Duration - PubSub message TTL, after which the message is purged
	@param logTags log.Fields - metadata fields to include in the logs
	@param addLogModifiers []LogMetadataModifier - additional log metadata modifiers to use
	@param supportWorkerCount int - number of support workers to spawn to process incoming messages
	@return new RequestResponseClient instance
*/
func GetNewPubSubRequestResponseClientInstance(
	parentCtxt context.Context,
	clientTargetID string,
	clientName string,
	client PubSubClient,
	msgRetentionTTL time.Duration,
	logTags log.Fields,
	addLogModifiers []LogMetadataModifier,
	supportWorkerCount int,
) (RequestResponseClient, error) {
	logTags["rr-instance"] = clientName
	logTags["rr-target-id"] = clientTargetID

	workerContext, workerCtxtCancel := context.WithCancel(parentCtxt)

	// -----------------------------------------------------------------------------------------
	// Prepare PubSub topic and subscription for inbound messages

	// Prepare the topic
	if err := client.CreateTopic(
		parentCtxt, clientTargetID, &pubsub.TopicConfig{RetentionDuration: msgRetentionTTL},
	); err != nil {
		log.
			WithError(err).
			WithFields(logTags).
			Errorf("Failed when creating PubSub topic '%s'", clientTargetID)
		workerCtxtCancel()
		return nil, err
	}

	// Prepare the subscription
	receiveSubscription := fmt.Sprintf("%s.%s", clientName, clientTargetID)
	if err := client.CreateSubscription(
		parentCtxt,
		clientTargetID,
		receiveSubscription,
		pubsub.SubscriptionConfig{RetentionDuration: msgRetentionTTL},
	); err != nil {
		log.
			WithError(err).
			WithFields(logTags).
			Errorf("Failed when creating PubSub subscription '%s'", receiveSubscription)
		workerCtxtCancel()
		return nil, err
	}

	// -----------------------------------------------------------------------------------------
	// Setup support daemon for processing inbound requests
	procInboundInstanceName := fmt.Sprintf("%s-inbound-process", clientName)
	procInboundLogTags := log.Fields{}
	for lKey, lVal := range logTags {
		procInboundLogTags[lKey] = lVal
	}
	procInboundLogTags["module"] = procInboundInstanceName
	inboundProcessor, err := GetNewTaskDemuxProcessorInstance(
		workerContext,
		procInboundInstanceName,
		supportWorkerCount*2,
		supportWorkerCount,
		procInboundLogTags,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Error("Unable to define inbound request processor")
		workerCtxtCancel()
		return nil, err
	}

	// Setup support daemon for processing outbound requests
	procOutboundInstanceName := fmt.Sprintf("%s-outbound-process", clientName)
	procOutboundLogTags := log.Fields{}
	for lKey, lVal := range logTags {
		procOutboundLogTags[lKey] = lVal
	}
	procOutboundLogTags["module"] = procOutboundInstanceName
	outboundProcessor, err := GetNewTaskProcessorInstance(
		workerContext, procOutboundInstanceName, supportWorkerCount*2, procOutboundLogTags,
	)
	if err != nil {
		log.WithError(err).WithFields(logTags).Error("Unable to define outbound request processor")
		workerCtxtCancel()
		return nil, err
	}

	// -----------------------------------------------------------------------------------------
	// Build the component
	instance := &pubsubReqRespClient{
		Component: Component{
			LogTags: logTags, LogTagModifiers: []LogMetadataModifier{modifyLogMetadataByRRRequestParam},
		},
		targetID:               clientTargetID,
		psClient:               client,
		inboundProcessor:       inboundProcessor,
		inboundRequestHandler:  nil,
		outboundProcessor:      outboundProcessor,
		outboundRequests:       make(map[string]requestContext),
		processorContext:       workerContext,
		processorContextCancel: workerCtxtCancel,
		wg:                     sync.WaitGroup{},
	}
	// Add additional log tag modifiers
	instance.LogTagModifiers = append(instance.LogTagModifiers, addLogModifiers...)

	// -----------------------------------------------------------------------------------------
	// Define support tasks for processing outgoing requests

	if err := outboundProcessor.AddToTaskExecutionMap(
		reflect.TypeOf(rrRequestPayload{}), instance.request,
	); err != nil {
		log.WithError(err).WithFields(logTags).Error("Unable to install task definition")
		return nil, err
	}

	// Incoming responses correspond with outgoing requests
	if err := outboundProcessor.AddToTaskExecutionMap(
		reflect.TypeOf(rrInboundResponsePayload{}), instance.receiveInboundResponseMsg,
	); err != nil {
		log.WithError(err).WithFields(logTags).Error("Unable to install task definition")
		return nil, err
	}

	// -----------------------------------------------------------------------------------------
	// Define support tasks for processing incoming requests

	if err := inboundProcessor.AddToTaskExecutionMap(
		reflect.TypeOf(rrInboundRequestPayload{}), instance.receiveInboundRequestMsg,
	); err != nil {
		log.WithError(err).WithFields(logTags).Error("Unable to install task definition")
		return nil, err
	}

	// -----------------------------------------------------------------------------------------
	// Listen for messages on previously created subscription

	instance.wg.Add(1)
	go func() {
		defer instance.wg.Done()
		err := client.Subscribe(workerContext, receiveSubscription, instance.ReceivePubSubMsg)
		logTags := instance.GetLogTagsForContext(workerContext)
		if err != nil {
			log.
				WithError(err).
				WithFields(logTags).
				Errorf("Receive failure on subscription '%s'", receiveSubscription)
		}
	}()

	// -----------------------------------------------------------------------------------------
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
	log.WithFields(logTags).Info("Changed in bound request handler")
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

	c.wg.Wait()
	return nil
}

// ===========================================================================================
// Make Outbound Request

type rrRequestPayload struct {
	requestID string
	entry     requestContext
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
		Info("Submitting new outbound request")

	// Make the request
	taskParams := rrRequestPayload{
		requestID: requestID,
		entry:     requestEntry,
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
	logTags := c.GetLogTagsForContext(c.processorContext)

	// Add additional information to message metadata
	params.metadata[rrMsgAttributeNameRequestID] = params.requestID
	params.metadata[rrMsgAttributeNameSenderID] = c.targetID
	params.metadata[rrMsgAttributeNameTargetID] = params.entry.targetID
	params.metadata[rrMsgAttributeNameIsRequest] = "true"

	// Publish the message
	log.
		WithFields(logTags).
		WithField("target-id", params.entry.targetID).
		WithField("request-id", params.requestID).
		Debug("Publishing new request")
	if _, err := c.psClient.Publish(
		c.processorContext, params.entry.targetID, params.message, params.metadata, true,
	); err != nil {
		log.
			WithError(err).
			WithFields(logTags).
			WithField("target-id", params.entry.targetID).
			WithField("request-id", params.requestID).
			Error("PubSub publish failed")
		return err
	}

	// Record the message context
	c.outboundRequests[params.requestID] = params.entry

	log.
		WithFields(logTags).
		WithField("target-id", params.entry.targetID).
		WithField("request-id", params.requestID).
		Debug("Published new request")

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
		IsRequest: false,
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
		SenderID:  originalReq.senderID,
		TargetID:  originalReq.targetID,
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
		// Notify anyone waiting
		originalReq.completeCallback()
	}

	return nil
}
