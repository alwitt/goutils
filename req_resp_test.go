package goutils_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestReqRespBasicOperation(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	// Create the PubSub clients
	pubsubClients := []goutils.PubSubClient{}
	for itr := 0; itr < 2; itr++ {
		coreClient, err := createTestPubSubClient(utCtxt)
		assert.Nil(err)

		psClient, err := goutils.GetNewPubSubClientInstance(
			coreClient, log.Fields{"instance": fmt.Sprintf("ut-ps-client-%d", itr)},
		)
		assert.Nil(err)

		assert.Nil(psClient.UpdateLocalTopicCache(utCtxt))
		assert.Nil(psClient.UpdateLocalSubscriptionCache(utCtxt))

		pubsubClients = append(pubsubClients, psClient)
	}

	// Create request-response clients
	rrTopics := []string{
		fmt.Sprintf("goutil-ut-rr-bo-topic-0-%s", uuid.NewString()),
		fmt.Sprintf("goutil-ut-rr-bo-topic-1-%s", uuid.NewString()),
	}
	uuts := []goutils.RequestResponseClient{}
	for itr := 0; itr < 2; itr++ {
		uut, err := goutils.GetNewPubSubRequestResponseClientInstance(
			utCtxt,
			goutils.PubSubRequestResponseClientParam{
				TargetID:           rrTopics[itr],
				Name:               "ut-client",
				PSClient:           pubsubClients[itr],
				MsgRetentionTTL:    time.Minute * 10,
				LogTags:            log.Fields{"instance": fmt.Sprintf("ut-rr-client-%d", itr)},
				CustomLogModifiers: nil,
				SupportWorkerCount: 2,
				TimeoutEnforceInt:  time.Minute,
			},
		)
		assert.Nil(err)
		uuts = append(uuts, uut)
	}

	// Sync both PubSub clients with the current set of topics and subscriptions
	for idx, psClient := range pubsubClients {
		log.Debugf("Re-syncing %d PubSub client", idx)
		assert.Nil(psClient.UpdateLocalTopicCache(utCtxt))
		assert.Nil(psClient.UpdateLocalSubscriptionCache(utCtxt))
	}

	// Install inbound request handlers
	inboundRequestChans := []chan goutils.ReqRespMessage{}
	genInboundRequestHandler := func(idx int, rxChan chan goutils.ReqRespMessage) goutils.ReqRespMessageHandler {
		return func(ctxt context.Context, msg goutils.ReqRespMessage) error {
			log.Debugf("Client %d received inbound REQUEST", idx)
			rxChan <- msg
			return nil
		}
	}
	inboundResponseChans := []chan goutils.ReqRespMessage{}
	genInboundResponseHandler := func(idx int, rxChan chan goutils.ReqRespMessage) goutils.ReqRespMessageHandler {
		return func(ctxt context.Context, msg goutils.ReqRespMessage) error {
			log.Debugf("Client %d received inbound RESPONSE", idx)
			rxChan <- msg
			return nil
		}
	}
	responsesHandlers := []goutils.ReqRespMessageHandler{}
	for itr := 0; itr < 2; itr++ {
		inboundRequestChans = append(inboundRequestChans, make(chan goutils.ReqRespMessage))
		inboundResponseChans = append(inboundResponseChans, make(chan goutils.ReqRespMessage))
		assert.Nil(uuts[itr].SetInboundRequestHandler(
			utCtxt, genInboundRequestHandler(itr, inboundRequestChans[itr]),
		))
		responsesHandlers = append(
			responsesHandlers, genInboundResponseHandler(itr, inboundResponseChans[itr]),
		)
	}

	// =========================================================================================

	// Case 0: send request 0 -> 1
	{
		lclCtxt0, cancel0 := context.WithTimeout(utCtxt, time.Second*10)
		defer cancel0()
		reqPayload := []byte(uuid.NewString())

		// Send request
		_, err := uuts[0].Request(lclCtxt0, rrTopics[1], reqPayload, nil, goutils.RequestCallParam{
			RespHandler:            responsesHandlers[0],
			ExpectedResponsesCount: 1,
			Blocking:               false,
			Timeout:                time.Second * 5,
			TimeoutHandler: func(ctxt context.Context) error {
				assert.False(true, "request is expected to timeout")
				return nil
			},
		})
		assert.Nil(err)

		// Check for request in other client
		var rxMsg goutils.ReqRespMessage
		select {
		case <-lclCtxt0.Done():
			assert.False(true, "Timed out waiting for uut[1] to receive request")
		case msg, ok := <-inboundRequestChans[1]:
			assert.True(ok)
			assert.True(msg.IsRequest)
			assert.Equal(rrTopics[1], msg.TargetID)
			assert.Equal(rrTopics[0], msg.SenderID)
			assert.Equal(reqPayload, msg.Payload)
			rxMsg = msg
		}

		lclCtxt1, cancel1 := context.WithTimeout(utCtxt, time.Second*10)
		defer cancel1()
		// Return a response
		respPayload := []byte(uuid.NewString())
		assert.Nil(uuts[1].Respond(lclCtxt1, rxMsg, respPayload, nil, false))

		// Check for response in other client
		select {
		case <-lclCtxt0.Done():
			assert.False(true, "Timed out waiting for uut[0] to receive response")
		case msg, ok := <-inboundResponseChans[0]:
			assert.True(ok)
			assert.False(msg.IsRequest)
			assert.Equal(rrTopics[0], msg.TargetID)
			assert.Equal(rrTopics[1], msg.SenderID)
			assert.Equal(respPayload, msg.Payload)
		}
	}

	// Case 1: blocking request with multi-response
	{
		lclCtxt0, cancel0 := context.WithTimeout(utCtxt, time.Second*20)
		defer cancel0()
		reqPayload := []byte(uuid.NewString())

		// Make request in separate thread
		reqWG := sync.WaitGroup{}
		reqWG.Add(1)
		go func() {
			defer reqWG.Done()
			_, err := uuts[0].Request(lclCtxt0, rrTopics[1], reqPayload, nil, goutils.RequestCallParam{
				RespHandler:            responsesHandlers[0],
				ExpectedResponsesCount: 2,
				Blocking:               true,
				Timeout:                time.Second * 5,
				TimeoutHandler: func(ctxt context.Context) error {
					assert.False(true, "request is expected to timeout")
					return nil
				},
			})
			assert.Nil(err)
		}()

		// Check for request in other client
		var rxMsg goutils.ReqRespMessage
		select {
		case <-lclCtxt0.Done():
			assert.False(true, "Timed out waiting for uut[1] to receive request")
		case msg, ok := <-inboundRequestChans[1]:
			assert.True(ok)
			assert.True(msg.IsRequest)
			assert.Equal(rrTopics[1], msg.TargetID)
			assert.Equal(rrTopics[0], msg.SenderID)
			assert.Equal(reqPayload, msg.Payload)
			rxMsg = msg
		}

		// Return two responses
		respPayloads := map[string]bool{}
		for itr := 0; itr < 2; itr++ {
			lclCtxt1, cancel1 := context.WithTimeout(utCtxt, time.Second*10)
			defer cancel1()
			payload := uuid.NewString()
			respPayloads[payload] = true
			assert.Nil(uuts[1].Respond(lclCtxt1, rxMsg, []byte(payload), nil, false))
		}

		// Check for responses in other client
		requesterReceived := map[string]bool{}
		for itr := 0; itr < 2; itr++ {
			select {
			case <-lclCtxt0.Done():
				assert.False(true, "Timed out waiting for uut[0] to receive response")
			case msg, ok := <-inboundResponseChans[0]:
				assert.True(ok)
				assert.False(msg.IsRequest)
				assert.Equal(rrTopics[0], msg.TargetID)
				assert.Equal(rrTopics[1], msg.SenderID)
				requesterReceived[string(msg.Payload)] = true
			}
		}
		assert.EqualValues(respPayloads, requesterReceived)

		assert.Nil(goutils.TimeBoundedWaitGroupWait(lclCtxt0, &reqWG, time.Second*5))
	}

	// =========================================================================================

	// Clean up
	{
		// Stop the request-response clients
		for itr := 0; itr < 2; itr++ {
			assert.Nil(uuts[itr].Stop(utCtxt))
		}

		for itr := 0; itr < 2; itr++ {
			assert.Nil(pubsubClients[itr].Close(utCtxt))

			// Delete the created subscriptions
			subscription := fmt.Sprintf("ut-client.%s", rrTopics[itr])
			assert.Nil(pubsubClients[itr].DeleteSubscription(utCtxt, subscription))

			// Delete the created topics
			assert.Nil(pubsubClients[itr].DeleteTopic(utCtxt, rrTopics[itr]))
		}
	}
}

func TestReqRespRequestTimeoutHandling(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	// Create the PubSub clients
	pubsubClients := []goutils.PubSubClient{}
	for itr := 0; itr < 2; itr++ {
		coreClient, err := createTestPubSubClient(utCtxt)
		assert.Nil(err)

		psClient, err := goutils.GetNewPubSubClientInstance(
			coreClient, log.Fields{"instance": fmt.Sprintf("ut-ps-client-%d", itr)},
		)
		assert.Nil(err)

		assert.Nil(psClient.UpdateLocalTopicCache(utCtxt))
		assert.Nil(psClient.UpdateLocalSubscriptionCache(utCtxt))

		pubsubClients = append(pubsubClients, psClient)
	}

	// Create request-response clients
	rrTopics := []string{
		fmt.Sprintf("goutil-ut-rr-rt-topic-0-%s", uuid.NewString()),
		fmt.Sprintf("goutil-ut-rr-rt-topic-1-%s", uuid.NewString()),
	}
	uuts := []goutils.RequestResponseClient{}
	for itr := 0; itr < 2; itr++ {
		uut, err := goutils.GetNewPubSubRequestResponseClientInstance(
			utCtxt,
			goutils.PubSubRequestResponseClientParam{
				TargetID:           rrTopics[itr],
				Name:               "ut-client",
				PSClient:           pubsubClients[itr],
				MsgRetentionTTL:    time.Minute * 10,
				LogTags:            log.Fields{"instance": fmt.Sprintf("ut-rr-client-%d", itr)},
				CustomLogModifiers: nil,
				SupportWorkerCount: 2,
				TimeoutEnforceInt:  time.Second * 5,
			},
		)
		assert.Nil(err)
		uuts = append(uuts, uut)
	}

	// Sync both PubSub clients with the current set of topics and subscriptions
	for idx, psClient := range pubsubClients {
		log.Debugf("Re-syncing %d PubSub client", idx)
		assert.Nil(psClient.UpdateLocalTopicCache(utCtxt))
		assert.Nil(psClient.UpdateLocalSubscriptionCache(utCtxt))
	}

	// Install inbound request handlers
	inboundRequestChans := []chan goutils.ReqRespMessage{}
	genInboundRequestHandler := func(idx int, rxChan chan goutils.ReqRespMessage) goutils.ReqRespMessageHandler {
		return func(ctxt context.Context, msg goutils.ReqRespMessage) error {
			log.Debugf("Client %d received inbound REQUEST", idx)
			rxChan <- msg
			return nil
		}
	}
	inboundResponseChans := []chan goutils.ReqRespMessage{}
	genInboundResponseHandler := func(idx int, rxChan chan goutils.ReqRespMessage) goutils.ReqRespMessageHandler {
		return func(ctxt context.Context, msg goutils.ReqRespMessage) error {
			log.Debugf("Client %d received inbound RESPONSE", idx)
			rxChan <- msg
			return nil
		}
	}
	responsesHandlers := []goutils.ReqRespMessageHandler{}
	for itr := 0; itr < 2; itr++ {
		inboundRequestChans = append(inboundRequestChans, make(chan goutils.ReqRespMessage))
		inboundResponseChans = append(inboundResponseChans, make(chan goutils.ReqRespMessage))
		assert.Nil(uuts[itr].SetInboundRequestHandler(
			utCtxt, genInboundRequestHandler(itr, inboundRequestChans[itr]),
		))
		responsesHandlers = append(
			responsesHandlers, genInboundResponseHandler(itr, inboundResponseChans[itr]),
		)
	}

	// =========================================================================================

	// Case 0: send request 0 -> 1, no response
	{
		lclCtxt0, cancel0 := context.WithTimeout(utCtxt, time.Second*10)
		defer cancel0()
		reqPayload := []byte(uuid.NewString())

		timeoutIndication := make(chan bool)
		timeoutHandler := func(ctxt context.Context) error {
			log.Debugf("Request timed out")
			timeoutIndication <- true
			return nil
		}

		// Send request
		_, err := uuts[0].Request(lclCtxt0, rrTopics[1], reqPayload, nil, goutils.RequestCallParam{
			RespHandler:            responsesHandlers[0],
			ExpectedResponsesCount: 1,
			Blocking:               false,
			Timeout:                time.Second * 5,
			TimeoutHandler:         timeoutHandler,
		})
		assert.Nil(err)

		// Check for request in other client
		select {
		case <-lclCtxt0.Done():
			assert.False(true, "Timed out waiting for uut[1] to receive request")
		case msg, ok := <-inboundRequestChans[1]:
			assert.True(ok)
			assert.True(msg.IsRequest)
			assert.Equal(rrTopics[1], msg.TargetID)
			assert.Equal(rrTopics[0], msg.SenderID)
			assert.Equal(reqPayload, msg.Payload)
		}

		// Wait for timeout
		lclCtxt1, cancel1 := context.WithTimeout(utCtxt, time.Second*10)
		defer cancel1()
		select {
		case <-lclCtxt1.Done():
			assert.False(true, "The request did not timeout")
		case <-timeoutIndication:
			// request did timeout
			break
		}
	}

	// Case 1: send request 0 -> 1. multiple responses, only one response
	{
		lclCtxt0, cancel0 := context.WithTimeout(utCtxt, time.Second*10)
		defer cancel0()
		reqPayload := []byte(uuid.NewString())

		timeoutIndication := make(chan bool)
		timeoutHandler := func(ctxt context.Context) error {
			log.Debugf("Request timed out")
			timeoutIndication <- true
			return nil
		}

		// Send request
		_, err := uuts[0].Request(lclCtxt0, rrTopics[1], reqPayload, nil, goutils.RequestCallParam{
			RespHandler:            responsesHandlers[0],
			ExpectedResponsesCount: 2,
			Blocking:               false,
			Timeout:                time.Second * 5,
			TimeoutHandler:         timeoutHandler,
		})
		assert.Nil(err)

		// Check for request in other client
		var rxMsg goutils.ReqRespMessage
		select {
		case <-lclCtxt0.Done():
			assert.False(true, "Timed out waiting for uut[1] to receive request")
		case msg, ok := <-inboundRequestChans[1]:
			assert.True(ok)
			assert.True(msg.IsRequest)
			assert.Equal(rrTopics[1], msg.TargetID)
			assert.Equal(rrTopics[0], msg.SenderID)
			assert.Equal(reqPayload, msg.Payload)
			rxMsg = msg
		}

		lclCtxt1, cancel1 := context.WithTimeout(utCtxt, time.Second*10)
		defer cancel1()
		// Return a response
		respPayload := []byte(uuid.NewString())
		assert.Nil(uuts[1].Respond(lclCtxt1, rxMsg, respPayload, nil, false))

		// Check for response in other client
		select {
		case <-lclCtxt0.Done():
			assert.False(true, "Timed out waiting for uut[0] to receive response")
		case msg, ok := <-inboundResponseChans[0]:
			assert.True(ok)
			assert.False(msg.IsRequest)
			assert.Equal(rrTopics[0], msg.TargetID)
			assert.Equal(rrTopics[1], msg.SenderID)
			assert.Equal(respPayload, msg.Payload)
		}

		// Wait for timeout
		select {
		case <-lclCtxt1.Done():
			assert.False(true, "The request did not timeout")
		case <-timeoutIndication:
			// request did timeout
			break
		}
	}

	// =========================================================================================

	// Clean up
	{
		// Stop the request-response clients
		for itr := 0; itr < 2; itr++ {
			assert.Nil(uuts[itr].Stop(utCtxt))
		}

		for itr := 0; itr < 2; itr++ {
			assert.Nil(pubsubClients[itr].Close(utCtxt))

			// Delete the created subscriptions
			subscription := fmt.Sprintf("ut-client.%s", rrTopics[itr])
			assert.Nil(pubsubClients[itr].DeleteSubscription(utCtxt, subscription))

			// Delete the created topics
			assert.Nil(pubsubClients[itr].DeleteTopic(utCtxt, rrTopics[itr]))
		}
	}
}

func TestReqRespSubscriptionReuse(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	// Create the PubSub clients
	pubsubClients := []goutils.PubSubClient{}
	for itr := 0; itr < 2; itr++ {
		coreClient, err := createTestPubSubClient(utCtxt)
		assert.Nil(err)

		psClient, err := goutils.GetNewPubSubClientInstance(
			coreClient, log.Fields{"instance": fmt.Sprintf("ut-ps-client-%d", itr)},
		)
		assert.Nil(err)

		assert.Nil(psClient.UpdateLocalTopicCache(utCtxt))
		assert.Nil(psClient.UpdateLocalSubscriptionCache(utCtxt))

		pubsubClients = append(pubsubClients, psClient)
	}

	// Create request-response clients
	rrTopic := fmt.Sprintf("goutil-ut-rr-bo-topic-0-%s", uuid.NewString())
	uuts := []goutils.RequestResponseClient{}
	for itr := 0; itr < 2; itr++ {
		log.Debugf("Re-syncing %d PubSub client", itr)
		assert.Nil(pubsubClients[itr].UpdateLocalTopicCache(utCtxt))
		assert.Nil(pubsubClients[itr].UpdateLocalSubscriptionCache(utCtxt))
		uut, err := goutils.GetNewPubSubRequestResponseClientInstance(
			utCtxt,
			goutils.PubSubRequestResponseClientParam{
				TargetID:           rrTopic,
				Name:               "ut-client",
				PSClient:           pubsubClients[itr],
				MsgRetentionTTL:    time.Minute * 10,
				LogTags:            log.Fields{"instance": fmt.Sprintf("ut-rr-client-%d", itr)},
				CustomLogModifiers: nil,
				SupportWorkerCount: 2,
				TimeoutEnforceInt:  time.Minute,
			},
		)
		assert.Nil(err)
		uuts = append(uuts, uut)
	}

	// =========================================================================================

	// Clean up
	{
		// Stop the request-response clients
		for itr := 0; itr < 2; itr++ {
			assert.Nil(uuts[itr].Stop(utCtxt))
		}

		for itr := 0; itr < 2; itr++ {
			assert.Nil(pubsubClients[itr].Close(utCtxt))
		}

		// Delete the created subscriptions
		subscription := fmt.Sprintf("ut-client.%s", rrTopic)
		assert.Nil(pubsubClients[0].DeleteSubscription(utCtxt, subscription))

		// Delete the created topics
		assert.Nil(pubsubClients[0].DeleteTopic(utCtxt, rrTopic))
	}
}
