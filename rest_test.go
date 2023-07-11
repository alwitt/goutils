package goutils_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

func TestRestAPIHanderRequestIDInjection(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	// Case 0: no user request ID header defined
	uutNoUserRequestIDHeader := goutils.RestAPIHandler{
		Component: goutils.Component{
			LogTags: log.Fields{"entity": "unit-tester"},
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		CallRequestIDHeaderField: nil,
		LogLevel:                 goutils.HTTPLogLevelDEBUG,
	}
	{
		rid := uuid.New().String()
		req, err := http.NewRequest("GET", "/testing", nil)
		assert.Nil(err)
		req.Header.Add("Request-ID", rid)

		dummyHandler := func(w http.ResponseWriter, r *http.Request) {
			callContext := r.Context()
			assert.NotNil(callContext.Value(goutils.RestRequestParamKey{}))
			v, ok := callContext.Value(goutils.RestRequestParamKey{}).(goutils.RestRequestParam)
			assert.True(ok)
			assert.NotEqual(rid, v.ID)
			assert.Equal("GET", v.Method)
			assert.Equal("/testing", v.URI)
		}

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/testing", uutNoUserRequestIDHeader.LoggingMiddleware(dummyHandler))
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		assert.Equal("", (respRecorder.Header().Get("Request-ID")))
	}

	// Case 1: user request ID header defined
	testReqIDHeader := uuid.New().String()
	uutWithUserRequestIDHeader := goutils.RestAPIHandler{
		Component: goutils.Component{
			LogTags: log.Fields{"entity": "unit-tester"},
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		CallRequestIDHeaderField: &testReqIDHeader,
		LogLevel:                 goutils.HTTPLogLevelINFO,
	}
	{
		rid := uuid.New().String()
		req, err := http.NewRequest("DELETE", "/testing2", nil)
		assert.Nil(err)
		req.Header.Add(testReqIDHeader, rid)

		dummyHandler := func(w http.ResponseWriter, r *http.Request) {
			callContext := r.Context()
			assert.NotNil(callContext.Value(goutils.RestRequestParamKey{}))
			v, ok := callContext.Value(goutils.RestRequestParamKey{}).(goutils.RestRequestParam)
			assert.True(ok)
			assert.Equal(rid, v.ID)
			assert.Equal("DELETE", v.Method)
			assert.Equal("/testing2", v.URI)
		}

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/testing2", uutWithUserRequestIDHeader.LoggingMiddleware(dummyHandler))
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
		assert.Equal(rid, (respRecorder.Header().Get(testReqIDHeader)))
	}
}

func TestRestAPIHandlerRequestLogging(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	uut := goutils.RestAPIHandler{
		Component: goutils.Component{
			LogTags: log.Fields{"entity": "unit-tester"},
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		DoNotLogHeaders: map[string]bool{"Not-Allowed": true},
		LogLevel:        goutils.HTTPLogLevelDEBUG,
	}
	{
		value1 := uuid.New().String()
		value2 := uuid.New().String()
		req, err := http.NewRequest("GET", "/testing", nil)
		assert.Nil(err)
		req.Header.Add("Allowed", value1)
		req.Header.Add("Not-Allowed", value2)

		dummyHandler := func(w http.ResponseWriter, r *http.Request) {
			callContext := r.Context()
			assert.NotNil(callContext.Value(goutils.RestRequestParamKey{}))
			v, ok := callContext.Value(goutils.RestRequestParamKey{}).(goutils.RestRequestParam)
			assert.True(ok)
			assert.Equal("GET", v.Method)
			assert.Equal("/testing", v.URI)
			assert.Equal(value1, v.RequestHeaders.Get("Allowed"))
			assert.Equal("", v.RequestHeaders.Get("Not-Allowed"))
		}

		router := mux.NewRouter()
		respRecorder := httptest.NewRecorder()
		router.HandleFunc("/testing", uut.LoggingMiddleware(dummyHandler))
		router.ServeHTTP(respRecorder, req)

		assert.Equal(http.StatusOK, respRecorder.Code)
	}
}

func TestRestAPIHandlerProcessStreamingEndpoints(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	testReqIDHeader := uuid.New().String()
	uut := goutils.RestAPIHandler{
		Component: goutils.Component{
			LogTags: log.Fields{"entity": "unit-tester"},
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		CallRequestIDHeaderField: &testReqIDHeader,
		LogLevel:                 goutils.HTTPLogLevelDEBUG,
	}

	type testMessage struct {
		Timestamp time.Time
		Msg       string
	}

	testMsgTX := make(chan testMessage, 1)
	testMsgRX := make(chan testMessage, 1)

	wg := sync.WaitGroup{}
	defer wg.Wait()
	utCtxt, ctxtCancel := context.WithCancel(context.Background())
	defer ctxtCancel()

	// Define streaming data handler
	testHandler := func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		assert.True(ok)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		log.Debug("Starting stream response handler")
		complete := false
		for !complete {
			select {
			case <-utCtxt.Done():
				complete = true
			case msg, ok := <-testMsgTX:
				assert.True(ok)
				t, err := json.Marshal(&msg)
				assert.Nil(err)
				fmt.Fprintf(w, "%s\n", t)
				flusher.Flush()
				log.Debugf("Sent %s\n", t)
			}
		}
		log.Debug("Stoping stream response handler")
	}

	router := mux.NewRouter()
	router.HandleFunc("/testing", uut.LoggingMiddleware(testHandler))

	// Define HTTP server
	testServerPort := rand.Intn(30000) + 32769
	testServerListen := fmt.Sprintf("127.0.0.1:%d", testServerPort)
	testServer := &http.Server{
		Addr:    testServerListen,
		Handler: router,
	}
	// Start the HTTP server
	log.Debugf("Starting test server on %s", testServerListen)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := testServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			assert.Nil(err)
		}
		log.Debugf("Stopped test server on %s", testServerListen)
	}()
	defer func() {
		// Helper function to shutdown the server
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		if err := testServer.Shutdown(ctx); err != nil {
			assert.Nil(err)
		}
	}()

	// Define test HTTP client
	testClient := http.Client{}
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/testing", testServerListen), nil)
	assert.Nil(err)
	testRID := uuid.New().String()
	req.Header.Add(testReqIDHeader, testRID)

	// Make the request in another thread
	wg.Add(1)
	go func() {
		defer wg.Done()
		var resp *http.Response
		var err error
		for i := 0; i < 3; i++ {
			log.Debug("Connecting to test server")
			resp, err = testClient.Do(req)
			if err == nil {
				break
			}
			time.Sleep(time.Millisecond * 25)
		}
		log.Debugf("Connected to test server http://%s/testing", testServerListen)
		assert.Nil(err)
		assert.Equal(http.StatusOK, resp.StatusCode)
		assert.Equal(testRID, resp.Header.Get(testReqIDHeader))
		// Process the resp stream
		scanner := bufio.NewScanner(resp.Body)
		scanner.Split(bufio.ScanLines)
		log.Debug("Scanning SSE stream")
		for scanner.Scan() {
			received := scanner.Text()
			log.Debugf("Received: %s", received)
			var parsed testMessage
			assert.Nil(json.Unmarshal([]byte(received), &parsed))
			testMsgRX <- parsed
		}
		log.Debug("Stopped scanner")
	}()

	// Send message multiple times
	for i := 0; i < 4; i++ {
		newMsg := testMessage{Timestamp: time.Now(), Msg: uuid.New().String()}
		testMsgTX <- newMsg
		ctxt, lclCancel := context.WithTimeout(utCtxt, time.Millisecond*100)
		defer lclCancel()
		select {
		case <-ctxt.Done():
			assert.Nil(ctxt.Err())
		case rx, ok := <-testMsgRX:
			assert.True(ok)
			assert.Equal(newMsg.Timestamp.UnixMicro(), rx.Timestamp.UnixMicro())
			assert.Equal(newMsg.Msg, rx.Msg)
		}
	}

	// Allow for clean shutdown
	ctxtCancel()
}
