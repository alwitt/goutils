package goutils

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

func TestRestAPIHanderRequestIDInjection(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	// Case 0: no user request ID header defined
	uutNoUserRequestIDHeader := RestAPIHandler{
		Component: Component{
			LogTags: log.Fields{"entity": "unit-tester"},
		},
		CallRequestIDHeaderField: nil,
	}
	{
		rid := uuid.New().String()
		req, err := http.NewRequest("GET", "/testing", nil)
		assert.Nil(err)
		req.Header.Add("Request-ID", rid)

		dummyHandler := func(w http.ResponseWriter, r *http.Request) {
			callContext := r.Context()
			assert.NotNil(callContext.Value(RestRequestParamKey{}))
			v, ok := callContext.Value(RestRequestParamKey{}).(RestRequestParam)
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
	uutWithUserRequestIDHeader := RestAPIHandler{
		Component: Component{
			LogTags: log.Fields{"entity": "unit-tester"},
		},
		CallRequestIDHeaderField: &testReqIDHeader,
	}
	{
		rid := uuid.New().String()
		req, err := http.NewRequest("DELETE", "/testing2", nil)
		assert.Nil(err)
		req.Header.Add(testReqIDHeader, rid)

		dummyHandler := func(w http.ResponseWriter, r *http.Request) {
			callContext := r.Context()
			assert.NotNil(callContext.Value(RestRequestParamKey{}))
			v, ok := callContext.Value(RestRequestParamKey{}).(RestRequestParam)
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

	uut := RestAPIHandler{
		Component: Component{
			LogTags: log.Fields{"entity": "unit-tester"},
		},
		DoNotLogHeaders: map[string]bool{"Not-Allowed": true},
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
			assert.NotNil(callContext.Value(RestRequestParamKey{}))
			v, ok := callContext.Value(RestRequestParamKey{}).(RestRequestParam)
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
