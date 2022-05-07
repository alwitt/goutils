package goutils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/urfave/negroni"
)

// RestAPIHandler base REST API handler
type RestAPIHandler struct {
	Component
	// CallRequestIDHeaderField the HTTP header containing the request ID provided by the caller
	CallRequestIDHeaderField *string
	// DoNotLogHeaders marks the set of HTTP headers to not log
	DoNotLogHeaders map[string]bool
}

// ErrorDetail is the response detail in case of error
type ErrorDetail struct {
	// Code is the response code
	Code int `json:"code" validate:"required"`
	// Msg is an optional descriptive message
	Msg string `json:"message,omitempty"`
	// Detail is an optional descriptive message providing additional details on the error
	Detail string `json:"detail,omitempty"`
}

// RestAPIBaseResponse standard REST API response
type RestAPIBaseResponse struct {
	// Success indicates whether the request was successful
	Success bool `json:"success" validate:"required"`
	// RequestID gives the request ID to match against logs
	RequestID string `json:"request_id" validate:"required"`
	// Error are details in case of errors
	Error *ErrorDetail `json:"error,omitempty"`
}

/*
LoggingMiddleware is a support middleware to be used with Mux to perform request logging

 @param next http.HandlerFunc - the core request handler function
 @return middleware http.HandlerFunc
*/
func (h RestAPIHandler) LoggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		// use provided request id from incoming request if any
		reqID := uuid.New().String()
		if h.CallRequestIDHeaderField != nil {
			reqID = r.Header.Get(*h.CallRequestIDHeaderField)
			if reqID == "" {
				// or use some generated string
				reqID = uuid.New().String()
			}
		}
		// Construct the request param tracking structure
		params := RestRequestParam{
			ID:             reqID,
			Host:           r.Host,
			URI:            r.URL.String(),
			Method:         r.Method,
			Referer:        r.Referer(),
			RemoteAddr:     r.RemoteAddr,
			Proto:          r.Proto,
			ProtoMajor:     r.ProtoMajor,
			ProtoMinor:     r.ProtoMinor,
			Timestamp:      time.Now(),
			RequestHeaders: make(http.Header),
		}
		// Fill in the request headers
		userAgentString := "-"
		for headerField, headerValues := range r.Header {
			if _, present := h.DoNotLogHeaders[headerField]; !present {
				params.RequestHeaders[headerField] = headerValues
				if headerField == "User-Agent" && len(headerValues) > 0 {
					userAgentString = fmt.Sprintf("\"%s\"", headerValues[0])
				}
			}
		}
		requestReferer := "-"
		if r.Referer() != "" {
			requestReferer = fmt.Sprintf("\"%s\"", r.Referer())
		}
		// Construct new context
		ctxt := context.WithValue(r.Context(), RestRequestParamKey{}, params)
		// Make the request
		newRespWriter := negroni.NewResponseWriter(rw)
		next(newRespWriter, r.WithContext(ctxt))
		if h.CallRequestIDHeaderField != nil {
			newRespWriter.Header().Set(*h.CallRequestIDHeaderField, reqID)
		}
		newRespWriter.Flush()
		respTimestamp := time.Now()
		// Log result of request
		logTags := h.GetLogTagsForContext(ctxt)
		respLen := newRespWriter.Size()
		respCode := newRespWriter.Status()
		log.WithFields(logTags).
			WithField("response_code", respCode).
			WithField("response_size", respLen).
			WithField("response_timestamp", respTimestamp.UTC().Format(time.RFC3339Nano)).
			Warn(
				fmt.Sprintf(
					"%s - - [%s] \"%s %s %s\" %d %d %s %s",
					params.RemoteAddr,
					params.Timestamp.UTC().Format(time.RFC3339Nano),
					params.Method,
					params.URI,
					params.Proto,
					respCode,
					respLen,
					requestReferer,
					userAgentString,
				),
			)
	}
}

/*
ReadRequestIDFromContext reads the request ID from the request context if available

 @param ctxt context.Context - a request context
 @return if available, the request ID
*/
func (h RestAPIHandler) ReadRequestIDFromContext(ctxt context.Context) string {
	if ctxt.Value(RestRequestParamKey{}) != nil {
		v, ok := ctxt.Value(RestRequestParamKey{}).(RestRequestParam)
		if ok {
			return v.ID
		}
	}
	return ""
}

/*
GetStdRESTSuccessMsg defines a standard success message

 @param ctxt context.Context - a request context
 @return the standard REST response
*/
func (h RestAPIHandler) GetStdRESTSuccessMsg(ctxt context.Context) RestAPIBaseResponse {
	return RestAPIBaseResponse{Success: true, RequestID: h.ReadRequestIDFromContext(ctxt)}
}

/*
GetStdRESTErrorMsg defines a standard error message

 @param ctxt context.Context - a request context
 @param respCode int - the request response code
 @param errMsg string - the error message
 @param errDetail string - the details on the error
 @return the standard REST response
*/
func (h RestAPIHandler) GetStdRESTErrorMsg(
	ctxt context.Context, respCode int, errMsg string, errDetail string,
) RestAPIBaseResponse {
	return RestAPIBaseResponse{
		Success:   false,
		RequestID: h.ReadRequestIDFromContext(ctxt),
		Error:     &ErrorDetail{Code: respCode, Msg: errMsg, Detail: errDetail},
	}
}

/*
WriteRESTResponse helper function to write out the REST API response

 @param w http.ResponseWriter - response writer
 @param respCode int - the response code
 @param resp interface{} - the response body
 @param headers map[string]string - the response header
 @return whether write succeeded
*/
func (h RestAPIHandler) WriteRESTResponse(
	w http.ResponseWriter, respCode int, resp interface{}, headers map[string]string,
) error {
	w.Header().Set("content-type", "application/json")
	for name, value := range headers {
		w.Header().Set(name, value)
	}
	w.WriteHeader(respCode)
	t, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return err
	}
	if _, err = w.Write(t); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return err
	}
	return nil
}
