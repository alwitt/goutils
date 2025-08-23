package goutils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/apex/log"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	"github.com/urfave/negroni"
)

// ==============================================================================
// Base HTTP Request Handling

// HTTPRequestLogLevel HTTP request log level data type
type HTTPRequestLogLevel string

// HTTP request log levels
const (
	HTTPLogLevelWARN  HTTPRequestLogLevel = "warn"
	HTTPLogLevelINFO  HTTPRequestLogLevel = "info"
	HTTPLogLevelDEBUG HTTPRequestLogLevel = "debug"
)

// RestAPIHandler base REST API handler
type RestAPIHandler struct {
	Component
	// CallRequestIDHeaderField the HTTP header containing the request ID provided by the caller
	CallRequestIDHeaderField *string
	// DoNotLogHeaders marks the set of HTTP headers to not log
	DoNotLogHeaders map[string]bool
	// LogLevel configure the request logging level
	LogLevel HTTPRequestLogLevel
	// MetricsHelper HTTP request metric collection agent
	MetricsHelper HTTPRequestMetricHelper
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
		if h.CallRequestIDHeaderField != nil {
			newRespWriter.Header().Add(*h.CallRequestIDHeaderField, reqID)
		}
		next(newRespWriter, r.WithContext(ctxt))
		newRespWriter.Flush()
		respTimestamp := time.Now()
		// Log result of request
		logTags := h.GetLogTagsForContext(ctxt)
		respLen := newRespWriter.Size()
		respCode := newRespWriter.Status()
		// Log level based on config
		logHandle := log.WithFields(logTags).
			WithField("response_code", respCode).
			WithField("response_size", respLen).
			WithField("response_timestamp", respTimestamp.UTC().Format(time.RFC3339Nano))
		switch h.LogLevel {
		case HTTPLogLevelDEBUG:
			logHandle.Debugf(
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
			)

		case HTTPLogLevelINFO:
			logHandle.Infof(
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
			)

		default:
			logHandle.Warnf(
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
			)
		}

		// Record metrics
		if h.MetricsHelper != nil {
			h.MetricsHelper.RecordRequest(
				r.Method, respCode, respTimestamp.Sub(params.Timestamp), int64(respLen),
			)
		}
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

// ==============================================================================
// HTTP Client Support

// HTTPClientAuthConfig HTTP client OAuth middleware configuration
//
// Currently only support client-credential OAuth flow configuration
type HTTPClientAuthConfig struct {
	// IssuerURL OpenID provider issuer URL
	IssuerURL string `json:"issuer"`
	// ClientID OAuth client ID
	ClientID string `json:"client_id"`
	// ClientSecret OAuth client secret
	ClientSecret string `json:"client_secret"`
	// TargetAudience target audience `aud` to acquire a token for
	TargetAudience *string `json:"target_audience"`
	// LogTags auth middleware log tags
	LogTags log.Fields
}

// HTTPClientRetryConfig HTTP client config retry configuration
type HTTPClientRetryConfig struct {
	// MaxAttempts max number of retry attempts
	MaxAttempts int `json:"max_attempts"`
	// InitWaitTime wait time before the first wait retry
	InitWaitTime time.Duration `json:"initialWaitTimeInSec"`
	// MaxWaitTime max wait time
	MaxWaitTime time.Duration `json:"maxWaitTimeInSec"`
}

// setHTTPClientRetryParam helper function to install retry on HTTP client
func setHTTPClientRetryParam(
	client *resty.Client, config HTTPClientRetryConfig,
) *resty.Client {
	return client.
		SetRetryCount(config.MaxAttempts).
		SetRetryWaitTime(config.InitWaitTime).
		SetRetryMaxWaitTime(config.MaxWaitTime)
}

// installHTTPClientAuthMiddleware install OAuth middleware on HTTP client
func installHTTPClientAuthMiddleware(
	parentCtxt context.Context,
	parentClient *resty.Client,
	config HTTPClientAuthConfig,
	retryCfg HTTPClientRetryConfig,
) error {
	// define client specifically to be used by
	oauthHTTPClient := setHTTPClientRetryParam(resty.New(), retryCfg)

	// define OAuth token manager
	oauthMgmt, err := GetNewClientCredOAuthTokenManager(
		parentCtxt, oauthHTTPClient, ClientCredOAuthTokenManagerParam{
			IDPIssuerURL:   config.IssuerURL,
			ClientID:       config.ClientID,
			ClientSecret:   config.ClientSecret,
			TargetAudience: config.TargetAudience,
			LogTags:        config.LogTags,
		},
	)
	if err != nil {
		return err
	}

	// Build middleware for parent client
	parentClient.OnBeforeRequest(func(c *resty.Client, r *resty.Request) error {
		// Fetch OAuth token for request
		token, err := oauthMgmt.GetToken(parentCtxt, time.Now().UTC())
		if err != nil {
			return err
		}

		// Apply the token
		r.SetAuthToken(token)

		return nil
	})

	return nil
}

/*
DefineHTTPClient helper function to define a resty HTTP client

	@param parentCtxt context.Context - caller context
	@param retryConfig HTTPClientRetryConfig - HTTP client retry config
	@param authConfig *HTTPClientAuthConfig - HTTP client auth config
	@returns new resty client
*/
func DefineHTTPClient(
	parentCtxt context.Context,
	retryConfig HTTPClientRetryConfig,
	authConfig *HTTPClientAuthConfig,
) (*resty.Client, error) {
	newClient := resty.New()

	// Configure resty client retry setting
	newClient = setHTTPClientRetryParam(newClient, retryConfig)

	// Install OAuth middleware
	if authConfig != nil {
		err := installHTTPClientAuthMiddleware(parentCtxt, newClient, *authConfig, retryConfig)
		if err != nil {
			return nil, err
		}
	}

	return newClient, nil
}
