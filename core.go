package goutils

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"net/http"
	"path"
	"runtime"
	"sync"
	"time"

	"github.com/apex/log"
)

// LogMetadataModifier is the function signature of a callback to update log.Fields with additional
// key-value pairs.
type LogMetadataModifier func(context.Context, log.Fields)

// Component is the base structure for all components
type Component struct {
	// LogTags the Apex logging message metadata tags
	LogTags log.Fields
	// LogTagModifiers is the list of log metadata modifier callbacks
	LogTagModifiers []LogMetadataModifier
}

/*
NewLogTagsForContext generates a new deep-copied LogTags for an execution context

	@return a new log.Fields
*/
func (c Component) NewLogTagsForContext() log.Fields {
	result := log.Fields{}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&c.LogTags); err != nil {
		return c.LogTags
	}
	if err := gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(&result); err != nil {
		return c.LogTags
	}
	return result
}

/*
GetLogTagsForContext creates a new Apex log.Fields metadata structure for a specific context

	@param ctxt context.Context - a request context
	@return the new Apec log.Fields metadata
*/
func (c Component) GetLogTagsForContext(ctxt context.Context) log.Fields {
	theTags := c.NewLogTagsForContext()
	for _, modifer := range c.LogTagModifiers {
		modifer(ctxt, theTags)
	}
	// Add file location
	if pc, file, lineNo, ok := runtime.Caller(1); ok {
		funcName := runtime.FuncForPC(pc).Name()
		fileName := path.Base(file)
		theTags["file"] = fileName
		theTags["line"] = lineNo
		theTags["func"] = funcName
	}
	return theTags
}

// ======================================================================================

// RestRequestParamKey associated key for RESTRequestParam when storing in request context
type RestRequestParamKey struct{}

// RestRequestParam is a helper object for logging a request's parameters into its context
type RestRequestParam struct {
	// ID is the request ID
	ID string `json:"id"`
	// Host is the request host
	Host string `json:"host" validate:"required,fqdn"`
	// URI is the request URI
	URI string `json:"uri" validate:"required,uri"`
	// Method is the request method
	Method string `json:"method" validate:"required,oneof=GET HEAD PUT POST PATCH DELETE OPTIONS"`
	// Referer is the request referer string
	Referer string `json:"referer"`
	// RemoteAddr is the request
	RemoteAddr string `json:"remote_address"`
	// Proto is the request HTTP proto string
	Proto string `json:"http_proto"`
	// ProtoMajor is the request HTTP proto major version
	ProtoMajor int `json:"http_version_major"`
	// ProtoMinor is the request HTTP proto minor version
	ProtoMinor int `json:"http_version_minor"`
	// RequestHeaders additional request headers
	RequestHeaders http.Header
	// Timestamp is when the request is first received
	Timestamp time.Time
}

// updateLogTags updates Apex log.Fields map with values the requests's parameters
func (i *RestRequestParam) updateLogTags(tags log.Fields) {
	tags["request_id"] = i.ID
	tags["request_host"] = i.Host
	tags["request_uri"] = fmt.Sprintf("'%s'", i.URI)
	tags["request_method"] = i.Method
	tags["request_referer"] = i.Referer
	tags["request_remote_address"] = i.RemoteAddr
	tags["request_proto"] = i.Proto
	tags["request_http_version_major"] = i.ProtoMajor
	tags["request_http_version_minor"] = i.ProtoMinor
	tags["request_timestamp"] = i.Timestamp.UTC().Format(time.RFC3339Nano)
	for header, headerValues := range i.RequestHeaders {
		tags[header] = headerValues
	}
}

/*
ModifyLogMetadataByRestRequestParam update log metadata with info from RestRequestParam

	@param ctxt context.Context - a request context
	@param theTags log.Fields - a log metadata to update
*/
func ModifyLogMetadataByRestRequestParam(ctxt context.Context, theTags log.Fields) {
	if ctxt.Value(RestRequestParamKey{}) != nil {
		v, ok := ctxt.Value(RestRequestParamKey{}).(RestRequestParam)
		if ok {
			v.updateLogTags(theTags)
		}
	}
}

// ======================================================================================

/*
TimeBoundedWaitGroupWait is a wrapper around wait group wait with a time limit

	@param wgCtxt context.Context - context associated with the wait group
	@param wg *sync.WaitGroup - the wait group to
	@param timeout time.Duration - wait timeout duration
*/
func TimeBoundedWaitGroupWait(
	wgCtxt context.Context, wg *sync.WaitGroup, timeout time.Duration,
) error {
	c := make(chan bool)
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return nil
	case <-wgCtxt.Done():
		return fmt.Errorf("associated context expired")
	case <-time.After(timeout):
		return fmt.Errorf("wait-group wait timed out")
	}
}

// ======================================================================================

// HTTPRequestRetryParam HTTP client request retry parameters
type HTTPRequestRetryParam struct {
	// MaxRetires maximum number of retries
	MaxRetires int
	// InitialWaitTime the initial retry wait time
	InitialWaitTime time.Duration
	// MaxWaitTime the max retry wait time
	MaxWaitTime time.Duration
}
