package goutils

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net/http"
	"time"

	"github.com/apex/log"
)

// Component is the base structure for all components
type Component struct {
	// LogTags the Apex logging message metadata tags
	LogTags log.Fields
}

// NewLogTagsForContext generates a new deep-copied LogTags for an execution context
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
