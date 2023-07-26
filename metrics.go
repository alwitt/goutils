package goutils

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/apex/log"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Standard metrics provided with the package
const (
	// ====================================================================================
	// HTTP

	// metricsNameHTTPRequest HTTP request tracking. Additional parameters are attached via labels
	//
	// - method: [GET, PUT, POST, DELETE, HEAD, PATCH, OPTIONS]
	//
	// - HTTP version
	//
	// - status code ENUM: [2XX, 3XX, 4XX, 5XX+]
	metricsNameHTTPRequest = "http_request_total"

	// metricsNameHTTPRequestLatency HTTP request latency tracking
	metricsNameHTTPRequestLatency = "http_request_latency_secs_total"

	// metricsNameHTTPResponseSize HTTP response size tracking
	metricsNameHTTPResponseSize = "http_response_size_bytes_total"
)

// Standard metrics labels provided with the package
const (
	// labelNameHTTPMethod HTTP request method label name
	labelNameHTTPMethod = "method"

	// labelNameHTTPStatus HTTP status label name
	labelNameHTTPStatus = "status"
)

// MetricsCollector metrics collection support client
type MetricsCollector interface {
	/*
		InstallApplicationMetrics install trackers for Golang application execution metrics
	*/
	InstallApplicationMetrics()

	/*
		InstallHTTPMetrics install trackers for HTTP request metrics collection. This will return
		a helper agent to record the metrics.

			@returns request metrics logging agent
	*/
	InstallHTTPMetrics() HTTPRequestMetricHelper

	/*
		ExposeCollectionEndpoint expose the Prometheus metric collection endpoint

			@param outer *mux.Router - HTTP router to install endpoint on
			@param metricsPath string - metrics endpoint path relative to the router provided
			@param maxSupportedRequest int - max number of request the endpoint will support
	*/
	ExposeCollectionEndpoint(router *mux.Router, metricsPath string, maxSupportedRequest int)
}

// metricsCollectorImpl implements MetricsCollector
type metricsCollectorImpl struct {
	Component
	lock        sync.Mutex
	prometheus  *prometheus.Registry
	httpMetrics HTTPRequestMetricHelper
}

/*
GetNewMetricsCollector get metrics collection support client

	@param logTags log.Fields - metadata fields to include in the logs
	@param customLogModifiers []LogMetadataModifier - additional log metadata modifiers to use
	@returns metric collection support client
*/
func GetNewMetricsCollector(
	logTags log.Fields,
	customLogModifiers []LogMetadataModifier,
) (MetricsCollector, error) {
	instance := &metricsCollectorImpl{
		Component: Component{
			LogTags:         logTags,
			LogTagModifiers: customLogModifiers,
		},
		lock:        sync.Mutex{},
		prometheus:  prometheus.NewRegistry(),
		httpMetrics: nil,
	}

	return instance, nil
}

func (c *metricsCollectorImpl) InstallApplicationMetrics() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.prometheus.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
}

func (c *metricsCollectorImpl) InstallHTTPMetrics() HTTPRequestMetricHelper {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.httpMetrics != nil {
		return c.httpMetrics
	}

	requestTracker := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricsNameHTTPRequest,
			Help: "HTTP request tracking",
		},
		[]string{labelNameHTTPMethod, labelNameHTTPStatus},
	)
	latencyTracker := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricsNameHTTPRequestLatency,
			Help: "HTTP request latency tracking",
		},
		[]string{labelNameHTTPMethod, labelNameHTTPStatus},
	)
	respSizeTracker := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricsNameHTTPResponseSize,
			Help: "HTTP response size tracking",
		},
		[]string{labelNameHTTPMethod, labelNameHTTPStatus},
	)
	c.prometheus.MustRegister(
		requestTracker, latencyTracker, respSizeTracker,
	)
	c.httpMetrics = &httpRequestMetricHelperImpl{
		requestTracker:  requestTracker,
		latencyTracker:  latencyTracker,
		respSizeTracker: respSizeTracker,
	}
	return c.httpMetrics
}

func (c *metricsCollectorImpl) ExposeCollectionEndpoint(
	router *mux.Router, metricsPath string, maxSupportedRequest int,
) {
	router.
		Path(metricsPath).
		Methods("GET", "POST").
		Handler(promhttp.HandlerFor(c.prometheus, promhttp.HandlerOpts{
			MaxRequestsInFlight: maxSupportedRequest,
			EnableOpenMetrics:   true,
		}))
}

// HTTPRequestMetricHelper HTTP request metric recording helper agent
type HTTPRequestMetricHelper interface {
	/*
		RecordRequest record parameters regarding a request to the metrics

			@param method string - HTTP request method
			@param status int - HTTP response status
			@param latency time.Duration - delay between request received, and response sent
			@param respSize int64 - HTTP response size in bytes
	*/
	RecordRequest(method string, status int, latency time.Duration, respSize int64)
}

// httpRequestMetricHelperImpl implements HTTPRequestMetricHelper
type httpRequestMetricHelperImpl struct {
	requestTracker  *prometheus.CounterVec
	latencyTracker  *prometheus.CounterVec
	respSizeTracker *prometheus.CounterVec
}

func (t *httpRequestMetricHelperImpl) RecordRequest(
	method string, status int, latency time.Duration, respSize int64,
) {
	// Standardize HTTP methods to upper case
	method = strings.ToUpper(method)
	// Convert HTTP response status to a enum
	statusStr := httpRespCodeToMetricLabel(status)

	// Record request
	t.requestTracker.
		With(prometheus.Labels{labelNameHTTPMethod: method, labelNameHTTPStatus: statusStr}).
		Inc()

	// Record request latency
	t.latencyTracker.
		With(prometheus.Labels{labelNameHTTPMethod: method, labelNameHTTPStatus: statusStr}).
		Add(latency.Seconds())

	// Record response size
	t.respSizeTracker.
		With(prometheus.Labels{labelNameHTTPMethod: method, labelNameHTTPStatus: statusStr}).
		Add(float64(respSize))
}

// httpRespCodeToMetricLabel helper function to quantize the HTTP response status code into
// defined catagories.
func httpRespCodeToMetricLabel(status int) string {
	if status >= http.StatusInternalServerError {
		return "5XX"
	} else if status >= http.StatusBadRequest && status < http.StatusInternalServerError {
		return "4XX"
	} else if status >= http.StatusMultipleChoices && status < http.StatusBadRequest {
		return "3XX"
	}
	return "2XX"
}
