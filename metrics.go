package goutils

import (
	"context"
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
	// - status code ENUM: [2XX, 3XX, 4XX, 5XX+]
	metricsNameHTTPRequest = "http_request_total"

	// metricsNameHTTPRequestLatency HTTP request latency tracking. Additional parameters are
	// attached via labels
	//
	// - method: [GET, PUT, POST, DELETE, HEAD, PATCH, OPTIONS]
	//
	// - status code ENUM: [2XX, 3XX, 4XX, 5XX+]
	metricsNameHTTPRequestLatency = "http_request_latency_secs_total"

	// metricsNameHTTPResponseSize HTTP response size tracking. Additional parameters are
	// attached via labels
	//
	// - method: [GET, PUT, POST, DELETE, HEAD, PATCH, OPTIONS]
	//
	// - status code ENUM: [2XX, 3XX, 4XX, 5XX+]
	metricsNameHTTPResponseSize = "http_response_size_bytes_total"

	// ====================================================================================
	// PubSub

	// metricsNamePubSubPublish PubSub publish tracking. Additional parameters are attached
	// via labels
	//
	// - topic
	//
	// - success
	metricsNamePubSubPublish = "pubsub_publish_total"

	// metricsNamePubSubPublishPayloadSize PubSub publish message size tracking. Additional
	// parameters are attached via labels
	//
	// - topic
	//
	// - success
	metricsNamePubSubPublishPayloadSize = "pubsub_publish_payload_size_bytes_total"

	// metricsNamePubSubReceive PubSub receive message tracking. Additional parameters are
	// attacked via labels
	//
	// - topic
	//
	// - success
	metricsNamePubSubReceive = "pubsub_receive_total"

	// metricsNamePubSubReceivePayloadSize PubSub receive message size tracking. Additional
	// parameters are attached via labels
	//
	// - topic
	//
	// - success
	metricsNamePubSubReceivePayloadSize = "pubsub_receive_payload_size_bytes_total"

	// ====================================================================================
	// Task Processor

	// metricsNameTaskProcessorSubmit Task processor submission tracking. Additional
	// parameters are attached via labels
	//
	// - processor instance
	//
	// - success
	metricsNameTaskProcessorSubmit = "task_processor_submit_total"

	// metricsNameTaskProcessorProcessed Task processor processed task tracking. Additional
	// parameters are attached via labels
	//
	// - processor instance
	metricsNameTaskProcessorProcessed = "task_processor_processed_total"
)

// Standard metrics labels provided with the package
const (
	// ====================================================================================
	// HTTP

	// labelNameHTTPMethod HTTP request method label name
	labelNameHTTPMethod = "method"

	// labelNameHTTPStatus HTTP status label name
	labelNameHTTPStatus = "status"

	// ====================================================================================
	// PubSub

	// labelNamePubSubTopic PubSub topic label name
	labelNamePubSubTopic = "topic"

	// labelNamePubSubSuccess whether processing is successful or not
	labelNamePubSubSuccess = "success"

	// ====================================================================================
	// Task Processor

	// labelNameTaskProcessorInstance task processor instance name
	labelNameTaskProcessorInstance = "instance"

	// labelNameTaskProcessorSuccess whether task submission is successful or not
	labelNameTaskProcessorSuccess = "success"
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
		InstallPubSubMetrics install trackers for PubSub messaging collection. This will return
		a helper agent to record the metrics.

			@return PubSub metrics logging agent
	*/
	InstallPubSubMetrics() PubSubMetricHelper

	/*
		InstallTaskProcessorMetrics install tracker for Task processor operations. This will return
		a helper agent to record the metrics

			@return Task process logging agent
	*/
	InstallTaskProcessorMetrics() TaskProcessorMetricHelper

	/*
		InstallCustomCounterVecMetrics install new custom `CounterVec` metrics

			@param ctxt context.Context - execution context
			@param metricsName string - metrics name
			@param metricsHelpMessage string - metrics help message
			@param metricsLabels []string - labels to support
			@returns new `CounterVec` handle
	*/
	InstallCustomCounterVecMetrics(
		ctxt context.Context, metricsName string, metricsHelpMessage string, metricsLabels []string,
	) (*prometheus.CounterVec, error)

	/*
		InstallCustomGaugeVecMetrics install new custom `GaugeVec` metrics

			@param ctxt context.Context - execution context
			@param metricsName string - metrics name
			@param metricsHelpMessage string - metrics help message
			@param metricsLabels []string - labels to support
			@returns new `GaugeVec` handle
	*/
	InstallCustomGaugeVecMetrics(
		ctxt context.Context, metricsName string, metricsHelpMessage string, metricsLabels []string,
	) (*prometheus.GaugeVec, error)

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
	lock                 sync.Mutex
	prometheus           *prometheus.Registry
	httpMetrics          HTTPRequestMetricHelper
	pubsubMetrics        PubSubMetricHelper
	taskProcessorMetrics TaskProcessorMetricHelper
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

func (c *metricsCollectorImpl) InstallPubSubMetrics() PubSubMetricHelper {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.pubsubMetrics != nil {
		return c.pubsubMetrics
	}

	publishTracker := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricsNamePubSubPublish,
			Help: "PubSub publish tracking",
		},
		[]string{labelNamePubSubTopic, labelNamePubSubSuccess},
	)
	publishPayloadTracker := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricsNamePubSubPublishPayloadSize,
			Help: "PubSub publish message size tracking",
		},
		[]string{labelNamePubSubTopic, labelNamePubSubSuccess},
	)
	receiveTracker := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricsNamePubSubReceive,
			Help: "PubSub receive message tracking",
		},
		[]string{labelNamePubSubTopic, labelNamePubSubSuccess},
	)
	receivePayloadTracker := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricsNamePubSubReceivePayloadSize,
			Help: "PubSub receive message size tracking",
		},
		[]string{labelNamePubSubTopic, labelNamePubSubSuccess},
	)
	c.prometheus.MustRegister(
		publishTracker, publishPayloadTracker, receiveTracker, receivePayloadTracker,
	)
	c.pubsubMetrics = &pubsubMetricHelperImpl{
		publishTracker:        publishTracker,
		publishPayloadTracker: publishPayloadTracker,
		receiveTracker:        receiveTracker,
		receivePayloadTracker: receivePayloadTracker,
	}
	return c.pubsubMetrics
}

func (c *metricsCollectorImpl) InstallTaskProcessorMetrics() TaskProcessorMetricHelper {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.taskProcessorMetrics != nil {
		return c.taskProcessorMetrics
	}

	submitTrakcer := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricsNameTaskProcessorSubmit,
			Help: "Task processor submission tracking",
		},
		[]string{labelNameTaskProcessorInstance, labelNameTaskProcessorSuccess},
	)
	processTracker := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: metricsNameTaskProcessorProcessed,
			Help: "Task processor processed tasks tracking",
		},
		[]string{labelNameTaskProcessorInstance},
	)
	c.prometheus.MustRegister(submitTrakcer, processTracker)
	c.taskProcessorMetrics = &taskProcessorMetricHelperImpl{
		submitTracker:  submitTrakcer,
		processTracker: processTracker,
	}
	return c.taskProcessorMetrics
}

func (c *metricsCollectorImpl) InstallCustomCounterVecMetrics(
	ctxt context.Context, metricsName string, metricsHelpMessage string, metricsLabels []string,
) (*prometheus.CounterVec, error) {
	logTags := c.GetLogTagsForContext(ctxt)
	newMetricsTracker := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: metricsName, Help: metricsHelpMessage}, metricsLabels,
	)
	if err := c.prometheus.Register(newMetricsTracker); err != nil {
		log.
			WithError(err).
			WithFields(logTags).
			Errorf("Failed to register new metrics '%s'", metricsName)
		return nil, err
	}
	return newMetricsTracker, nil
}

func (c *metricsCollectorImpl) InstallCustomGaugeVecMetrics(
	ctxt context.Context, metricsName string, metricsHelpMessage string, metricsLabels []string,
) (*prometheus.GaugeVec, error) {
	logTags := c.GetLogTagsForContext(ctxt)
	newMetricsTracker := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: metricsName, Help: metricsHelpMessage}, metricsLabels,
	)
	if err := c.prometheus.Register(newMetricsTracker); err != nil {
		log.
			WithError(err).
			WithFields(logTags).
			Errorf("Failed to register new metrics '%s'", metricsName)
		return nil, err
	}
	return newMetricsTracker, nil
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

// PubSubMetricHelper PubSub publish and receive metric recording helper agent
type PubSubMetricHelper interface {
	/*
		RecordPublish record PubSub publish message

			@param topic string - PubSub topic
			@param successful bool - whether the operation was successful
			@param payloadLen int64 - publish payload length
	*/
	RecordPublish(topic string, successful bool, payloadLen int64)

	/*
		RecordReceive record PubSub receive message

			@param topic string - PubSub topic
			@param successful bool - whether the operation was successful
			@param payloadLen int64 - receive payload length
	*/
	RecordReceive(topic string, successful bool, payloadLen int64)
}

type pubsubMetricHelperImpl struct {
	publishTracker        *prometheus.CounterVec
	publishPayloadTracker *prometheus.CounterVec
	receiveTracker        *prometheus.CounterVec
	receivePayloadTracker *prometheus.CounterVec
}

func (t *pubsubMetricHelperImpl) RecordPublish(topic string, successful bool, payloadLen int64) {
	successStr := "true"
	if !successful {
		successStr = "false"
	}
	t.publishTracker.
		With(prometheus.Labels{labelNamePubSubTopic: topic, labelNamePubSubSuccess: successStr}).
		Inc()
	t.publishPayloadTracker.
		With(prometheus.Labels{labelNamePubSubTopic: topic, labelNamePubSubSuccess: successStr}).
		Add(float64(payloadLen))
}

func (t *pubsubMetricHelperImpl) RecordReceive(topic string, successful bool, payloadLen int64) {
	successStr := "true"
	if !successful {
		successStr = "false"
	}
	t.receiveTracker.
		With(prometheus.Labels{labelNamePubSubTopic: topic, labelNamePubSubSuccess: successStr}).
		Inc()
	t.receivePayloadTracker.
		With(prometheus.Labels{labelNamePubSubTopic: topic, labelNamePubSubSuccess: successStr}).
		Add(float64(payloadLen))
}

// TaskProcessorMetricHelper Task processor metric recording helper agent
type TaskProcessorMetricHelper interface {
	/*
		RecordSubmit record task submission

			@param instance string - task processor instance name
			@param successful bool - whether the operation was successful
	*/
	RecordSubmit(instance string, successful bool)

	/*
		RecordSubmit record task processed

			@param instance string - task processor instance name
	*/
	RecordProcessed(instance string)
}

type taskProcessorMetricHelperImpl struct {
	submitTracker  *prometheus.CounterVec
	processTracker *prometheus.CounterVec
}

func (t *taskProcessorMetricHelperImpl) RecordSubmit(instance string, successful bool) {
	successStr := "true"
	if !successful {
		successStr = "false"
	}
	t.submitTracker.With(prometheus.Labels{
		labelNameTaskProcessorInstance: instance, labelNameTaskProcessorSuccess: successStr,
	}).Inc()
}

func (t *taskProcessorMetricHelperImpl) RecordProcessed(instance string) {
	t.processTracker.With(prometheus.Labels{labelNameTaskProcessorInstance: instance}).Inc()
}
