package goutils_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alwitt/goutils"
	"github.com/apex/log"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

func TestHTTPRequestsMetricsCollection(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	uut, err := goutils.GetNewMetricsCollector(
		log.Fields{"entity": "metric-collector"}, []goutils.LogMetadataModifier{
			goutils.ModifyLogMetadataByRestRequestParam,
		},
	)
	assert.Nil(err)

	uut.InstallApplicationMetrics()

	httpMetricsAgent := uut.InstallHTTPMetrics()

	testHTTPMiddleware := goutils.RestAPIHandler{
		Component: goutils.Component{
			LogTags: log.Fields{"entity": "ut-middleware"},
			LogTagModifiers: []goutils.LogMetadataModifier{
				goutils.ModifyLogMetadataByRestRequestParam,
			},
		},
		DoNotLogHeaders: map[string]bool{},
		LogLevel:        goutils.HTTPLogLevelDEBUG,
		MetricsHelper:   httpMetricsAgent,
	}

	type testParam struct {
		method string
		status int
	}
	tests := []testParam{
		{method: "GET", status: 200},
		{method: "PUT", status: 300},
		{method: "POST", status: 400},
		{method: "DELETE", status: 500},
	}

	for _, test := range tests {
		req, err := http.NewRequest(test.method, "/ut/testing", nil)
		assert.Nil(err)

		testHandler := func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(test.method, r.Method)
			w.WriteHeader(test.status)
			_, err := w.Write([]byte(uuid.NewString()))
			assert.Nil(err)
		}

		testRootRouter := mux.NewRouter()
		testMetricRouter := testRootRouter.PathPrefix("/metric").Subrouter()
		testAppRouter := testRootRouter.PathPrefix("/ut").Subrouter()

		// Setup metric reporting endpoint
		uut.ExposeCollectionEndpoint(testMetricRouter, "/report", 4)

		// Setup test route
		respRecorder := httptest.NewRecorder()
		testAppRouter.HandleFunc("/testing", testHTTPMiddleware.LoggingMiddleware(testHandler))

		testAppRouter.ServeHTTP(respRecorder, req)
		assert.Equal(test.status, respRecorder.Code)
	}

	// Read the metrics
	{
		testRootRouter := mux.NewRouter()
		testMetricRouter := testRootRouter.PathPrefix("/metric").Subrouter()

		// Setup metric reporting endpoint
		uut.ExposeCollectionEndpoint(testMetricRouter, "/report", 4)

		req, err := http.NewRequest("GET", "/metric/report", nil)
		assert.Nil(err)
		respRecorder := httptest.NewRecorder()
		testMetricRouter.ServeHTTP(respRecorder, req)
		assert.Equal(http.StatusOK, respRecorder.Code)
		report, err := io.ReadAll(respRecorder.Body)
		assert.Nil(err)
		log.Debugf("Metrics report:\n%s\n", string(report))
	}
}
