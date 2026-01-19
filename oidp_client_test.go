package goutils_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	mockgoutils "github.com/alwitt/goutils/mocks/goutils"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func getOIDPTestParams(assert *assert.Assertions) goutils.OIDPClientParam {
	oidIssuerURL := os.Getenv("UNITTEST_OID_ISSUER_URL")
	oidpClient := os.Getenv("UNITTEST_OIDP_CLIENT")
	oidpClientCred := os.Getenv("UNITTEST_OIDP_CLIENT_CRED")
	validCheck := validator.New()
	params := goutils.OIDPClientParam{
		Issuer:          oidIssuerURL,
		ClientID:        &oidpClient,
		ClientCred:      &oidpClientCred,
		TargetAudiences: []string{getOIDPTestAudience(assert)},
	}
	assert.Nil(validCheck.Struct(&params))
	return params
}

func getOIDPTestAudience(assert *assert.Assertions) string {
	audience := os.Getenv("UNITTEST_OID_AUDIENCE")
	assert.NotEmpty(audience)
	return audience
}

func TestOpenIDProviderClientInit(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	oidpParams := getOIDPTestParams(assert)
	oidpParams.LogTags = log.Fields{"module": "unit-test"}

	httpClient, err := goutils.DefineHTTPClient(
		utCtxt,
		goutils.HTTPClientRetryConfig{
			MaxAttempts:  5,
			InitWaitTime: time.Second * 5,
			MaxWaitTime:  time.Second * 30,
		},
		nil,
		nil,
	)
	assert.Nil(err)

	uut, err := goutils.DefineOpenIDProviderClient(oidpParams, httpClient)
	assert.Nil(err)

	assert.False(uut.CanIntrospect())
}

func TestOpenIDProviderClientTokenParseValid(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	oidpParams := getOIDPTestParams(assert)
	oidpParams.LogTags = log.Fields{"module": "unit-test", "component": "oidp-client"}

	oidpHTTPClient, err := goutils.DefineHTTPClient(
		utCtxt,
		goutils.HTTPClientRetryConfig{
			MaxAttempts:  5,
			InitWaitTime: time.Second * 5,
			MaxWaitTime:  time.Second * 30,
		},
		nil,
		nil,
	)
	assert.Nil(err)

	uut, err := goutils.DefineOpenIDProviderClient(oidpParams, oidpHTTPClient)
	assert.Nil(err)
	assert.False(uut.CanIntrospect())

	tokenMgmtHTTPClient, err := goutils.DefineHTTPClient(
		utCtxt,
		goutils.HTTPClientRetryConfig{
			MaxAttempts:  5,
			InitWaitTime: time.Second * 5,
			MaxWaitTime:  time.Second * 30,
		},
		nil,
		nil,
	)
	assert.Nil(err)

	tokenMgmt, err := goutils.GetNewClientCredOAuthTokenManager(
		utCtxt, tokenMgmtHTTPClient, goutils.ClientCredOAuthTokenManagerParam{
			IDPIssuerURL:   oidpParams.Issuer,
			ClientID:       *oidpParams.ClientID,
			ClientSecret:   *oidpParams.ClientCred,
			TargetAudience: &oidpParams.TargetAudiences[0],
			LogTags:        log.Fields{"module": "unit-test", "component": "oauth-token-mgmt"},
			TimeBuffer:     time.Minute,
		},
	)
	assert.Nil(err)

	// Fetch a token
	var testToken string
	{
		timestamp := time.Now().UTC()
		lclCtx, lclCtxCancel := context.WithTimeout(utCtxt, time.Second*10)
		defer lclCtxCancel()
		testToken, err = tokenMgmt.GetToken(lclCtx, timestamp)
		assert.Nil(err)
	}

	// Verify the token
	parsedClaims := new(jwt.MapClaims)
	parsedToken, err := uut.ParseJWT(testToken, parsedClaims)
	assert.Nil(err)
	assert.NotNil(parsedToken)
	{
		t, err := json.MarshalIndent(&parsedToken, "", "  ")
		assert.Nil(err)
		log.Debugf("New Token:\n%s", string(t))
	}
	readAudience, err := parsedToken.Claims.GetAudience()
	assert.Nil(err)
	assert.Equal(oidpParams.TargetAudiences[0], readAudience[0])
	getURLHost := func(orig string) (string, error) {
		parsed, err := url.Parse(orig)
		if err != nil {
			return "", err
		}
		return parsed.Host, nil
	}
	expectedIssuer, err := getURLHost(oidpParams.Issuer)
	assert.Nil(err)
	readIssuerRaw, err := parsedToken.Claims.GetIssuer()
	assert.Nil(err)
	readIssuer, err := getURLHost(readIssuerRaw)
	assert.Nil(err)
	assert.Equal(expectedIssuer, readIssuer)

	// Clean up
	assert.Nil(tokenMgmt.Stop(utCtxt))
}

func TestOpenIDProviderClientTokenParseInvalid(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	oidpParams := getOIDPTestParams(assert)
	oidpParams.LogTags = log.Fields{"module": "unit-test", "component": "oidp-client"}

	oidpHTTPClient, err := goutils.DefineHTTPClient(
		utCtxt,
		goutils.HTTPClientRetryConfig{
			MaxAttempts:  5,
			InitWaitTime: time.Second * 5,
			MaxWaitTime:  time.Second * 30,
		},
		nil,
		nil,
	)
	assert.Nil(err)

	uut, err := goutils.DefineOpenIDProviderClient(oidpParams, oidpHTTPClient)
	assert.Nil(err)
	assert.False(uut.CanIntrospect())

	type testCase struct {
		Token    string
		ErrorMsg string
	}

	tests := []testCase{
		{
			Token:    "eyJhbGciOiJub25lIiwidHlwIjoiSldUIiwia2lkIjoiX3B6Sno3MXZCWGRKMnNkSmdVQ1RQIn0K.eyJpc3MiOiJodHRwczovL2Rldi1rcnk0emM2MzVyc3d3emttLnVzLmF1dGgwLmNvbS8iLCJzdWIiOiJvUDVkV2Yzc1ZGNnZxUnZpWHMwT3Z3S0ZvUjlPaGNOd0BjbGllbnRzIiwiYXVkIjoiaHR0cDovL3V0LmdvdXRpbHMuaGNtLm9yZyIsImlhdCI6MTc2ODc5NzQ2OSwiZXhwIjoxNzY4ODgzODY5LCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiJvUDVkV2Yzc1ZGNnZxUnZpWHMwT3Z3S0ZvUjlPaGNOdyJ9",
			ErrorMsg: "token contains an invalid number of segments",
		},
		{
			Token:    "eyJhbGciOiJub25lIiwidHlwIjoiSldUIiwia2lkIjoiX3B6Sno3MXZCWGRKMnNkSmdVQ1RQIn0K.eyJpc3MiOiJodHRwczovL2Rldi1rcnk0emM2MzVyc3d3emttLnVzLmF1dGgwLmNvbS8iLCJzdWIiOiJvUDVkV2Yzc1ZGNnZxUnZpWHMwT3Z3S0ZvUjlPaGNOd0BjbGllbnRzIiwiYXVkIjoiaHR0cDovL3V0LmdvdXRpbHMuaGNtLm9yZyIsImlhdCI6MTc2ODc5NzQ2OSwiZXhwIjoxNzY4ODgzODY5LCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiJvUDVkV2Yzc1ZGNnZxUnZpWHMwT3Z3S0ZvUjlPaGNOdyJ9.StOE0oz-rDGBQiO48L77b-HPhCB-P68MaibbriFfzVltTUTIdR2N6rQskyLBRgtdWSznueweeDOJud9rNPidycO1uY1wkOKWxe5M6jd8r2o2m8pQamWckG8jQD0mad5VGEFqE0hLXPqPnKcUfZwe4Y8rZc887kuq0GqCC0n1yuaadlCk9uw2AkAOkEhU0uOBj8VtfWUPV6UwzyoDZ1SKxSlWjd80Rwh-g_A8ItJQygWXeLcrHOts-FuZ68Z-Zxshn4RIMiKfpK72hsqAuKIs3EiuXcFdiXCxeHaZJU-ev7vEZ_M_DkOkRvZsToNxa8pfOesns5ez4QI-do3SVoLM_Q",
			ErrorMsg: "signing method none is invalid",
		},
		{
			Token:    "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImhlbGxvIn0K.eyJpc3MiOiJodHRwczovL2Rldi1rcnk0emM2MzVyc3d3emttLnVzLmF1dGgwLmNvbS8iLCJzdWIiOiJvUDVkV2Yzc1ZGNnZxUnZpWHMwT3Z3S0ZvUjlPaGNOd0BjbGllbnRzIiwiYXVkIjoiaHR0cDovL3V0LmdvdXRpbHMuaGNtLm9yZyIsImlhdCI6MTc2ODc5NzQ2OSwiZXhwIjoxNzY4ODgzODY5LCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiJvUDVkV2Yzc1ZGNnZxUnZpWHMwT3Z3S0ZvUjlPaGNOdyJ9.StOE0oz-rDGBQiO48L77b-HPhCB-P68MaibbriFfzVltTUTIdR2N6rQskyLBRgtdWSznueweeDOJud9rNPidycO1uY1wkOKWxe5M6jd8r2o2m8pQamWckG8jQD0mad5VGEFqE0hLXPqPnKcUfZwe4Y8rZc887kuq0GqCC0n1yuaadlCk9uw2AkAOkEhU0uOBj8VtfWUPV6UwzyoDZ1SKxSlWjd80Rwh-g_A8ItJQygWXeLcrHOts-FuZ68Z-Zxshn4RIMiKfpK72hsqAuKIs3EiuXcFdiXCxeHaZJU-ev7vEZ_M_DkOkRvZsToNxa8pfOesns5ez4QI-do3SVoLM_Q",
			ErrorMsg: "Encountered JWT referring public key hello which is unknown",
		},
		{
			Token:    "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9Cg.eyJpc3MiOiJodHRwczovL2Rldi1rcnk0emM2MzVyc3d3emttLnVzLmF1dGgwLmNvbS8iLCJzdWIiOiJvUDVkV2Yzc1ZGNnZxUnZpWHMwT3Z3S0ZvUjlPaGNOd0BjbGllbnRzIiwiYXVkIjoiaHR0cDovL3V0LmdvdXRpbHMuaGNtLm9yZyIsImlhdCI6MTc2ODc5NzQ2OSwiZXhwIjoxNzY4ODgzODY5LCJndHkiOiJjbGllbnQtY3JlZGVudGlhbHMiLCJhenAiOiJvUDVkV2Yzc1ZGNnZxUnZpWHMwT3Z3S0ZvUjlPaGNOdyJ9.StOE0oz-rDGBQiO48L77b-HPhCB-P68MaibbriFfzVltTUTIdR2N6rQskyLBRgtdWSznueweeDOJud9rNPidycO1uY1wkOKWxe5M6jd8r2o2m8pQamWckG8jQD0mad5VGEFqE0hLXPqPnKcUfZwe4Y8rZc887kuq0GqCC0n1yuaadlCk9uw2AkAOkEhU0uOBj8VtfWUPV6UwzyoDZ1SKxSlWjd80Rwh-g_A8ItJQygWXeLcrHOts-FuZ68Z-Zxshn4RIMiKfpK72hsqAuKIs3EiuXcFdiXCxeHaZJU-ev7vEZ_M_DkOkRvZsToNxa8pfOesns5ez4QI-do3SVoLM_Q",
			ErrorMsg: "jwt missing 'kid' field",
		},
	}

	for _, oneTest := range tests {
		// Verify the token
		parsedClaims := new(jwt.MapClaims)
		_, err = uut.ParseJWT(oneTest.Token, parsedClaims)
		assert.NotNil(err)
		assert.Contains(err.Error(), oneTest.ErrorMsg)
	}
}

func TestOpenIDProviderClientValidateTokenMiddlewareNoToken(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	mockOIDPClient := mockgoutils.NewOpenIDProviderClient(t)

	uut := goutils.DefineJWTCheckMiddleware(
		mockOIDPClient, log.Fields{"module": "unit-test", "component": "jwt-token-check"},
	)

	// Setup request
	req, err := http.NewRequest("GET", "/testing", nil)
	assert.Nil(err)

	// Setup request handler
	dummyHandler := func(_ http.ResponseWriter, _ *http.Request) {
		assert.False(true)
	}

	router := mux.NewRouter()
	respRecorder := httptest.NewRecorder()
	router.HandleFunc("/testing", uut.ParseAndValidateJWT(dummyHandler))
	router.ServeHTTP(respRecorder, req)

	assert.Equal(http.StatusUnauthorized, respRecorder.Code)
	log.Debugf("Response: %s", respRecorder.Body.String())
	{
		var resp goutils.RestAPIBaseResponse
		assert.Nil(json.Unmarshal(respRecorder.Body.Bytes(), &resp))
		assert.False(resp.Success)
		assert.Equal(http.StatusUnauthorized, resp.Error.Code)
		assert.Equal("Header 'Authorization' missing", resp.Error.Msg)
	}
}

func TestOpenIDProviderClientValidateTokenMiddlewareInvalidToken(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	mockOIDPClient := mockgoutils.NewOpenIDProviderClient(t)

	uut := goutils.DefineJWTCheckMiddleware(
		mockOIDPClient, log.Fields{"module": "unit-test", "component": "jwt-token-check"},
	)

	// Setup request
	testToken := uuid.NewString()
	req, err := http.NewRequest("GET", "/testing", nil)
	assert.Nil(err)
	req.Header.Add("Authorization", fmt.Sprintf("bearer %s", testToken))

	// Setup request handler
	dummyHandler := func(_ http.ResponseWriter, _ *http.Request) {
		assert.False(true)
	}

	errDetail := uuid.NewString()
	mockOIDPClient.On(
		"ParseJWT",
		testToken,
		mock.AnythingOfType("*jwt.MapClaims"),
	).Return(nil, fmt.Errorf("%s", errDetail))

	router := mux.NewRouter()
	respRecorder := httptest.NewRecorder()
	router.HandleFunc("/testing", uut.ParseAndValidateJWT(dummyHandler))
	router.ServeHTTP(respRecorder, req)

	assert.Equal(http.StatusUnauthorized, respRecorder.Code)
	log.Debugf("Response: %s", respRecorder.Body.String())
	{
		var resp goutils.RestAPIBaseResponse
		assert.Nil(json.Unmarshal(respRecorder.Body.Bytes(), &resp))
		assert.False(resp.Success)
		assert.Equal(http.StatusUnauthorized, resp.Error.Code)
		assert.Equal("Invalid token", resp.Error.Msg)
		assert.Equal(errDetail, resp.Error.Detail)
	}
}

func TestOpenIDProviderClientValidateTokenMiddlewareExpiredToken(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	mockOIDPClient := mockgoutils.NewOpenIDProviderClient(t)

	uut := goutils.DefineJWTCheckMiddleware(
		mockOIDPClient, log.Fields{"module": "unit-test", "component": "jwt-token-check"},
	)

	// Setup request
	testToken := uuid.NewString()
	req, err := http.NewRequest("GET", "/testing", nil)
	assert.Nil(err)
	req.Header.Add("Authorization", fmt.Sprintf("bearer %s", testToken))

	timestamp := time.Now().UTC()

	tokenExp := timestamp.Add(-time.Hour)

	testJWT := &jwt.Token{Raw: uuid.NewString()}
	testClaims := map[string]interface{}{}
	testClaims["exp"] = float64(tokenExp.Unix())
	testJWT.Claims = jwt.MapClaims(testClaims)

	mockOIDPClient.On(
		"ParseJWT",
		testToken,
		mock.AnythingOfType("*jwt.MapClaims"),
	).Return(testJWT, nil)

	// Setup request handler
	dummyHandler := func(_ http.ResponseWriter, _ *http.Request) {
		assert.False(true)
	}

	router := mux.NewRouter()
	respRecorder := httptest.NewRecorder()
	router.HandleFunc("/testing", uut.ParseAndValidateJWT(dummyHandler))
	router.ServeHTTP(respRecorder, req)

	assert.Equal(http.StatusUnauthorized, respRecorder.Code)
	log.Debugf("Response: %s", respRecorder.Body.String())
	{
		var resp goutils.RestAPIBaseResponse
		assert.Nil(json.Unmarshal(respRecorder.Body.Bytes(), &resp))
		assert.False(resp.Success)
		assert.Equal(http.StatusUnauthorized, resp.Error.Code)
		assert.Equal("Expired token", resp.Error.Msg)
	}
}

func TestOpenIDProviderClientValidateTokenMiddlewareValidToken(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	mockOIDPClient := mockgoutils.NewOpenIDProviderClient(t)

	uut := goutils.DefineJWTCheckMiddleware(
		mockOIDPClient, log.Fields{"module": "unit-test", "component": "jwt-token-check"},
	)

	// Setup request
	testToken := uuid.NewString()
	req, err := http.NewRequest("GET", "/testing", nil)
	assert.Nil(err)
	req.Header.Add("Authorization", fmt.Sprintf("bearer %s", testToken))

	timestamp := time.Now().UTC()

	tokenExp := timestamp.Add(+time.Hour)

	testJWT := &jwt.Token{Raw: uuid.NewString()}
	testClaims := map[string]interface{}{}
	testClaims["exp"] = float64(tokenExp.Unix())
	testJWT.Claims = jwt.MapClaims(testClaims)

	mockOIDPClient.On(
		"ParseJWT",
		testToken,
		mock.AnythingOfType("*jwt.MapClaims"),
	).Return(testJWT, nil)

	// Setup request handler
	dummyHandler := func(_ http.ResponseWriter, r *http.Request) {
		callContext := r.Context()
		jwtToken, err := goutils.GetJWTTokenFromContext(callContext)
		assert.Nil(err)
		assert.Equal(testJWT.Raw, jwtToken.Raw)
	}

	router := mux.NewRouter()
	respRecorder := httptest.NewRecorder()
	router.HandleFunc("/testing", uut.ParseAndValidateJWT(dummyHandler))
	router.ServeHTTP(respRecorder, req)

	assert.Equal(http.StatusOK, respRecorder.Code)
	log.Debugf("Response: %s", respRecorder.Body.String())
}
