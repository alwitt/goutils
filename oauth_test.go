package goutils_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	"github.com/apex/log"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

func TestClientCredOAuthTokenManager(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	utCtxt := context.Background()

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	testClient := resty.New()
	// Install with mock
	httpmock.ActivateNonDefault(testClient.GetClient())

	// ------------------------------------------------------------------------------------
	// Prepare mock

	idpBaseURL := "http://idp.testing.dev"
	configURL := fmt.Sprintf("%s/.well-known/openid-configuration", idpBaseURL)
	tokenURL := fmt.Sprintf("%s/auth/token", idpBaseURL)

	testCfg := goutils.ClientCredOAuthTokenManagerParam{
		IDPIssuerURL:       idpBaseURL,
		ClientID:           uuid.NewString(),
		ClientSecret:       uuid.NewString(),
		TargetAudience:     "http://application.testing.dev",
		LogTags:            log.Fields{"module": "goutils", "component": "client-cred-oauth"},
		CustomLogModifiers: []goutils.LogMetadataModifier{},
	}

	// Prepare to receive IDP config call
	httpmock.RegisterResponder(
		"GET",
		configURL,
		func(r *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(200, map[string]string{
				"token_endpoint": tokenURL,
			})
		},
	)

	uut, err := goutils.GetNewClientCredOAuthTokenManager(utCtxt, testClient, testCfg)
	assert.Nil(err)

	currentTime := time.Now().UTC()

	// Case 0: get token
	testToken0 := map[string]interface{}{
		"access_token": uuid.NewString(),
		"expires_in":   300,
	}
	{
		// Prepare mock
		httpmock.RegisterResponder(
			"POST",
			tokenURL,
			func(r *http.Request) (*http.Response, error) {
				req, err := io.ReadAll(r.Body)
				assert.Nil(err)

				reqBody := map[string]string{}
				assert.Nil(json.Unmarshal(req, &reqBody))

				clientID, ok := reqBody["client_id"]
				assert.True(ok)
				assert.Equal(testCfg.ClientID, clientID)

				clientSecret, ok := reqBody["client_secret"]
				assert.True(ok)
				assert.Equal(testCfg.ClientSecret, clientSecret)

				audience, ok := reqBody["audience"]
				assert.True(ok)
				assert.Equal(testCfg.TargetAudience, audience)

				grantType, ok := reqBody["grant_type"]
				assert.True(ok)
				assert.Equal("client_credentials", grantType)

				// Return the token
				return httpmock.NewJsonResponse(200, testToken0)
			},
		)

		waitChan := make(chan bool, 1)

		lclCtxt, lclCancel := context.WithTimeout(utCtxt, time.Second)
		go func() {
			workingToken, err := uut.GetToken(lclCtxt, currentTime)
			assert.Nil(err)
			assert.Equal(testToken0["access_token"], workingToken)
			waitChan <- true
		}()

		select {
		case <-lclCtxt.Done():
			assert.False(true, "request timed out")
		case <-waitChan:
			break
		}
		lclCancel()
	}

	// Clear mocks
	httpmock.Reset()

	// Case 1: get token again
	{
		lclCtxt, lclCancel := context.WithTimeout(utCtxt, time.Second)
		workingToken, err := uut.GetToken(lclCtxt, currentTime)
		assert.Nil(err)
		assert.Equal(testToken0["access_token"], workingToken)
		lclCancel()
	}

	// Case 2: token timeout, get new token
	testToken1 := map[string]interface{}{
		"access_token": uuid.NewString(),
		"expires_in":   300,
	}
	currentTime = currentTime.Add(time.Second * 400)
	{
		// Prepare mock
		httpmock.RegisterResponder(
			"POST",
			tokenURL,
			func(r *http.Request) (*http.Response, error) {
				return httpmock.NewJsonResponse(200, testToken1)
			},
		)

		waitChan := make(chan bool, 1)

		lclCtxt, lclCancel := context.WithTimeout(utCtxt, time.Second)
		go func() {
			workingToken, err := uut.GetToken(lclCtxt, currentTime)
			assert.Nil(err)
			assert.Equal(testToken1["access_token"], workingToken)
			waitChan <- true
		}()

		select {
		case <-lclCtxt.Done():
			assert.False(true, "request timed out")
		case <-waitChan:
			break
		}
		lclCancel()
	}

	// Clean up
	assert.Nil(uut.Stop(utCtxt))
}
