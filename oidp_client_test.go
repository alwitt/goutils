package goutils_test

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/alwitt/goutils"
	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
)

func getOIDPTestParams(assert *assert.Assertions) goutils.OIDPClientParam {
	oidIssuerURL := os.Getenv("UNITTEST_OID_ISSUER_URL")
	oidpClient := os.Getenv("UNITTEST_OIDP_CLIENT")
	oidpClientCred := os.Getenv("UNITTEST_OIDP_CLIENT_CRED")
	validCheck := validator.New()
	params := goutils.OIDPClientParam{
		Issuer:     oidIssuerURL,
		ClientID:   &oidpClient,
		ClientCred: &oidpClientCred,
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

func TestOpenIDProviderClientTokenParse(t *testing.T) {
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

	testOIDAudience := getOIDPTestAudience(assert)

	tokenMgmt, err := goutils.GetNewClientCredOAuthTokenManager(
		utCtxt, tokenMgmtHTTPClient, goutils.ClientCredOAuthTokenManagerParam{
			IDPIssuerURL:   oidpParams.Issuer,
			ClientID:       *oidpParams.ClientID,
			ClientSecret:   *oidpParams.ClientCred,
			TargetAudience: &testOIDAudience,
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
	assert.Equal(testOIDAudience, readAudience[0])
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
