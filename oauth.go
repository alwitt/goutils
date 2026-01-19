package goutils

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/apex/log"
	"github.com/go-playground/validator/v10"
	"github.com/go-resty/resty/v2"
)

// openIDIssuerConfig holds the OpenID issuer's API info.
//
// This is typically read from http://{{ OpenID issuer }}/.well-known/openid-configuration.
type openIDIssuerConfig struct {
	Issuer               string   `json:"issuer"`
	AuthorizationEP      string   `json:"authorization_endpoint"`
	TokenEP              string   `json:"token_endpoint"`
	IntrospectionEP      string   `json:"introspection_endpoint"`
	TokenIntrospectionEP string   `json:"token_introspection_endpoint"`
	UserinfoEP           string   `json:"userinfo_endpoint"`
	EndSessionEP         string   `json:"end_session_endpoint"`
	JwksURI              string   `json:"jwks_uri"`
	ClientRegistrationEP string   `json:"registration_endpoint"`
	RevocationEP         string   `json:"revocation_endpoint"`
	TokenSigningMethods  []string `json:"token_endpoint_auth_signing_alg_values_supported"`
	TokenEPAuthMethods   []string `json:"token_endpoint_auth_methods_supported"`
	ClaimsSupported      []string `json:"claims_supported"`
}

// OAuthTokenManager Oauth token manager handles fetching and refreshing of OAuth tokens
type OAuthTokenManager interface {
	/*
		GetToken fetch the current valid OAuth token

		 @param ctxt context.Context - the execution context
		 @param timestamp time.Time - the current timestamp
		 @returns the token
	*/
	GetToken(ctxt context.Context, timestamp time.Time) (string, error)

	/*
		Stop stop any support background tasks which were started

		 @param ctxt context.Context - execution context
	*/
	Stop(ctxt context.Context) error
}

// clientCredOAuthTokenManager client credential flow oauth token manager
type clientCredOAuthTokenManager struct {
	Component
	httpClient       *resty.Client
	tasks            TaskProcessor
	clientID         string
	clientSecret     string
	tokenAudience    *string
	idpConfig        openIDIssuerConfig
	workerCtxt       context.Context
	workerCtxtCancel context.CancelFunc
	token            *string
	tokenExpire      time.Time
	timeBuffer       time.Duration
	wg               sync.WaitGroup
	validate         *validator.Validate
}

// ClientCredOAuthTokenManagerParam configuration for client credential flow oauth token manager
type ClientCredOAuthTokenManagerParam struct {
	// IDPIssuerURL OpenID provider issuing URL
	IDPIssuerURL string `validate:"required,url"`
	// ClientID OAuth client ID
	ClientID string `validate:"required"`
	// ClientSecret OAuth client secret
	ClientSecret string `validate:"required"`
	// TargetAudience the token's target audience
	TargetAudience *string
	// LogTags metadata fields to include in the logs
	LogTags log.Fields
	// CustomLogModifiers additional log metadata modifiers to use
	CustomLogModifiers []LogMetadataModifier
	// TimeBuffer time buffer before a token expires to perform the token refresh / renew.
	// This helps in situations where there is a time offset between the client and the
	// server.
	TimeBuffer time.Duration

	// SupportTaskMetricsHelper metrics collection helper for the support tasks
	SupportTaskMetricsHelper TaskProcessorMetricHelper
}

/*
GetNewClientCredOAuthTokenManager get client credential flow oauth token manager

	@param parentCtxt context.Context - parent context
	@param httpClient *resty.Client - use this HTTP client to interact with the IDP
	@param params ClientCredOAuthTokenManagerParam - configuration for the token manager
	@returns new OAuthTokenManager instance
*/
func GetNewClientCredOAuthTokenManager(
	parentCtxt context.Context,
	httpClient *resty.Client,
	params ClientCredOAuthTokenManagerParam,
) (OAuthTokenManager, error) {
	validate := validator.New()
	if err := validate.Struct(&params); err != nil {
		return nil, err
	}

	params.LogTags["idp-issuer"] = params.IDPIssuerURL
	params.LogTags["oauth-client"] = params.ClientID

	workerCtxt, workerCtxtCancel := context.WithCancel(parentCtxt)

	// -----------------------------------------------------------------------------------------
	// Query the OpenID provider config first
	var idpConfig openIDIssuerConfig
	idpCfgEP := fmt.Sprintf("%s/.well-known/openid-configuration", params.IDPIssuerURL)
	log.WithFields(params.LogTags).Infof("Fetching IDP config at %s", idpCfgEP)
	resp, err := httpClient.R().SetResult(&idpConfig).Get(idpCfgEP)
	if err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Failed to read IDP config")
		workerCtxtCancel()
		return nil, err
	}
	if !resp.IsSuccess() {
		err := fmt.Errorf("got status code %d when reading IDP config", resp.StatusCode())
		log.WithError(err).WithFields(params.LogTags).Error("Failed to read IDP config")
		workerCtxtCancel()
		return nil, err
	}
	{
		t, _ := json.Marshal(&idpConfig)
		log.WithFields(params.LogTags).Debugf("OpenID config: %s", t)
	}

	// -----------------------------------------------------------------------------------------
	// Prepare instance

	instance := &clientCredOAuthTokenManager{
		Component: Component{
			LogTags:         params.LogTags,
			LogTagModifiers: []LogMetadataModifier{modifyLogMetadataByRRRequestParam},
		},
		httpClient:       httpClient,
		idpConfig:        idpConfig,
		clientID:         params.ClientID,
		clientSecret:     params.ClientSecret,
		tokenAudience:    params.TargetAudience,
		workerCtxt:       workerCtxt,
		workerCtxtCancel: workerCtxtCancel,
		token:            nil,
		tokenExpire:      time.Time{},
		timeBuffer:       params.TimeBuffer,
		wg:               sync.WaitGroup{},
		validate:         validate,
	}

	// Add additional log tag modifiers
	instance.LogTagModifiers = append(instance.LogTagModifiers, params.CustomLogModifiers...)

	// -----------------------------------------------------------------------------------------
	// Define worker task

	workerLogTags := log.Fields{}
	for lKey, lVal := range params.LogTags {
		workerLogTags[lKey] = lVal
	}
	workerLogTags["sub-module"] = "core-worker"
	worker, err := GetNewTaskProcessorInstance(
		workerCtxt,
		"core-worker",
		8,
		workerLogTags,
		params.SupportTaskMetricsHelper,
	)
	if err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Unable to define worker")
		return nil, err
	}
	instance.tasks = worker

	// -----------------------------------------------------------------------------------------
	// Define support tasks

	if err := worker.AddToTaskExecutionMap(
		reflect.TypeOf(getTokenRequest{}), instance.processGetToken,
	); err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Unable to install task definition")
		return nil, err
	}

	// -----------------------------------------------------------------------------------------
	// Start worker
	if err := worker.StartEventLoop(&instance.wg); err != nil {
		log.WithError(err).WithFields(params.LogTags).Error("Unable to start support worker")
		return nil, err
	}

	return instance, nil
}

type getTokenRequest struct {
	timestamp time.Time
	resultCB  func(string)
	errorCB   func(error)
}

func (c *clientCredOAuthTokenManager) GetToken(
	ctxt context.Context, timestamp time.Time,
) (string, error) {
	logTags := c.GetLogTagsForContext(ctxt)

	resultChan := make(chan string, 1)
	errorChan := make(chan error, 1)

	resultCB := func(token string) {
		resultChan <- token
	}
	errorCB := func(err error) {
		errorChan <- err
	}

	// Make the request
	request := getTokenRequest{timestamp: timestamp, resultCB: resultCB, errorCB: errorCB}
	log.WithFields(logTags).Debug("Submitting 'GetToken' job")
	if err := c.tasks.Submit(ctxt, request); err != nil {
		log.WithError(err).WithFields(logTags).Error("Failed to submit 'GetToken' job")
		return "", err
	}
	log.WithFields(logTags).Debug("Submitted 'GetToken' job. AWaiting response")

	select {
	case <-ctxt.Done():
		err := fmt.Errorf("request timed out waiting for response")
		log.WithError(err).WithFields(logTags).Error("Unable to get current active token")
		return "", err
	case err, ok := <-errorChan:
		if !ok {
			err = fmt.Errorf("error channel failure")
		}
		log.WithError(err).WithFields(logTags).Error("Unable to get current active token")
		return "", err
	case token, ok := <-resultChan:
		if !ok {
			err := fmt.Errorf("result channel failure")
			log.WithError(err).WithFields(logTags).Error("Unable to get current active token")
			return "", err
		}
		return token, nil
	}
}

func (c *clientCredOAuthTokenManager) processGetToken(params interface{}) error {
	// Convert params into expected data type
	if requestParams, ok := params.(getTokenRequest); ok {
		return c.handleGetToken(requestParams)
	}
	err := fmt.Errorf("received unexpected call parameters: %s", reflect.TypeOf(params))
	logTags := c.GetLogTagsForContext(c.workerCtxt)
	log.WithError(err).WithFields(logTags).Error("'GetToken' processing failure")
	return err
}

func (c *clientCredOAuthTokenManager) handleGetToken(params getTokenRequest) error {
	logTags := c.GetLogTagsForContext(c.workerCtxt)

	if c.token == nil || c.tokenExpire.Before(params.timestamp.Add(c.timeBuffer)) {
		log.WithFields(logTags).Debug("Fetching new token")

		// Get new token
		buildRequest := map[string]string{
			"client_id":     c.clientID,
			"client_secret": c.clientSecret,
			"grant_type":    "client_credentials",
		}
		if c.tokenAudience != nil {
			buildRequest["audience"] = *c.tokenAudience
		}

		// Make the request
		type tokenResp struct {
			Token string `json:"access_token" validate:"required"`
			TTL   uint32 `json:"expires_in" validate:"gte=0"`
		}

		var newToken tokenResp
		resp, err := c.httpClient.
			R().
			SetHeader("Content-Type", "application/x-www-form-urlencoded").
			SetHeader("Accept", "application/json").
			SetFormData(buildRequest).
			SetResult(&newToken).
			Post(c.idpConfig.TokenEP)
		if err != nil {
			log.WithError(err).WithFields(logTags).Error("Token fetch failure")
			params.errorCB(err)
			return err
		}
		if !resp.IsSuccess() {
			err := fmt.Errorf(
				"token fetch returned status code %d '%s'", resp.StatusCode(), string(resp.Body()),
			)
			log.WithError(err).WithFields(logTags).Error("Token fetch failure")
			params.errorCB(err)
			return err
		}

		log.WithFields(logTags).Debugf("Token response is %s", resp.Body())

		if err := c.validate.Struct(&newToken); err != nil {
			log.WithError(err).WithFields(logTags).Error("Invalid token response")
			params.errorCB(err)
			return err
		}

		// Compute when the token will expire
		expireAt := params.timestamp.Add(time.Second * time.Duration(newToken.TTL))

		// Store token
		c.token = &newToken.Token
		c.tokenExpire = expireAt

		log.WithFields(logTags).Debugf("New token expires at '%s'", expireAt)
	} else {
		log.WithFields(logTags).Debug("Reusing existing token")
	}

	// Return current active token
	params.resultCB(*c.token)

	return nil
}

func (c *clientCredOAuthTokenManager) Stop(_ context.Context) error {
	c.workerCtxtCancel()

	return c.tasks.StopEventLoop()
}
