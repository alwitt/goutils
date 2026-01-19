package goutils

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/apex/log"
	"github.com/go-resty/resty/v2"
	"github.com/golang-jwt/jwt/v5"
)

// OpenIDProviderClient a client to interact with an OpenID provider
type OpenIDProviderClient interface {
	/*
		AssociatedPublicKey fetches the associated public based on "kid" value of a JWT token

		 @param token *jwt.Token - the JWT token to find the public key for
		 @return public key material
	*/
	AssociatedPublicKey(token *jwt.Token) (interface{}, error)

	/*
		ParseJWT parses a string into a JWT token object.

		 @param raw string - the original JWT string
		 @param claimStore jwt.Claims - the object to store the claims in
		 @return the parsed JWT token object
	*/
	ParseJWT(raw string, claimStore jwt.Claims) (*jwt.Token, error)

	/*
		CanIntrospect whether the client can perform introspection

		 @return whether the client can perform introspection
	*/
	CanIntrospect() bool

	/*
		IntrospectToken perform introspection for a token

		 @param ctxt context.Context - the operating context
		 @param token string - the token to introspect
		 @return whether token is still valid
	*/
	IntrospectToken(ctxt context.Context, token string) (bool, error)
}

// oidpSigningJWK the public key used by the OpenID provider to sign tokens
type oidpSigningJWK struct {
	Algorithm string `json:"alg"`
	Exponent  string `json:"e"`
	Modulus   string `json:"n"`
	ID        string `json:"kid"`
	Type      string `json:"kty"`
	Use       string `json:"use"`
}

// oidpClientImpl implements OpenIDProviderClient
type oidpClientImpl struct {
	Component
	cfg             openIDIssuerConfig
	hostOverride    *string
	httpClient      *resty.Client
	publicKey       map[string]interface{}
	clientID        *string
	clientSecret    *string
	targetAudiences []string
}

// OIDPClientParam defines connection parameters to one OpenID provider
type OIDPClientParam struct {
	// Issuer is the URL of the OpenID provider issuer URL
	Issuer string `json:"issuer" validate:"required,url"`
	// ClientID is the client ID to use during token introspection
	ClientID *string `json:"client_id" validate:"omitempty"`
	// ClientCred is the client credential to use during token introspection
	ClientCred *string `json:"client_cred" validate:"omitempty"`
	// RequestHostOverride if specified, use this as "Host" header when communicating with provider
	RequestHostOverride *string `json:"host_override" validate:"omitempty"`
	// TargetAudiences a set of audiences which are accepted
	TargetAudiences []string `json:"target_audiences"`
	// LogTags metadata fields to include in the logs
	LogTags log.Fields
}

/*
DefineOpenIDProviderClient defines a new OpenID provider client

	@param params OpenIDProviderConfig - OpenID provider client parameters
	@param httpClient *resty.Client - the HTTP client to use to communicate with the OpenID provider
	@return new client instance
*/
func DefineOpenIDProviderClient(
	params OIDPClientParam, httpClient *resty.Client,
) (OpenIDProviderClient, error) {
	params.LogTags["idp-issuer"] = params.Issuer
	params.LogTags["oauth-client"] = params.ClientID

	// Read the OpenID config first
	var cfg openIDIssuerConfig
	cfgEP := fmt.Sprintf("%s/.well-known/openid-configuration", params.Issuer)
	log.WithFields(params.LogTags).Debugf("OpenID provider config at %s", cfgEP)
	resp, err := httpClient.R().SetResult(&cfg).Get(cfgEP)
	if err != nil {
		log.WithError(err).WithFields(params.LogTags).Errorf("GET %s call failure", cfgEP)
		return nil, err
	}
	if !resp.IsSuccess() {
		err := fmt.Errorf(
			"reading OpenID configuration from %s returned %d: %s",
			cfgEP,
			resp.StatusCode(),
			string(resp.Body()),
		)
		log.WithError(err).WithFields(params.LogTags).Errorf("GET %s unsuccessful", cfgEP)
		return nil, err
	}

	// Read the provider's signing public key
	type jwksResp struct {
		Keys []oidpSigningJWK `json:"keys"`
	}
	var signingKeys jwksResp
	resp, err = httpClient.R().SetResult(&signingKeys).Get(cfg.JwksURI)
	if err != nil {
		log.WithError(err).WithFields(params.LogTags).Errorf("GET %s unsuccessful", cfg.JwksURI)
		return nil, err
	}
	if !resp.IsSuccess() {
		err := fmt.Errorf(
			"reading JWKS from %s returned %d: %s", cfg.JwksURI, resp.StatusCode(), string(resp.Body()),
		)
		log.WithError(err).WithFields(params.LogTags).Errorf("GET %s unsuccessful", cfg.JwksURI)
		return nil, err
	}

	// Perform post processing on the keys
	keyMaterial := make(map[string]interface{})
	for _, key := range signingKeys.Keys {
		n := new(big.Int)
		var pubKey interface{}

		nBytes, _ := base64.RawURLEncoding.DecodeString(key.Modulus)
		n.SetBytes(nBytes)

		eBytes, _ := base64.RawURLEncoding.DecodeString(key.Exponent)
		e := int(new(big.Int).SetBytes(eBytes).Int64())

		switch key.Type {
		case "RSA":
			pubKey = &rsa.PublicKey{N: n, E: e}
		default:
			pubKey = nil
		}

		keyMaterial[key.ID] = pubKey
	}

	{
		t, _ := json.MarshalIndent(&cfg, "", "  ")
		log.WithFields(params.LogTags).Debugf("OpenID provider parameters\n%s", t)
	}

	if params.RequestHostOverride != nil {
		log.WithFields(params.LogTags).Warnf(
			"Using host override '%s' when communicating with IDP", *params.RequestHostOverride,
		)
	}

	return &oidpClientImpl{
		Component: Component{
			LogTags: params.LogTags,
			LogTagModifiers: []LogMetadataModifier{
				ModifyLogMetadataByRestRequestParam,
			},
		},
		cfg:             cfg,
		hostOverride:    params.RequestHostOverride,
		httpClient:      httpClient,
		publicKey:       keyMaterial,
		clientID:        params.ClientID,
		clientSecret:    params.ClientCred,
		targetAudiences: params.TargetAudiences,
	}, nil
}

/*
AssociatedPublicKey fetches the associated public based on "kid" value of a JWT token

	@param token *jwt.Token - the JWT token to find the public key for
	@return public key material
*/
func (c *oidpClientImpl) AssociatedPublicKey(token *jwt.Token) (interface{}, error) {
	kidRaw, ok := token.Header["kid"]
	if !ok {
		return nil, fmt.Errorf("jwt missing 'kid' field")
	}
	kid, ok := kidRaw.(string)
	if !ok {
		return nil, fmt.Errorf("jwt 'kid' field does not contain a string")
	}
	if pubKey, ok := c.publicKey[kid]; ok {
		return pubKey, nil
	}
	msg := fmt.Sprintf("Encountered JWT referring public key %s which is unknown", kid)
	log.WithFields(c.LogTags).Error(msg)
	return nil, fmt.Errorf("%s", msg)
}

/*
ParseJWT parses a string into a JWT token object.

	@param raw string - the original JWT string
	@param claimStore jwt.Claims - the object to store the claims in
	@return the parsed JWT token object
*/
func (c *oidpClientImpl) ParseJWT(raw string, claimStore jwt.Claims) (*jwt.Token, error) {
	if len(c.targetAudiences) > 0 {
		return jwt.ParseWithClaims(
			raw,
			claimStore,
			c.AssociatedPublicKey,
			jwt.WithAudience(c.targetAudiences...),
			jwt.WithIssuer(c.cfg.Issuer),
			jwt.WithValidMethods(c.cfg.TokenSigningMethods),
		)
	}
	return jwt.ParseWithClaims(
		raw,
		claimStore,
		c.AssociatedPublicKey,
		jwt.WithIssuer(c.cfg.Issuer),
		jwt.WithValidMethods(c.cfg.TokenSigningMethods),
	)
}

// Potential response from introspection
type introspectResponse struct {
	ID                string           `json:"jti"`
	Expire            int64            `json:"exp"`
	NotValidBefore    int64            `json:"nbf"`
	IssuedAt          int64            `json:"iat"`
	Issuer            string           `json:"iss"`
	Audience          jwt.ClaimStrings `json:"aud"`
	TokenType         string           `json:"typ"`
	AuthorizedParty   string           `json:"azp"`
	AuthenticateTime  int64            `json:"auth_time"`
	SessionState      string           `json:"session_state"`
	PerferredUsername string           `json:"preferred_username"`
	Email             string           `json:"email"`
	VerifiedEmail     bool             `json:"email_verified"`
	AuthnContextClass string           `json:"acr"`
	AllowedOrigins    []string         `json:"allowed-origins"`
	Scope             string           `json:"scope"`
	ClientID          string           `json:"client_id"`
	Username          string           `json:"username"`
	Active            bool             `json:"active"`
}

/*
CanIntrospect whether the client can perform introspection

	@return whether the client can perform introspection
*/
func (c *oidpClientImpl) CanIntrospect() bool {
	if c.clientID == nil || c.clientSecret == nil || c.cfg.IntrospectionEP == "" {
		// Introspection require
		// * Introspection endpoint
		// * Client ID
		// * Client secret
		return false
	}
	return true
}

/*
IntrospectToken perform introspection for a token

	@param ctxt context.Context - the operating context
	@param token string - the token to introspect
	@return whether token is still valid
*/
func (c *oidpClientImpl) IntrospectToken(ctxt context.Context, token string) (bool, error) {
	logtags := c.GetLogTagsForContext(ctxt)
	if c.clientID == nil || c.clientSecret == nil || c.cfg.IntrospectionEP == "" {
		// Introspection require
		// * Introspection endpoint
		// * Client ID
		// * Client secret
		log.WithFields(logtags).Error("Missing required settings to perform introspection")
		return false, fmt.Errorf("missing required settings to perform introspection")
	}

	var response introspectResponse
	introspectURL := c.cfg.IntrospectionEP

	// Prepare the request
	requestBody := []byte(fmt.Sprintf("token=%s", token))
	req, err := http.NewRequest("POST", introspectURL, bytes.NewBuffer(requestBody))
	if err != nil {
		log.WithError(err).WithFields(logtags).Error("Failed to define introspect POST request")
		return false, err
	}
	req.SetBasicAuth(*c.clientID, *c.clientSecret)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if c.hostOverride != nil {
		req.Host = *c.hostOverride
	}

	// Prepare the request
	request := c.httpClient.
		R().
		SetHeaders(map[string]string{
			"Accept":       "*/*",
			"Content-Type": "application/x-www-form-urlencoded",
		}).
		SetBasicAuth(*c.clientID, *c.clientSecret).
		SetResult(&response).
		SetFormData(map[string]string{"token": token})
	if c.hostOverride != nil {
		request = request.SetHeader("Host", *c.hostOverride)
	}

	// Perform the request
	resp, err := request.Post(introspectURL)
	if err != nil {
		log.WithError(err).WithFields(logtags).Errorf("Introspect against %s failed", introspectURL)
		return false, err
	}
	if !resp.IsSuccess() {
		err := fmt.Errorf(
			"introspection returned status code %d '%s'", resp.StatusCode(), string(resp.Body()),
		)
		log.WithError(err).WithFields(logtags).Errorf("Introspect against %s failed", introspectURL)
		return false, err
	}

	// Parse the response
	log.WithFields(logtags).Debugf("Raw introspect response %s", string(resp.Body()))

	return response.Active, nil
}

// bearerTokenExtractor helper function to extract a token from a Authorization header
func bearerTokenExtractor(header string) (string, error) {
	// Check if header is too short
	if len(header) < 6 {
		return "", errors.New("header must be at least 6 characters long")
	}

	// Convert to lowercase for case-insensitive match
	s := strings.ToLower(header)

	// Find "bearer" substring
	pos := strings.Index(s, "bearer")
	if pos == -1 {
		return "", errors.New("header does not contain 'bearer'")
	}

	// Calculate token start index (after "bearer")
	tokenStart := pos + 6
	if tokenStart >= len(header) {
		return "", errors.New("no token found after 'bearer'")
	}

	// Extract token and remove leading whitespace
	token := header[tokenStart:]
	token = strings.TrimLeft(token, " \t\n\r\f\v")

	// Validate token
	if token == "" {
		return "", errors.New("token is empty after removing leading whitespace")
	}

	return token, nil
}

// RestRequestOAuthTokenKey associated key for *jwt.Token when storing in request context
type RestRequestOAuthTokenKey struct{}

// GetJWTTokenFromContext parse out the JWT token recorded in the request context
func GetJWTTokenFromContext(ctx context.Context) (*jwt.Token, error) {
	if ctx.Value(RestRequestOAuthTokenKey{}) != nil {
		v, ok := ctx.Value(RestRequestOAuthTokenKey{}).(*jwt.Token)
		if ok {
			return v, nil
		}
	}
	return nil, nil
}

// JWTCheckMiddleware middleware for validating Oauth JWT tokens
type JWTCheckMiddleware struct {
	Component
	core OpenIDProviderClient
}

/*
DefineJWTCheckMiddleware define a new Oauth JWT validation middleware

	@param providerClient OpenIDProviderClient - core client
	@param logTags log.Fields - metadata fields to include in the logs
	@returns new middleware
*/
func DefineJWTCheckMiddleware(
	providerClient OpenIDProviderClient, logTags log.Fields,
) JWTCheckMiddleware {
	return JWTCheckMiddleware{
		Component: Component{
			LogTags: logTags,
			LogTagModifiers: []LogMetadataModifier{
				ModifyLogMetadataByRestRequestParam,
			},
		},
		core: providerClient,
	}
}

/*
ParseAndValidateJWT is a support middleware to be used with Mux to parse and
validate OAuth bearer token in the request.

	@param next http.HandlerFunc - the core request handler function
	@return middleware http.HandlerFunc
*/
func (m JWTCheckMiddleware) ParseAndValidateJWT(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logTags := m.GetLogTagsForContext(r.Context())

		authTokenHeader := r.Header.Get("Authorization")
		if authTokenHeader == "" {
			// Missing bearer token
			resp := getStdRESTErrorMsg(
				r.Context(), http.StatusUnauthorized, "Header 'Authorization' missing", "",
			)
			err := writeRESTResponse(w, http.StatusUnauthorized, resp, map[string]string{})
			if err != nil {
				log.WithError(err).WithFields(logTags).Error("Failed to form response")
			}
			return
		}
		bearerToken, err := bearerTokenExtractor(authTokenHeader)
		if err != nil {
			// Unable to extract the token
			resp := getStdRESTErrorMsg(
				r.Context(), http.StatusUnauthorized, "Malformed 'Authorization' header", err.Error(),
			)
			err := writeRESTResponse(w, http.StatusUnauthorized, resp, map[string]string{})
			if err != nil {
				log.WithError(err).WithFields(logTags).Error("Failed to form response")
			}
			return
		}

		parsedClaims := new(jwt.MapClaims)
		parsedToken, err := m.core.ParseJWT(bearerToken, parsedClaims)
		if err != nil {
			// Invalid token
			resp := getStdRESTErrorMsg(
				r.Context(), http.StatusUnauthorized, "Invalid token", err.Error(),
			)
			err := writeRESTResponse(w, http.StatusUnauthorized, resp, map[string]string{})
			if err != nil {
				log.WithError(err).WithFields(logTags).Error("Failed to form response")
			}
			return
		}

		expireAt, err := parsedToken.Claims.GetExpirationTime()
		if err != nil {
			// Invalid expiration
			resp := getStdRESTErrorMsg(
				r.Context(), http.StatusUnauthorized, "Invalid token", err.Error(),
			)
			err := writeRESTResponse(w, http.StatusUnauthorized, resp, map[string]string{})
			if err != nil {
				log.WithError(err).WithFields(logTags).Error("Failed to form response")
			}
			return
		}

		if expireAt.UTC().Before(time.Now().UTC()) {
			// Token expired
			resp := getStdRESTErrorMsg(
				r.Context(), http.StatusUnauthorized, "Expired token", "",
			)
			err := writeRESTResponse(w, http.StatusUnauthorized, resp, map[string]string{})
			if err != nil {
				log.WithError(err).WithFields(logTags).Error("Failed to form response")
			}
			return
		}

		// Valid token, proceed
		newCtx := context.WithValue(r.Context(), RestRequestOAuthTokenKey{}, parsedToken)
		next(w, r.WithContext(newCtx))
	}
}
