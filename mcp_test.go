package goutils

import (
	"encoding/json"
	"testing"

	"github.com/apex/log"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/stretchr/testify/assert"
)

// mustParseSchema unmarshals raw JSON into a *jsonschema.Schema, mirroring the way
// config-supplied schemas reach the runtime. It fails the test on a malformed fixture
// rather than returning an error, so the cases below assert only on ResolveToolInputSchema's
// behavior.
func mustParseSchema(t *testing.T, raw string) *jsonschema.Schema {
	var s jsonschema.Schema
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatalf("fixture schema failed to unmarshal: %v", err)
	}
	return &s
}

func TestMCPResolveToolInputSchema(t *testing.T) {
	assert := assert.New(t)
	log.SetLevel(log.DebugLevel)

	// cases exercises the well-formedness assertions plus the resolve step. valid marks the schemas
	// that should resolve without error.
	cases := []struct {
		name  string
		raw   string
		valid bool
	}{
		{
			// a proper object schema forbidding extra keys — the mandated shape
			name:  "object with additionalProperties:false",
			raw:   `{"type":"object","properties":{"x":{"type":"string"}},"additionalProperties":false}`,
			valid: true,
		},
		{
			// the "type" keyword may legally arrive as a single-element array
			name:  "type as single-element array",
			raw:   `{"type":["object"],"properties":{},"additionalProperties":false}`,
			valid: true,
		},
		{
			// a no-argument tool still emits a valid empty object schema
			name:  "empty object with additionalProperties:false",
			raw:   `{"type":"object","properties":{},"additionalProperties":false}`,
			valid: true,
		},
		{
			// missing additionalProperties would let an agent smuggle extra keys
			name:  "additionalProperties absent",
			raw:   `{"type":"object","properties":{}}`,
			valid: false,
		},
		{
			// additionalProperties:true explicitly permits extra keys — rejected
			name:  "additionalProperties true",
			raw:   `{"type":"object","properties":{},"additionalProperties":true}`,
			valid: false,
		},
		{
			// a sub-schema is not the mandated false schema — rejected
			name:  "additionalProperties as sub-schema",
			raw:   `{"type":"object","properties":{},"additionalProperties":{"type":"string"}}`,
			valid: false,
		},
		{
			// the root must be an object schema
			name:  "non-object type",
			raw:   `{"type":"string","additionalProperties":false}`,
			valid: false,
		},
		{
			// well-formed but a dangling $ref cannot resolve
			name:  "unresolvable ref",
			raw:   `{"type":"object","properties":{"x":{"$ref":"#/$defs/missing"}},"additionalProperties":false}`,
			valid: false,
		},
		{
			// well-formed but an invalid default is caught by ValidateDefaults
			name:  "invalid default value",
			raw:   `{"type":"object","properties":{"n":{"type":"integer","default":"nope"}},"additionalProperties":false}`,
			valid: false,
		},
	}

	for _, tc := range cases {
		schema := mustParseSchema(t, tc.raw)
		resolved, err := mcpResolveToolInputSchema("test-tool", schema)
		if tc.valid {
			assert.Nilf(err, "expected schema '%s' to resolve", tc.name)
			assert.NotNilf(resolved, "expected resolved schema for '%s'", tc.name)
		} else {
			assert.NotNilf(err, "expected schema '%s' to be rejected", tc.name)
			assert.Nilf(resolved, "expected no resolved schema for '%s'", tc.name)
		}
	}
}
