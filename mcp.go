package goutils

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/apex/log"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// MCPHandler MCP request handler
type MCPHandler struct {
	Component

	// LogLevel configure the request logging level
	LogLevel HTTPRequestLogLevel

	// EnumTypeSchemas the per-Go-type enumerated JSON schemas handed to jsonschema-go so ENUM
	// fields in a tool's input emit a proper enumeration rather than a bare string. Must be
	// allocated before use (e.g. by the struct embedding this handler); populate it via
	// MCPInstallEnumSchema and treat it as read-only thereafter.
	EnumTypeSchemas map[reflect.Type]*jsonschema.Schema
}

// ======================================================================================
// ENUM schema support

// mcpBuildEnumSchema build a string JSON schema whose enum is populated from the
// given ENUM values.
func mcpBuildEnumSchema[T ~string](values []T) *jsonschema.Schema {
	enum := make([]any, len(values))
	for i, v := range values {
		enum[i] = string(v)
	}
	return &jsonschema.Schema{Type: "string", Enum: enum}
}

// MCPInstallEnumSchema register the enumerated JSON schema for ENUM type T against the handler's
// per-type schema table, so any tool input carrying a field of type T advertises T's permitted
// members rather than a bare string. The member list is taken from T's Values() method, keeping
// the registration in lock-step with the const block that defines the type.
//
// This is a generic function rather than a method because Go methods cannot introduce their own
// type parameters.
func MCPInstallEnumSchema[T Enum[T]](h *MCPHandler) {
	var zero T
	h.EnumTypeSchemas[reflect.TypeFor[T]()] = mcpBuildEnumSchema(zero.Values())
}

// mcpInputSchemaFor build the input JSON schema for tool parameter type In, resolving any ENUM
// fields to their enumerated schemas via the handler's registered per-type schema table. The
// result is assigned to Tool.InputSchema so the SDK uses it verbatim instead of inferring a
// schema that would omit the ENUM values.
func mcpInputSchemaFor[In any](h *MCPHandler) (*jsonschema.Schema, error) {
	schema, err := jsonschema.For[In](&jsonschema.ForOptions{TypeSchemas: h.EnumTypeSchemas})
	if err != nil {
		return nil, fmt.Errorf(
			"failed to infer input schema for %s: %w", reflect.TypeFor[In]().Name(), err,
		)
	}
	mcpDenullRequired(schema)
	return schema, nil
}

// mcpDenullRequired collapse the "null" member out of the type of every REQUIRED property whose
// type was inferred as a two-member ["null", X] union, recursively through the schema tree.
//
// jsonschema-go renders a Go slice or pointer field as a nullable type union (e.g. a []T becomes
// Types: ["null", "array"]) because a nil value is representable. For a field carrying
// validate:"required" that union is misleading: the field can never legitimately be null. Beyond
// reading cleanly to an agent, a plain single type also avoids the type-array form that weaker MCP
// client schema converters mishandle or drop, which can leave the agent guessing a tool's shape.
func mcpDenullRequired(schema *jsonschema.Schema) {
	if schema == nil {
		return
	}

	required := make(map[string]struct{}, len(schema.Required))
	for _, name := range schema.Required {
		required[name] = struct{}{}
	}

	for name, prop := range schema.Properties {
		if _, isRequired := required[name]; isRequired {
			mcpDenullType(prop)
		}
	}

	// Recurse into nested schemas so required properties at any depth are covered.
	for _, prop := range schema.Properties {
		mcpDenullRequired(prop)
	}
	mcpDenullRequired(schema.Items)
}

// mcpDenullType collapse a two-member ["null", X] type union on the given schema down to the
// single non-null type X. Any other type shape is left untouched.
func mcpDenullType(schema *jsonschema.Schema) {
	if schema == nil || len(schema.Types) != 2 {
		return
	}

	var nonNull string
	sawNull := false
	for _, t := range schema.Types {
		if t == "null" {
			sawNull = true
			continue
		}
		nonNull = t
	}
	if !sawNull || nonNull == "" {
		return
	}

	schema.Types = nil
	schema.Type = nonNull
}

/*
MCPAddTool register a typed tool, building its input schema with ENUM support. It is a thin
wrapper over mcp.AddTool that pre-populates Tool.InputSchema (see mcpInputSchemaFor); passing a
Tool with a nil InputSchema to mcp.AddTool would infer a schema without ENUM enumerations.

This is a generic function rather than a method because Go methods cannot introduce their own
type parameters.

	@param h *MCPHandler - the handler whose registered ENUM schemas resolve the tool's input schema
	@param server *mcp.Server - target MCP server to register the tool against
	@param tool *mcp.Tool - the tool definition; its InputSchema is populated in place
	@param handler mcp.ToolHandlerFor[In, Out] - the tool call handler for input type In and
	    output type Out
	@returns error if the input schema for In could not be built
*/
func MCPAddTool[In, Out any](
	h *MCPHandler, server *mcp.Server, tool *mcp.Tool, handler mcp.ToolHandlerFor[In, Out],
) error {
	schema, err := mcpInputSchemaFor[In](h)
	if err != nil {
		return err
	}
	tool.InputSchema = schema
	mcp.AddTool(server, tool, handler)
	return nil
}

// MCPTextResult build a successful tool result carrying a single plain-text content block. Used
// by the action tools, whose meaningful result is a short confirmation string.
func MCPTextResult(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: text}},
	}
}

// ======================================================================================
// Logging Support

// MCPRequestParamKey associated key for MCPRequestParam when storing in request context
type MCPRequestParamKey struct{}

// MCPRequestParam a helper object for logging a MCP request's parameters into its context
type MCPRequestParam struct {
	// ID is the request session ID
	ID string `json:"id"`
	// IsToolCall whether the request is tool call
	IsToolCall bool `json:"is_tool_call"`
	// Method is the request method
	Method string `json:"method"`
	// ToolName tool being called
	ToolName string `json:"tool_name,omitempty"`
	// ToolArgs tool call arguments
	ToolArgs json.RawMessage `json:"tool_args,omitempty"`
	// Timestamp is when the request is first received
	Timestamp time.Time
}

// updateLogTags updates Apex log.Fields map with values the requests's parameters
func (i *MCPRequestParam) updateLogTags(tags log.Fields) {
	tags["mcp_request_session_id"] = i.ID
	tags["mcp_request_is_tool_call"] = i.IsToolCall
	tags["mcp_request_method"] = i.Method
	tags["mcp_request_timestamp"] = i.Timestamp.UTC().Format(time.RFC3339Nano)
	if i.IsToolCall {
		tags["mcp_request_tool"] = i.ToolName
		tags["mcp_request_tool_args"] = string(i.ToolArgs)
	}
}

// ModifyLogMetadataByMCPRequestParam update log metadata with info from MCPRequestParam
func ModifyLogMetadataByMCPRequestParam(ctx context.Context, theTags log.Fields) {
	if ctx.Value(MCPRequestParamKey{}) != nil {
		v, ok := ctx.Value(MCPRequestParamKey{}).(MCPRequestParam)
		if ok {
			v.updateLogTags(theTags)
		}
	}
}

// LoggingMiddleware support middleware to log MCP requests
func (h MCPHandler) LoggingMiddleware(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (result mcp.Result, err error) {
		// Construct the request param tracking structure
		requestParams := MCPRequestParam{
			ID:         req.GetSession().ID(),
			IsToolCall: false,
			Method:     method,
			Timestamp:  time.Now().UTC(),
		}
		if toolCallParam, ok := req.(*mcp.CallToolRequest); ok {
			requestParams.IsToolCall = true
			requestParams.ToolName = toolCallParam.Params.Name
			requestParams.ToolArgs = toolCallParam.Params.Arguments
		}

		// Construct new context
		workingCtx := context.WithValue(ctx, MCPRequestParamKey{}, requestParams)
		logTags := h.GetLogTagsForContext(workingCtx)

		// Continue request
		start := time.Now().UTC()
		resp, err := next(workingCtx, method, req)
		duration := time.Since(start)
		respTimestamp := time.Now().UTC()

		// Build string presentation of the request
		mcpRequestStr := ""
		{
			builder := strings.Builder{}
			_, _ = builder.WriteString("\nMCP Method: " + method + "\n")
			if requestParams.IsToolCall {
				_, _ = builder.WriteString("Tool: " + requestParams.ToolName + "\n")
				args := map[string]interface{}{}
				_ = json.Unmarshal(requestParams.ToolArgs, &args)
				_, _ = builder.WriteString("Tool Args:\n")
				argsPretty, _ := json.MarshalIndent(&args, "", "  ")
				_, _ = builder.Write(argsPretty)
				_, _ = builder.WriteString("\n")
			}
			_, _ = builder.WriteString("\n")
			mcpRequestStr = builder.String()
		}

		logHandle := log.WithFields(UpdateCodePositionInTags(logTags)).
			WithField("mcp_response_timestamp", respTimestamp.UTC().Format(time.RFC3339Nano)).
			WithField("mcp_request_duration_ms", duration.Milliseconds())
		if err != nil {
			stackTraceErr := DeepestErrorWithTrace(err)
			l := logHandle.WithError(err)
			if stackTraceErr != nil {
				l.Errorf("MCP Request failed:\n%+v\n%s", stackTraceErr, mcpRequestStr)
			} else {
				l.Errorf("MCP Request failed\n%s", mcpRequestStr)
			}
		} else {
			switch h.LogLevel {
			case HTTPLogLevelDEBUG:
				logHandle.Debugf("MCP Request success\n%s", mcpRequestStr)

			case HTTPLogLevelINFO:
				logHandle.Infof("MCP Request success\n%s", mcpRequestStr)

			default:
				logHandle.Warnf("MCP Request success\n%s", mcpRequestStr)
			}
		}

		return resp, err
	}
}
