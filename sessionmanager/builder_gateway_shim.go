package sessionmanager

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// The shim speaks MCP over stdio because that is the one transport both
// providers configure the same way. Everything it does is forwarding: it holds
// no allowlist, performs no path normalization, and cannot read a file. A
// builder that replaced this binary would gain nothing, because the manager on
// the other end of the socket makes every decision and records every access.
const (
	builderGatewayShimProtocolVersion = "2025-06-18"
	builderGatewayShimName            = "task-board-repository-evidence"
	builderGatewayShimVersion         = "1"
)

// builderGatewayToolSchemas is the tool surface the builder sees.
//
// It is exactly the gateway's four operations, and the descriptions say plainly
// that a search returns paths rather than matching text. A builder told
// otherwise would waste turns asking for content this surface will never return.
func builderGatewayToolSchemas() []map[string]any {
	return []map[string]any{
		{
			"name": "stat",
			"description": "Report whether a repository path exists, what kind of node it is, and its " +
				"size. Returns no file content.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{
						"type":        "string",
						"description": "Repository-relative path.",
					},
				},
				"required": []string{"path"},
			},
		},
		{
			"name": "read",
			"description": "Read an exact byte range of one repository file. Omit length to read from " +
				"the offset to the end, subject to a per-call size bound.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path": map[string]any{
						"type":        "string",
						"description": "Repository-relative path.",
					},
					"offset": map[string]any{
						"type":        "integer",
						"description": "Byte offset to start at. Defaults to 0.",
					},
					"length": map[string]any{
						"type":        "integer",
						"description": "Number of bytes to return. Defaults to the rest of the file.",
					},
				},
				"required": []string{"path"},
			},
		},
		{
			"name":        "list",
			"description": "Enumerate one repository directory. Entries outside the read scope are omitted.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path_spec": map[string]any{
						"type":        "string",
						"description": "Repository-relative directory.",
					},
				},
				"required": []string{"path_spec"},
			},
		},
		{
			"name": "search",
			"description": "Find repository files containing a literal string. Returns matching paths " +
				"and match counts, never the matching text; use read to retrieve content.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"path_spec": map[string]any{
						"type":        "string",
						"description": "Repository-relative directory or file to search under.",
					},
					"query": map[string]any{
						"type":        "string",
						"description": "Literal string to find.",
					},
				},
				"required": []string{"path_spec", "query"},
			},
		},
	}
}

type builderGatewayShimMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type builderGatewayShimReply struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *shimError      `json:"error,omitempty"`
}

type shimError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type builderGatewayToolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
	// _meta is the protocol-defined extension point inherited by request params.
	// It cannot change repository semantics, so accepting it preserves MCP client
	// interoperability while the operation arguments themselves stay closed.
	Meta json.RawMessage `json:"_meta,omitempty"`
}

// RunBuilderGatewayShim serves the MCP stdio loop until the provider closes the
// stream.
//
// Every tool call is forwarded with the JSON-RPC request id as the provider
// event identity. That is what binds a trace event to the provider's own record
// of the call, and it is why the manager can later detect a tool call the
// provider made that never reached the gateway: the two sides are counting the
// same identities.
func RunBuilderGatewayShim(
	client *BuilderGatewayClient,
	input io.Reader,
	output io.Writer,
) error {
	reader := bufio.NewReaderSize(input, 64*1024)
	encoder := json.NewEncoder(output)
	if err := client.Connect(); err != nil {
		return err
	}
	// EOF alone cannot distinguish a clean provider completion from a crash.  The
	// provider adapter owns that verdict; the shim merely drops its transport.
	defer func() { _ = client.Disconnect() }()
	recordUnknown := func() error { return client.ObserveUnknown() }
	for {
		line, err := readBoundedLine(reader, builderGatewayMaxRequestBytes)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if strings.TrimSpace(string(line)) == "" {
			continue
		}
		var message builderGatewayShimMessage
		if err := decodeStrictJSON(line, &message); err != nil ||
			message.JSONRPC != "2.0" || strings.TrimSpace(message.Method) == "" {
			if err := recordUnknown(); err != nil {
				return err
			}
			continue
		}
		// A notification carries no id and expects no reply. Answering one would
		// desynchronize the provider's request/response pairing.
		if len(message.ID) == 0 {
			if !builderGatewayShimAllowsNotification(message) {
				if err := recordUnknown(); err != nil {
					return err
				}
			}
			continue
		}
		if !builderGatewayShimValidID(message.ID) {
			if err := recordUnknown(); err != nil {
				return err
			}
			continue
		}
		reply := builderGatewayShimReply{JSONRPC: "2.0", ID: message.ID}
		switch message.Method {
		case "initialize":
			reply.Result = map[string]any{
				"protocolVersion": builderGatewayShimProtocolVersion,
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo": map[string]any{
					"name":    builderGatewayShimName,
					"version": builderGatewayShimVersion,
				},
			}
		case "tools/list":
			reply.Result = map[string]any{"tools": builderGatewayToolSchemas()}
		case "tools/call":
			var unknown bool
			reply.Result, reply.Error, unknown = builderGatewayShimCall(client, message)
			if unknown {
				if err := recordUnknown(); err != nil {
					return err
				}
			}
		case "ping":
			reply.Result = map[string]any{}
		default:
			// A method this shim does not model is a request whose intent nobody
			// classified. Replying "method not found" and moving on would leave the
			// manager reconciling a stream it could not fully read while believing it
			// could, so the frame is counted before the refusal is sent.
			if err := recordUnknown(); err != nil {
				return err
			}
			reply.Error = &shimError{Code: -32601, Message: "method not found"}
		}
		if err := encoder.Encode(reply); err != nil {
			return err
		}
	}
}

func decodeStrictJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func builderGatewayShimAllowsNotification(message builderGatewayShimMessage) bool {
	if message.Method != "notifications/initialized" {
		return false
	}
	trimmed := bytes.TrimSpace(message.Params)
	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) ||
		bytes.Equal(trimmed, []byte("{}"))
}

func builderGatewayShimValidID(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return false
	}
	var value any
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return false
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed) != ""
	case float64:
		return true
	default:
		return false
	}
}

// builderGatewayShimCall forwards one tools/call frame.
//
// The third result says the frame was unclassifiable. It is separate from the
// error because the two answers are for different audiences: the error tells the
// provider its call failed, and the flag tells the manager it can no longer
// claim to have read the whole tool stream. A call refused for a shape reason is
// both — the provider gets a refusal, and the run becomes unpublishable.
func builderGatewayShimCall(
	client *BuilderGatewayClient,
	message builderGatewayShimMessage,
) (any, *shimError, bool) {
	var params builderGatewayToolCallParams
	if err := decodeStrictJSON(message.Params, &params); err != nil {
		return nil, &shimError{Code: -32602, Message: "invalid tool call parameters"}, true
	}
	if len(bytes.TrimSpace(params.Meta)) > 0 {
		var metadata map[string]any
		if err := json.Unmarshal(params.Meta, &metadata); err != nil || metadata == nil {
			return nil, &shimError{Code: -32602, Message: "invalid tool call metadata"}, true
		}
	}
	// An unmodelled tool name is refused here rather than forwarded. Forwarding it
	// would spend a gateway call and a trace event on a request whose meaning
	// nobody established, and the resulting event would describe an operation the
	// builder did not ask for.
	if !builderGatewayShimServesTool(params.Name) {
		return nil, &shimError{
			Code:    -32602,
			Message: "unknown repository-evidence gateway tool",
		}, true
	}
	request, err := decodeBuilderGatewayToolArguments(params.Name, params.Arguments)
	if err != nil {
		return nil, &shimError{Code: -32602, Message: "invalid tool call parameters"}, true
	}
	request.Operation = params.Name
	// The JSON-RPC id is the provider's identity for this call. Trimming the
	// quotes keeps a string id readable without losing its uniqueness.
	request.ProviderEventID = strings.Trim(string(message.ID), `"`)
	response, err := client.Call(request)
	if err != nil {
		// A transport or gateway error is a call whose outcome this shim cannot
		// describe. The manager already counts it — the call was tallied before the
		// exchange — but it is reported as unclassifiable too, because a call with no
		// known outcome is exactly the shape that must not reconcile as complete.
		return nil, &shimError{Code: -32000, Message: err.Error()}, true
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, &shimError{Code: -32603, Message: "encoding gateway response"}, true
	}
	if !builderGatewayShimModelsOutcome(response.Outcome) {
		// A result shape this shim cannot classify means the gateway and the shim
		// disagree about the protocol. Passing it through would hand the builder a
		// payload nobody validated.
		return nil, &shimError{
			Code:    -32603,
			Message: "unclassifiable repository-evidence gateway outcome",
		}, true
	}
	// A denied or failed call is surfaced as an MCP tool error rather than as a
	// successful result carrying a refusal. The builder has to be able to tell
	// "this file is empty" from "you may not read this file", and a result-shaped
	// denial reads as the former.
	isError := response.Outcome != "granted"
	return map[string]any{
		"content": []map[string]any{
			{"type": "text", "text": string(encoded)},
		},
		"isError": isError,
	}, nil, false
}

func decodeBuilderGatewayToolArguments(
	name string,
	raw json.RawMessage,
) (BuilderGatewayRequest, error) {
	switch strings.TrimSpace(name) {
	case "stat":
		var args struct {
			Path string `json:"path"`
		}
		if err := decodeStrictJSON(raw, &args); err != nil || strings.TrimSpace(args.Path) == "" {
			return BuilderGatewayRequest{}, fmt.Errorf("invalid stat arguments")
		}
		return BuilderGatewayRequest{Path: args.Path}, nil
	case "read":
		var args struct {
			Path   string `json:"path"`
			Offset int64  `json:"offset"`
			Length int64  `json:"length"`
		}
		if err := decodeStrictJSON(raw, &args); err != nil || strings.TrimSpace(args.Path) == "" {
			return BuilderGatewayRequest{}, fmt.Errorf("invalid read arguments")
		}
		return BuilderGatewayRequest{Path: args.Path, Offset: args.Offset, Length: args.Length}, nil
	case "list":
		var args struct {
			PathSpec string `json:"path_spec"`
		}
		if err := decodeStrictJSON(raw, &args); err != nil || strings.TrimSpace(args.PathSpec) == "" {
			return BuilderGatewayRequest{}, fmt.Errorf("invalid list arguments")
		}
		return BuilderGatewayRequest{PathSpec: args.PathSpec}, nil
	case "search":
		var args struct {
			PathSpec string `json:"path_spec"`
			Query    string `json:"query"`
		}
		if err := decodeStrictJSON(raw, &args); err != nil || strings.TrimSpace(args.PathSpec) == "" {
			return BuilderGatewayRequest{}, fmt.Errorf("invalid search arguments")
		}
		return BuilderGatewayRequest{PathSpec: args.PathSpec, Query: args.Query}, nil
	default:
		return BuilderGatewayRequest{}, fmt.Errorf("unknown tool")
	}
}

// builderGatewayShimServesTool is the shim's own closed set, kept here rather
// than derived from the request so an unmodelled name cannot reach the gateway.
func builderGatewayShimServesTool(name string) bool {
	name = strings.TrimSpace(name)
	for _, tool := range builderGatewayToolNames {
		if name == tool {
			return true
		}
	}
	return false
}

func builderGatewayShimModelsOutcome(outcome string) bool {
	switch outcome {
	case "granted", "denied", "failed":
		return true
	}
	return false
}

// BuilderGatewayShimUsage is the argv a provider is configured to spawn.
func BuilderGatewayShimUsage(executable string) (string, []string) {
	return executable, []string{"session", "context-gateway-shim"}
}
