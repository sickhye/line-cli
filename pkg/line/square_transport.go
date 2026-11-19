package line

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	thrift "github.com/apache/thrift/lib/go/thrift"
)

// SquareClient is the isolated transport used for LINE Square APIs. It wraps
// an existing Talk Client so HTTP policy, credentials, and session lifecycle
// remain owned by the normal LINE session.
type SquareClient struct {
	client *Client
}

// SquareReadOperation identifies one explicitly registered Square read.
// Operations outside the fixed read allowlist are rejected.
type SquareReadOperation string

const (
	SquareReadGetJoinedSquares     SquareReadOperation = "getJoinedSquares"
	SquareReadGetJoinedSquareChats SquareReadOperation = "getJoinedSquareChats"
	SquareReadFetchChatEvents      SquareReadOperation = "fetchSquareChatEvents"
	squareAPIHost                                      = "legy.line-apps.com"
	squareAPIPath                                      = "/SQ1"
)

var (
	ErrSquareOperationNotAllowed = errors.New("square read operation is not allowlisted")
	ErrSquareServiceException    = errors.New("LINE Square service exception")
)

type squareOperationSpec struct {
	host              string
	path              string
	method            string
	validateArguments func([]SquareField) error
}

// The registered methods are read-only calls whose schemas appear in the
// public softnetics/line-rs IDL. The live Chrome Extension contract is unverified.
var allowlistedSquareReadOperations = map[SquareReadOperation]squareOperationSpec{
	SquareReadGetJoinedSquares: {
		host:              squareAPIHost,
		path:              squareAPIPath,
		method:            string(SquareReadGetJoinedSquares),
		validateArguments: validateSquarePageArguments,
	},
	SquareReadGetJoinedSquareChats: {
		host:              squareAPIHost,
		path:              squareAPIPath,
		method:            string(SquareReadGetJoinedSquareChats),
		validateArguments: validateSquarePageArguments,
	},
	SquareReadFetchChatEvents: {
		host:              squareAPIHost,
		path:              squareAPIPath,
		method:            string(SquareReadFetchChatEvents),
		validateArguments: validateSquareChatEventsArguments,
	},
}

// SquareField represents a field in a compact-Thrift struct.
type SquareField struct {
	ID    int16
	Value any
}

// SquareStruct represents a nested compact-Thrift struct.
type SquareStruct struct {
	Fields []SquareField
}

// SquareList and SquareSet preserve collection element types from Thrift.
type SquareList struct {
	ElementType thrift.TType
	Values      []any
}

type SquareSet struct {
	ElementType thrift.TType
	Values      []any
}

type SquareMapEntry struct {
	Key   any
	Value any
}

// SquareMap preserves Thrift map types and entries without inferring Go map
// key types from incomplete protocol information.
type SquareMap struct {
	KeyType   thrift.TType
	ValueType thrift.TType
	Entries   []SquareMapEntry
}

const maxSquareCollectionEntries = 1 << 20

// SquareReadRequest is the wire envelope used by the Compact Protocol encoder.
// Network calls construct it only from a registered Square operation.
type SquareReadRequest struct {
	Method     string
	SequenceID int32
	Arguments  []SquareField
}

// SquareReadResponse preserves the Thrift method, sequence, and opaque result
// fields without assuming a service-exception field layout.
type SquareReadResponse struct {
	Method     string
	SequenceID int32
	Fields     []SquareField
}

// NewSquareClient creates an isolated Square transport. It never loads or
// persists access tokens; the parent Client is the existing session lifecycle.
func NewSquareClient(client *Client) *SquareClient {
	return &SquareClient{client: client}
}

// SquareClient provides the isolated transport for the existing session API.
// It exists as an optional bridge; no independent credential source is used.
func (c *Client) SquareClient() *SquareClient {
	return NewSquareClient(c)
}

// EncodeSquareRequest encodes a Square request call using Compact Protocol.
func EncodeSquareRequest(request SquareReadRequest) ([]byte, error) {
	if request.Method == "" {
		return nil, errors.New("square method must not be empty")
	}
	buffer := thrift.NewTMemoryBuffer()
	protocol := thrift.NewTCompactProtocolConf(buffer, nil)
	ctx := context.Background()
	if err := protocol.WriteMessageBegin(ctx, request.Method, thrift.CALL, request.SequenceID); err != nil {
		return nil, fmt.Errorf("write Square message: %w", err)
	}
	if err := protocol.WriteStructBegin(ctx, request.Method+"_args"); err != nil {
		return nil, fmt.Errorf("write Square arguments: %w", err)
	}
	for _, field := range request.Arguments {
		if err := writeSquareField(ctx, protocol, field); err != nil {
			return nil, err
		}
	}
	if err := protocol.WriteFieldStop(ctx); err != nil {
		return nil, fmt.Errorf("write Square arguments end: %w", err)
	}
	if err := protocol.WriteStructEnd(ctx); err != nil {
		return nil, fmt.Errorf("write Square arguments struct end: %w", err)
	}
	if err := protocol.WriteMessageEnd(ctx); err != nil {
		return nil, fmt.Errorf("finish Square request: %w", err)
	}
	encoded := append([]byte(nil), buffer.Bytes()...)
	if len(request.Arguments) > 0 {
		// line-rs mirrors linejs and emits a second trailing STOP after
		// non-empty argument structs.
		encoded = append(encoded, 0)
	}
	return encoded, nil
}

func writeSquareField(ctx context.Context, protocol thrift.TProtocol, field SquareField) error {
	return writeSquareFieldDepth(ctx, protocol, field, 0)
}

func writeSquareFieldDepth(ctx context.Context, protocol thrift.TProtocol, field SquareField, depth int) error {
	if depth > thrift.DEFAULT_RECURSION_DEPTH {
		return errors.New("square Thrift nesting exceeds limit")
	}
	typeID, err := squareFieldType(field.Value)
	if err != nil {
		return fmt.Errorf("square field %d: %w", field.ID, err)
	}
	if err := protocol.WriteFieldBegin(ctx, "", typeID, field.ID); err != nil {
		return fmt.Errorf("write Square field %d: %w", field.ID, err)
	}
	switch value := field.Value.(type) {
	case bool:
		err = protocol.WriteBool(ctx, value)
	case int8:
		err = protocol.WriteByte(ctx, value)
	case int16:
		err = protocol.WriteI16(ctx, value)
	case int32:
		err = protocol.WriteI32(ctx, value)
	case int:
		err = protocol.WriteI32(ctx, int32(value))
	case int64:
		err = protocol.WriteI64(ctx, value)
	case float64:
		err = protocol.WriteDouble(ctx, value)
	case string:
		err = protocol.WriteString(ctx, value)
	case []byte:
		err = protocol.WriteBinary(ctx, value)
	case SquareStruct:
		err = writeSquareStructDepth(ctx, protocol, value, depth+1)
	case *SquareStruct:
		if value == nil {
			err = errors.New("nil Square struct")
		} else {
			err = writeSquareStructDepth(ctx, protocol, *value, depth+1)
		}
	default:
		err = fmt.Errorf("unsupported Thrift value type %T", value)
	}
	if err != nil {
		return fmt.Errorf("write Square field %d value: %w", field.ID, err)
	}
	return protocol.WriteFieldEnd(ctx)
}

func writeSquareStructDepth(ctx context.Context, protocol thrift.TProtocol, value SquareStruct, depth int) error {
	if depth > thrift.DEFAULT_RECURSION_DEPTH {
		return errors.New("square Thrift nesting exceeds limit")
	}
	if err := protocol.WriteStructBegin(ctx, ""); err != nil {
		return err
	}
	for _, field := range value.Fields {
		if err := writeSquareFieldDepth(ctx, protocol, field, depth); err != nil {
			return err
		}
	}
	if err := protocol.WriteFieldStop(ctx); err != nil {
		return err
	}
	return protocol.WriteStructEnd(ctx)
}

func squareFieldType(value any) (thrift.TType, error) {
	switch value.(type) {
	case bool:
		return thrift.BOOL, nil
	case int8:
		return thrift.BYTE, nil
	case int16:
		return thrift.I16, nil
	case int32, int:
		return thrift.I32, nil
	case int64:
		return thrift.I64, nil
	case float64:
		return thrift.DOUBLE, nil
	case string:
		return thrift.STRING, nil
	case []byte:
		return thrift.STRING, nil
	case SquareStruct, *SquareStruct:
		return thrift.STRUCT, nil
	default:
		return thrift.STOP, fmt.Errorf("unsupported Thrift value type %T", value)
	}
}

// DecodeSquareResponse decodes a Compact-Thrift reply or Thrift application
// exception without interpreting any result fields as a login or mutation.
func DecodeSquareResponse(ctx context.Context, data []byte) (*SquareReadResponse, error) {
	if len(data) == 0 {
		return nil, errors.New("empty Square Thrift response")
	}
	buffer := thrift.NewTMemoryBuffer()
	buffer.Write(data)
	protocol := thrift.NewTCompactProtocolConf(buffer, nil)
	name, messageType, sequenceID, err := protocol.ReadMessageBegin(ctx)
	if err != nil {
		return nil, fmt.Errorf("decode Square message: %w", err)
	}
	response := &SquareReadResponse{Method: name, SequenceID: sequenceID}
	switch messageType {
	case thrift.EXCEPTION:
		exception := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, "")
		if err := exception.Read(ctx, protocol); err != nil {
			return nil, fmt.Errorf("decode Square application exception: %w", err)
		}
		if err := protocol.ReadMessageEnd(ctx); err != nil {
			return nil, fmt.Errorf("finish Square application exception: %w", err)
		}
		return nil, &SquareApplicationError{ApplicationException: exception}
	case thrift.REPLY:
		if _, err := protocol.ReadStructBegin(ctx); err != nil {
			return nil, fmt.Errorf("decode Square reply struct: %w", err)
		}
		for {
			_, typeID, fieldID, err := protocol.ReadFieldBegin(ctx)
			if err != nil {
				return nil, fmt.Errorf("decode Square reply field: %w", err)
			}
			if typeID == thrift.STOP {
				break
			}
			field, err := readSquareFieldDepth(ctx, protocol, fieldID, typeID, 0)
			if err != nil {
				return nil, fmt.Errorf("decode Square reply field %d: %w", fieldID, err)
			}
			response.Fields = append(response.Fields, field)
			if err := protocol.ReadFieldEnd(ctx); err != nil {
				return nil, fmt.Errorf("finish Square reply field %d: %w", fieldID, err)
			}
		}
		if err := protocol.ReadStructEnd(ctx); err != nil {
			return nil, fmt.Errorf("finish Square reply struct: %w", err)
		}
		if err := protocol.ReadMessageEnd(ctx); err != nil {
			return nil, fmt.Errorf("finish Square reply: %w", err)
		}
		return response, nil
	default:
		return nil, fmt.Errorf("unexpected Square Thrift message type %d", messageType)
	}
}

func readSquareFieldDepth(ctx context.Context, protocol thrift.TProtocol, fieldID int16, typeID thrift.TType, depth int) (SquareField, error) {
	value, err := readSquareValue(ctx, protocol, typeID, depth)
	return SquareField{ID: fieldID, Value: value}, err
}

func readSquareValue(ctx context.Context, protocol thrift.TProtocol, typeID thrift.TType, depth int) (any, error) {
	if depth > thrift.DEFAULT_RECURSION_DEPTH {
		return nil, errors.New("square Thrift nesting exceeds limit")
	}
	switch typeID {
	case thrift.BOOL:
		return protocol.ReadBool(ctx)
	case thrift.BYTE:
		return protocol.ReadByte(ctx)
	case thrift.I16:
		return protocol.ReadI16(ctx)
	case thrift.I32:
		return protocol.ReadI32(ctx)
	case thrift.I64:
		return protocol.ReadI64(ctx)
	case thrift.DOUBLE:
		return protocol.ReadDouble(ctx)
	case thrift.STRING:
		return protocol.ReadString(ctx)
	case thrift.STRUCT:
		return readSquareStructDepth(ctx, protocol, depth+1)
	case thrift.LIST:
		elementType, size, err := protocol.ReadListBegin(ctx)
		if err != nil {
			return nil, err
		}
		if size > maxSquareCollectionEntries {
			return nil, errors.New("square Thrift collection exceeds size limit")
		}
		if depth >= thrift.DEFAULT_RECURSION_DEPTH {
			return nil, errors.New("square Thrift nesting exceeds limit")
		}
		value := SquareList{ElementType: elementType, Values: make([]any, size)}
		for i := range value.Values {
			value.Values[i], err = readSquareValue(ctx, protocol, elementType, depth+1)
			if err != nil {
				return nil, err
			}
		}
		if err := protocol.ReadListEnd(ctx); err != nil {
			return nil, err
		}
		return value, nil
	case thrift.SET:
		elementType, size, err := protocol.ReadSetBegin(ctx)
		if err != nil {
			return nil, err
		}
		if size > maxSquareCollectionEntries {
			return nil, errors.New("square Thrift collection exceeds size limit")
		}
		if depth >= thrift.DEFAULT_RECURSION_DEPTH {
			return nil, errors.New("square Thrift nesting exceeds limit")
		}
		value := SquareSet{ElementType: elementType, Values: make([]any, size)}
		for i := range value.Values {
			value.Values[i], err = readSquareValue(ctx, protocol, elementType, depth+1)
			if err != nil {
				return nil, err
			}
		}
		if err := protocol.ReadSetEnd(ctx); err != nil {
			return nil, err
		}
		return value, nil
	case thrift.MAP:
		keyType, valueType, size, err := protocol.ReadMapBegin(ctx)
		if err != nil {
			return nil, err
		}
		if size > maxSquareCollectionEntries {
			return nil, errors.New("square Thrift collection exceeds size limit")
		}
		if depth >= thrift.DEFAULT_RECURSION_DEPTH {
			return nil, errors.New("square Thrift nesting exceeds limit")
		}
		value := SquareMap{KeyType: keyType, ValueType: valueType, Entries: make([]SquareMapEntry, size)}
		for i := range value.Entries {
			value.Entries[i].Key, err = readSquareValue(ctx, protocol, keyType, depth+1)
			if err != nil {
				return nil, err
			}
			value.Entries[i].Value, err = readSquareValue(ctx, protocol, valueType, depth+1)
			if err != nil {
				return nil, err
			}
		}
		if err := protocol.ReadMapEnd(ctx); err != nil {
			return nil, err
		}
		return value, nil
	case thrift.UUID:
		return protocol.ReadUUID(ctx)
	default:
		if err := thrift.SkipDefaultDepth(ctx, protocol, typeID); err != nil {
			return nil, err
		}
		return nil, nil
	}
}

func readSquareStructDepth(ctx context.Context, protocol thrift.TProtocol, depth int) (SquareStruct, error) {
	if depth > thrift.DEFAULT_RECURSION_DEPTH {
		return SquareStruct{}, errors.New("square Thrift nesting exceeds limit")
	}
	value := SquareStruct{}
	if _, err := protocol.ReadStructBegin(ctx); err != nil {
		return value, err
	}
	for {
		_, typeID, fieldID, err := protocol.ReadFieldBegin(ctx)
		if err != nil {
			return value, err
		}
		if typeID == thrift.STOP {
			break
		}
		field, err := readSquareFieldDepth(ctx, protocol, fieldID, typeID, depth)
		if err != nil {
			return value, err
		}
		value.Fields = append(value.Fields, field)
		if err := protocol.ReadFieldEnd(ctx); err != nil {
			return value, err
		}
	}
	if err := protocol.ReadStructEnd(ctx); err != nil {
		return value, err
	}
	return value, nil
}

// SquareApplicationError translates Thrift application exceptions into a typed
// error compatible with the existing session error wrappers.
type SquareApplicationError struct {
	ApplicationException thrift.TApplicationException
}

func (e *SquareApplicationError) Error() string {
	if e.ApplicationException == nil {
		return "LINE Square Thrift application exception"
	}
	return fmt.Sprintf("LINE Square Thrift application exception (type %d)", e.ApplicationException.TypeId())
}

func (e *SquareApplicationError) Unwrap() error {
	if e.ApplicationException == nil {
		return nil
	}
	return e.ApplicationException
}

// CallRead performs one registered read RPC. Callers cannot select a URL or
// method; both are resolved from the verified operation registry.
func (c *SquareClient) CallRead(ctx context.Context, operation SquareReadOperation, arguments []SquareField) (*SquareReadResponse, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("square transport has no LINE client")
	}
	spec, ok := allowlistedSquareReadOperations[operation]
	if !ok {
		return nil, ErrSquareOperationNotAllowed
	}
	return c.callReadWithSpec(ctx, spec, arguments)
}

func (c *SquareClient) callReadWithSpec(ctx context.Context, spec squareOperationSpec, arguments []SquareField) (*SquareReadResponse, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("square transport has no LINE client")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if spec.method == "" || spec.validateArguments == nil {
		return nil, ErrSquareOperationNotAllowed
	}
	if err := spec.validateArguments(arguments); err != nil {
		return nil, fmt.Errorf("invalid Square read arguments: %w", err)
	}
	fullURL, err := squareEndpoint(spec.host, spec.path)
	if err != nil {
		return nil, fmt.Errorf("invalid Square allowlist entry: %w", err)
	}
	request := SquareReadRequest{Method: spec.method, Arguments: arguments}
	body, err := EncodeSquareRequest(request)
	if err != nil {
		return nil, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create Square request: %w", err)
	}
	httpRequest.Header.Set("Accept", "application/x-thrift")
	httpRequest.Header.Set("Content-Type", "application/x-thrift")
	httpRequest.Header.Set("User-Agent", UserAgent)
	httpRequest.Header.Set("x-line-application", lineApplicationHeader)
	httpRequest.Header.Set("x-lal", "en_US")
	httpRequest.Header.Set("x-lpv", "1")
	httpRequest.Header.Set("x-lhm", http.MethodPost)
	if c.client.AccessToken != "" {
		httpRequest.Header.Set("x-line-access", c.client.AccessToken)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	response, err := doLINERequest(c.client.HTTPClient, httpRequest)
	if err != nil {
		return nil, requestFailure("transport", err)
	}
	c.client.recordClock(response.Header)
	responseBody, err := readResponseBody(response, maxRPCResponseBytes)
	if err != nil {
		return nil, requestFailure("response_read", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, &ResponseError{Status: response.StatusCode, body: string(responseBody)}
	}
	decoded, err := DecodeSquareResponse(ctx, responseBody)
	if err != nil {
		return nil, err
	}
	if decoded.Method != request.Method || decoded.SequenceID != request.SequenceID {
		return nil, errors.New("square Thrift response does not match request")
	}
	return decoded, nil
}

func squareEndpoint(host, path string) (string, error) {
	if host == "" || path == "" || !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") ||
		strings.ContainsAny(path, "?#") || strings.Contains(path, "\\") {
		return "", errors.New("host and exact absolute path are required")
	}
	parsed, err := url.Parse("https://" + host + path)
	if err != nil || parsed.Scheme != "https" || parsed.Host != host || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != path {
		return "", errors.New("host/path is not a valid exact HTTPS endpoint")
	}
	return parsed.String(), nil
}
