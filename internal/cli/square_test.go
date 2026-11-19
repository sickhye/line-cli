package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/kongesque/line-cli/internal/session"
	"github.com/kongesque/line-cli/pkg/line"
)

type squareCLIAPI struct {
	session.API
	square *line.SquareClient
}

func (a *squareCLIAPI) SquareClient() *line.SquareClient { return a.square }

type squareCLIRoundTripper func(*http.Request) (*http.Response, error)

func (f squareCLIRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func encodeEmptySquareCLIReply(t *testing.T, method string, listField int16) []byte {
	t.Helper()
	buffer := thrift.NewTMemoryBuffer()
	protocol := thrift.NewTCompactProtocolConf(buffer, nil)
	ctx := context.Background()
	for _, step := range []struct {
		name string
		call func() error
	}{
		{"message begin", func() error { return protocol.WriteMessageBegin(ctx, method, thrift.REPLY, 0) }},
		{"result begin", func() error { return protocol.WriteStructBegin(ctx, method+"_result") }},
		{"success field", func() error { return protocol.WriteFieldBegin(ctx, "", thrift.STRUCT, 0) }},
		{"success begin", func() error { return protocol.WriteStructBegin(ctx, "success") }},
		{"list field", func() error { return protocol.WriteFieldBegin(ctx, "", thrift.LIST, listField) }},
		{"list begin", func() error { return protocol.WriteListBegin(ctx, thrift.STRUCT, 0) }},
		{"list end", func() error { return protocol.WriteListEnd(ctx) }},
		{"list field end", func() error { return protocol.WriteFieldEnd(ctx) }},
		{"success stop", func() error { return protocol.WriteFieldStop(ctx) }},
		{"success end", func() error { return protocol.WriteStructEnd(ctx) }},
		{"success field end", func() error { return protocol.WriteFieldEnd(ctx) }},
		{"result stop", func() error { return protocol.WriteFieldStop(ctx) }},
		{"result end", func() error { return protocol.WriteStructEnd(ctx) }},
		{"message end", func() error { return protocol.WriteMessageEnd(ctx) }},
	} {
		if err := step.call(); err != nil {
			t.Fatalf("write fixture %s: %v", step.name, err)
		}
	}
	return append([]byte(nil), buffer.Bytes()...)
}

func TestSquareReadCommandsUseExistingSessionAndAllowReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name       string
		method     line.SquareReadOperation
		listField  int16
		args       []string
		jsonFields map[string]any
	}{
		{
			name: "squares", method: line.SquareReadGetJoinedSquares, listField: 1,
			args: []string{"squares", "--json", "--limit", "10"}, jsonFields: map[string]any{"squares": []any{}},
		},
		{
			name: "square chats", method: line.SquareReadGetJoinedSquareChats, listField: 1,
			args: []string{"square-chats", "--json"}, jsonFields: map[string]any{"chats": []any{}},
		},
		{
			name: "square messages", method: line.SquareReadFetchChatEvents, listField: 2,
			args: []string{"square-messages", "chat-mid", "--json"}, jsonFields: map[string]any{"chat_mid": "chat-mid", "messages": []any{}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := encodeEmptySquareCLIReply(t, string(tc.method), tc.listField)
			calls := 0
			api := &squareCLIAPI{}
			a, out, _ := testApp(nil)
			a.Manager.NewClient = func(token string) session.API {
				client := line.NewClient(token)
				client.HTTPClient.Transport = squareCLIRoundTripper(func(request *http.Request) (*http.Response, error) {
					calls++
					if request.URL.Host != "legy.line-apps.com" || request.URL.Path != "/SQ1" || request.Method != http.MethodPost {
						t.Fatalf("unexpected Square request URL: %s %s", request.Method, request.URL)
					}
					if request.Header.Get("x-line-access") != "secret" {
						t.Fatal("Square request did not use the session token provided by Manager")
					}
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
				})
				api.square = line.NewSquareClient(client)
				return api
			}
			args := append([]string{"--read-only"}, tc.args...)
			if err := a.Run(args); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("Square requests = %d, want one", calls)
			}
			var result map[string]any
			if err := json.Unmarshal(out.Bytes(), &result); err != nil {
				t.Fatalf("JSON output %q: %v", out.String(), err)
			}
			for key, want := range tc.jsonFields {
				if !reflect.DeepEqual(result[key], want) {
					t.Fatalf("missing JSON field %q in %s", key, out.String())
				}
			}
			if strings.Contains(out.String(), "session-token") || strings.Contains(out.String(), "secret") {
				t.Fatal("JSON output exposed session credentials")
			}
		})
	}
}

func TestSquareCommandArgumentsFailBeforeSessionAccess(t *testing.T) {
	for _, args := range [][]string{
		{"squares", "--limit", "101"},
		{"square-chats", "--cursor", strings.Repeat("x", 4097)},
		{"square-messages", "--limit", "0", "chat-mid"},
		{"square-messages", "bad\nmid"},
		{"square-messages"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			a, _, _ := testApp(nil)
			a.Lock = func() (func(), error) {
				t.Fatal("invalid Square arguments accessed the session")
				return nil, nil
			}
			if err := a.Run(args); err == nil {
				t.Fatal("invalid Square arguments accepted")
			}
		})
	}
}

func TestSquareCommandStructuredValidationError(t *testing.T) {
	a, out, diagnostics := testApp(nil)
	err := a.Run([]string{"--error-format=json", "--read-only", "squares", "--limit", "0"})
	var machine *MachineError
	if !errors.As(err, &machine) || machine.Detail.Code != "invalid_arguments" || out.Len() != 0 || diagnostics.Len() != 0 {
		t.Fatalf("err = %v, stdout=%q stderr=%q", err, out.String(), diagnostics.String())
	}
	if strings.Contains(machine.Error(), "session-token") {
		t.Fatal("structured validation error leaked credentials")
	}
}

func TestSquareReadOnlyIsNotARequestToMutate(t *testing.T) {
	a, out, _ := testApp(nil)
	a.Lock = func() (func(), error) { return nil, errors.New("stop before session") }
	err := a.Run([]string{"--read-only", "square-messages", "chat-mid", "--limit", "1"})
	if err == nil || !strings.Contains(err.Error(), "stop before session") || out.Len() != 0 {
		t.Fatalf("unexpected read-only handling: err=%v stdout=%q", err, out.String())
	}
}
