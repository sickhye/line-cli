package line

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	thrift "github.com/apache/thrift/lib/go/thrift"
)

func encodeSquareReadFixture(t *testing.T, method string, result SquareStruct) []byte {
	t.Helper()
	buffer := thrift.NewTMemoryBuffer()
	protocol := thrift.NewTCompactProtocolConf(buffer, nil)
	ctx := context.Background()
	if err := protocol.WriteMessageBegin(ctx, method, thrift.REPLY, 0); err != nil {
		t.Fatal(err)
	}
	if err := protocol.WriteStructBegin(ctx, method+"_result"); err != nil {
		t.Fatal(err)
	}
	if err := protocol.WriteFieldBegin(ctx, "", thrift.STRUCT, 0); err != nil {
		t.Fatal(err)
	}
	writeSquareFixtureStruct(t, ctx, protocol, result)
	if err := protocol.WriteFieldEnd(ctx); err != nil {
		t.Fatal(err)
	}
	if err := protocol.WriteFieldStop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := protocol.WriteStructEnd(ctx); err != nil {
		t.Fatal(err)
	}
	if err := protocol.WriteMessageEnd(ctx); err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), buffer.Bytes()...)
}

func writeSquareFixtureStruct(t *testing.T, ctx context.Context, protocol thrift.TProtocol, value SquareStruct) {
	t.Helper()
	if err := protocol.WriteStructBegin(ctx, ""); err != nil {
		t.Fatal(err)
	}
	for _, field := range value.Fields {
		if list, ok := field.Value.(SquareList); ok {
			if err := protocol.WriteFieldBegin(ctx, "", thrift.LIST, field.ID); err != nil {
				t.Fatal(err)
			}
			if err := protocol.WriteListBegin(ctx, list.ElementType, len(list.Values)); err != nil {
				t.Fatal(err)
			}
			for _, item := range list.Values {
				if itemStruct, ok := item.(SquareStruct); ok {
					writeSquareFixtureStruct(t, ctx, protocol, itemStruct)
					continue
				}
				writeSquareFixtureValue(t, ctx, protocol, list.ElementType, item)
			}
			if err := protocol.WriteListEnd(ctx); err != nil {
				t.Fatal(err)
			}
			if err := protocol.WriteFieldEnd(ctx); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := writeSquareField(ctx, protocol, field); err != nil {
			t.Fatal(err)
		}
	}
	if err := protocol.WriteFieldStop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := protocol.WriteStructEnd(ctx); err != nil {
		t.Fatal(err)
	}
}

func writeSquareFixtureValue(t *testing.T, ctx context.Context, protocol thrift.TProtocol, typ thrift.TType, value any) {
	t.Helper()
	var err error
	switch typ {
	case thrift.STRING:
		if binary, ok := value.([]byte); ok {
			err = protocol.WriteBinary(ctx, binary)
		} else {
			err = protocol.WriteString(ctx, value.(string))
		}
	case thrift.I32:
		err = protocol.WriteI32(ctx, value.(int32))
	case thrift.I64:
		err = protocol.WriteI64(ctx, value.(int64))
	case thrift.BOOL:
		err = protocol.WriteBool(ctx, value.(bool))
	case thrift.DOUBLE:
		err = protocol.WriteDouble(ctx, value.(float64))
	default:
		t.Fatalf("unsupported test fixture type %s", typ)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func squareReadFixtureClient(t *testing.T, body []byte) *SquareClient {
	t.Helper()
	client := NewClient("synthetic-session-token")
	client.HTTPClient.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.Host != squareAPIHost || request.URL.Path != squareAPIPath {
			t.Fatalf("Square endpoint = %s %s", request.Method, request.URL)
		}
		if request.Header.Get("x-line-access") != "synthetic-session-token" {
			t.Fatal("Square request did not use the existing session credential")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	})
	return NewSquareClient(client)
}

func TestGetJoinedSquaresDecodesTypedPageAndCursor(t *testing.T) {
	body := encodeSquareReadFixture(t, string(SquareReadGetJoinedSquares), SquareStruct{Fields: []SquareField{
		{ID: 1, Value: SquareList{ElementType: thrift.STRUCT, Values: []any{
			SquareStruct{Fields: []SquareField{{ID: 1, Value: "square-mid"}, {ID: 2, Value: "Community"}, {ID: 5, Value: "Description"}}},
		}}},
		{ID: 5, Value: "next-square-page"},
	}})
	page, err := squareReadFixtureClient(t, body).GetJoinedSquares(context.Background(), 25, "previous")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Squares) != 1 || page.Squares[0].MID != "square-mid" || page.Squares[0].Name != "Community" ||
		page.Squares[0].Description != "Description" || page.NextCursor != "next-square-page" {
		t.Fatalf("decoded page = %#v", page)
	}
}

func TestGetJoinedSquareChatsDecodesRoomMIDAndCursor(t *testing.T) {
	body := encodeSquareReadFixture(t, string(SquareReadGetJoinedSquareChats), SquareStruct{Fields: []SquareField{
		{ID: 1, Value: SquareList{ElementType: thrift.STRUCT, Values: []any{
			SquareStruct{Fields: []SquareField{{ID: 1, Value: "chat-mid"}, {ID: 2, Value: "square-mid"}, {ID: 4, Value: "Room"}}},
		}}},
		{ID: 4, Value: "next-chat-page"},
	}})
	page, err := squareReadFixtureClient(t, body).GetJoinedSquareChats(context.Background(), 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Chats) != 1 || page.Chats[0].MID != "chat-mid" || page.Chats[0].SquareMID != "square-mid" ||
		page.Chats[0].Name != "Room" || page.NextCursor != "next-chat-page" {
		t.Fatalf("decoded page = %#v", page)
	}
}

func TestFetchSquareChatMessagesDecodesTextEventAndCursor(t *testing.T) {
	message := SquareStruct{Fields: []SquareField{
		{ID: 1, Value: "sender-mid"},
		{ID: 4, Value: "message-id"},
		{ID: 5, Value: int64(1700000000000)},
		{ID: 10, Value: "hello"},
		{ID: 15, Value: int32(0)},
	}}
	receiveMessage := SquareStruct{Fields: []SquareField{
		{ID: 2, Value: SquareStruct{Fields: []SquareField{
			{ID: 1, Value: message},
		}}},
		{ID: 3, Value: "Sender"},
	}}
	receiveEvent := SquareStruct{Fields: []SquareField{
		{ID: 2, Value: int64(1700000000000)},
		{ID: 3, Value: int32(0)},
		{ID: 4, Value: SquareStruct{Fields: []SquareField{{ID: 1, Value: receiveMessage}}}},
	}}
	sentMessage := SquareStruct{Fields: []SquareField{
		{ID: 1, Value: "self-mid"},
		{ID: 4, Value: "sent-message-id"},
		{ID: 10, Value: "sent text"},
	}}
	sendMessage := SquareStruct{Fields: []SquareField{
		{ID: 2, Value: SquareStruct{Fields: []SquareField{{ID: 1, Value: sentMessage}}}},
		{ID: 4, Value: "Self"},
	}}
	sendEvent := SquareStruct{Fields: []SquareField{
		{ID: 3, Value: int32(1)},
		{ID: 4, Value: SquareStruct{Fields: []SquareField{{ID: 2, Value: sendMessage}}}},
	}}
	body := encodeSquareReadFixture(t, string(SquareReadFetchChatEvents), SquareStruct{Fields: []SquareField{
		{ID: 2, Value: SquareList{ElementType: thrift.STRUCT, Values: []any{receiveEvent, sendEvent}}},
		{ID: 4, Value: "next-event-page"},
	}})
	page, err := squareReadFixtureClient(t, body).FetchSquareChatMessages(context.Background(), "chat-mid", 20, "prior-event")
	if err != nil {
		t.Fatal(err)
	}
	if page.ChatMID != "chat-mid" || len(page.Messages) != 2 {
		t.Fatalf("decoded page = %#v", page)
	}
	got := page.Messages[0]
	if got.ID != "message-id" || got.SenderMID != "sender-mid" || got.SenderDisplayName != "Sender" ||
		got.CreatedAt != 1700000000000 || got.Text != "hello" || got.Direction != "received" ||
		page.NextCursor != "next-event-page" {
		t.Fatalf("decoded message page = %#v", page)
	}
	sent := page.Messages[1]
	if sent.ID != "sent-message-id" || sent.SenderMID != "self-mid" || sent.SenderDisplayName != "Self" ||
		sent.Text != "sent text" || sent.Direction != "sent" {
		t.Fatalf("decoded sent message = %#v", sent)
	}
}

func TestSquareReadRejectsMalformedTypedResult(t *testing.T) {
	body := encodeSquareReadFixture(t, string(SquareReadGetJoinedSquares), SquareStruct{Fields: []SquareField{
		{ID: 1, Value: SquareList{ElementType: thrift.STRING, Values: []any{"not-a-square-struct"}}},
	}})
	if _, err := squareReadFixtureClient(t, body).GetJoinedSquares(context.Background(), 1, ""); err == nil {
		t.Fatal("accepted a response with the wrong list element schema")
	}
}

func TestSquareServiceExceptionIsNotMappedToSessionAuth(t *testing.T) {
	body := encodeTestSquareReply(t, "getJoinedSquares", 0, true)
	response, err := DecodeSquareResponse(context.Background(), body)
	if err != nil {
		t.Fatal(err)
	}
	_, err = squareSuccess(response, string(SquareReadGetJoinedSquares))
	if !errors.Is(err, ErrSquareServiceException) {
		t.Fatalf("service exception = %v", err)
	}
	if IsAuthError(err) || IsLoggedOut(err) {
		t.Fatalf("unverified Square service exception was mapped to session auth: %v", err)
	}
}

func TestSquareResultRejectsMalformedUnion(t *testing.T) {
	for _, response := range []*SquareReadResponse{
		{Method: "getJoinedSquares"},
		{Method: "getJoinedSquares", Fields: []SquareField{
			{ID: 0, Value: SquareStruct{}},
			{ID: 1, Value: SquareStruct{}},
		}},
		{Method: "getJoinedSquares", Fields: []SquareField{{ID: 2, Value: SquareStruct{}}}},
		{Method: "getJoinedSquares", Fields: []SquareField{{ID: 1, Value: "not-an-exception-struct"}}},
	} {
		if _, err := squareSuccess(response, response.Method); err == nil {
			t.Fatalf("accepted malformed result union: %#v", response)
		}
	}
}
