package line

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	thrift "github.com/apache/thrift/lib/go/thrift"
)

const (
	defaultSquarePageSize = 100
	maxSquarePageSize     = 100
	maxSquareCursorLength = 4096
)

type JoinedSquare struct {
	MID              string `json:"mid"`
	Name             string `json:"name"`
	Description      string `json:"description,omitempty"`
	ProfileImageHash string `json:"profile_image_hash,omitempty"`
	Type             int32  `json:"type,omitempty"`
}

type JoinedSquaresPage struct {
	Squares    []JoinedSquare `json:"squares"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

type JoinedSquareChat struct {
	MID              string `json:"mid"`
	SquareMID        string `json:"square_mid"`
	Name             string `json:"name"`
	ProfileImageHash string `json:"profile_image_hash,omitempty"`
}

type JoinedSquareChatsPage struct {
	Chats      []JoinedSquareChat `json:"chats"`
	NextCursor string             `json:"next_cursor,omitempty"`
}

type SquareMessage struct {
	ID                string `json:"id"`
	SenderMID         string `json:"sender_mid,omitempty"`
	SenderDisplayName string `json:"sender_display_name,omitempty"`
	CreatedAt         int64  `json:"created_at,omitempty"`
	Text              string `json:"text,omitempty"`
	ContentType       int32  `json:"content_type,omitempty"`
	Direction         string `json:"direction"`
}

type SquareMessagesPage struct {
	ChatMID    string          `json:"chat_mid"`
	Messages   []SquareMessage `json:"messages"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

type SquareServiceExceptionError struct {
	Operation string
}

func (e *SquareServiceExceptionError) Error() string {
	if e == nil || e.Operation == "" {
		return ErrSquareServiceException.Error()
	}
	return fmt.Sprintf("%s during %s", ErrSquareServiceException, e.Operation)
}

func (e *SquareServiceExceptionError) Unwrap() error { return ErrSquareServiceException }

func (c *SquareClient) GetJoinedSquares(ctx context.Context, limit int, cursor string) (*JoinedSquaresPage, error) {
	limit, err := normalizeSquarePage(limit, cursor)
	if err != nil {
		return nil, err
	}
	response, err := c.CallRead(ctx, SquareReadGetJoinedSquares, squarePageArguments(limit, cursor))
	if err != nil {
		return nil, err
	}
	result, err := squareSuccess(response, string(SquareReadGetJoinedSquares))
	if err != nil {
		return nil, err
	}
	values, err := requiredSquareList(result, 1)
	if err != nil {
		return nil, fmt.Errorf("decode joined Squares: %w", err)
	}
	page := &JoinedSquaresPage{Squares: make([]JoinedSquare, 0, len(values))}
	for _, value := range values {
		square, ok := value.(SquareStruct)
		if !ok {
			return nil, errors.New("decode joined Squares: item is not a struct")
		}
		entry, err := decodeJoinedSquare(square)
		if err != nil {
			return nil, err
		}
		page.Squares = append(page.Squares, entry)
	}
	page.NextCursor, err = optionalSquareString(result, 5)
	if err != nil {
		return nil, fmt.Errorf("decode joined Squares cursor: %w", err)
	}
	return page, nil
}

func (c *SquareClient) GetJoinedSquareChats(ctx context.Context, limit int, cursor string) (*JoinedSquareChatsPage, error) {
	limit, err := normalizeSquarePage(limit, cursor)
	if err != nil {
		return nil, err
	}
	response, err := c.CallRead(ctx, SquareReadGetJoinedSquareChats, squarePageArguments(limit, cursor))
	if err != nil {
		return nil, err
	}
	result, err := squareSuccess(response, string(SquareReadGetJoinedSquareChats))
	if err != nil {
		return nil, err
	}
	values, err := requiredSquareList(result, 1)
	if err != nil {
		return nil, fmt.Errorf("decode joined Square chats: %w", err)
	}
	page := &JoinedSquareChatsPage{Chats: make([]JoinedSquareChat, 0, len(values))}
	for _, value := range values {
		chat, ok := value.(SquareStruct)
		if !ok {
			return nil, errors.New("decode joined Square chats: item is not a struct")
		}
		entry, err := decodeJoinedSquareChat(chat)
		if err != nil {
			return nil, err
		}
		page.Chats = append(page.Chats, entry)
	}
	page.NextCursor, err = optionalSquareString(result, 4)
	if err != nil {
		return nil, fmt.Errorf("decode joined Square chats cursor: %w", err)
	}
	return page, nil
}

func (c *SquareClient) FetchSquareChatMessages(ctx context.Context, chatMID string, limit int, cursor string) (*SquareMessagesPage, error) {
	if !validSquareIdentifier(chatMID) {
		return nil, errors.New("invalid Square chat MID")
	}
	limit, err := normalizeSquarePage(limit, cursor)
	if err != nil {
		return nil, err
	}
	requestFields := []SquareField{
		{ID: 2, Value: chatMID},
		{ID: 4, Value: int32(limit)},
		{ID: 5, Value: int32(2)}, // FetchDirection.BACKWARD
	}
	if cursor != "" {
		requestFields = append(requestFields, SquareField{ID: 7, Value: cursor})
	}
	response, err := c.CallRead(ctx, SquareReadFetchChatEvents, []SquareField{{
		ID: 1, Value: SquareStruct{Fields: requestFields},
	}})
	if err != nil {
		return nil, err
	}
	result, err := squareSuccess(response, string(SquareReadFetchChatEvents))
	if err != nil {
		return nil, err
	}
	events, err := requiredSquareList(result, 2)
	if err != nil {
		return nil, fmt.Errorf("decode Square chat events: %w", err)
	}
	page := &SquareMessagesPage{ChatMID: chatMID, Messages: make([]SquareMessage, 0, len(events))}
	for _, value := range events {
		event, ok := value.(SquareStruct)
		if !ok {
			return nil, errors.New("decode Square chat events: item is not a struct")
		}
		message, found, err := decodeSquareMessageEvent(event)
		if err != nil {
			return nil, err
		}
		if found {
			page.Messages = append(page.Messages, message)
		}
	}
	page.NextCursor, err = optionalSquareString(result, 4)
	if err != nil {
		return nil, fmt.Errorf("decode Square chat events cursor: %w", err)
	}
	return page, nil
}

func squarePageArguments(limit int, cursor string) []SquareField {
	fields := make([]SquareField, 0, 2)
	if cursor != "" {
		fields = append(fields, SquareField{ID: 2, Value: cursor})
	}
	fields = append(fields, SquareField{ID: 3, Value: int32(limit)})
	return []SquareField{{ID: 1, Value: SquareStruct{Fields: fields}}}
}

func validateSquarePageArguments(arguments []SquareField) error {
	request, err := squareRequestStruct(arguments)
	if err != nil {
		return err
	}
	if err := validateSquareFields(request, map[int16]thrift.TType{2: thrift.STRING, 3: thrift.I32}); err != nil {
		return err
	}
	limit, ok := squareFieldValue(request, 3).(int32)
	if !ok || limit < 1 || limit > maxSquarePageSize {
		return errors.New("square page limit must be between 1 and 100")
	}
	cursor, _ := squareFieldValue(request, 2).(string)
	if cursor != "" && len(cursor) > maxSquareCursorLength {
		return errors.New("square cursor exceeds size limit")
	}
	return nil
}

func validateSquareChatEventsArguments(arguments []SquareField) error {
	request, err := squareRequestStruct(arguments)
	if err != nil {
		return err
	}
	if err := validateSquareFields(request, map[int16]thrift.TType{
		2: thrift.STRING,
		4: thrift.I32,
		5: thrift.I32,
		7: thrift.STRING,
	}); err != nil {
		return err
	}
	chatMID, _ := squareFieldValue(request, 2).(string)
	if !validSquareIdentifier(chatMID) {
		return errors.New("invalid Square chat MID")
	}
	limit, ok := squareFieldValue(request, 4).(int32)
	if !ok || limit < 1 || limit > maxSquarePageSize {
		return errors.New("square event limit must be between 1 and 100")
	}
	direction, ok := squareFieldValue(request, 5).(int32)
	if !ok || direction != 2 {
		return errors.New("square chat history requires backward event direction")
	}
	cursor, _ := squareFieldValue(request, 7).(string)
	if cursor != "" && len(cursor) > maxSquareCursorLength {
		return errors.New("square cursor exceeds size limit")
	}
	return nil
}

func squareRequestStruct(arguments []SquareField) (SquareStruct, error) {
	if len(arguments) != 1 || arguments[0].ID != 1 {
		return SquareStruct{}, errors.New("square request requires exactly one request struct")
	}
	request, ok := arguments[0].Value.(SquareStruct)
	if !ok {
		return SquareStruct{}, errors.New("square request field 1 must be a struct")
	}
	return request, nil
}

func validateSquareFields(value SquareStruct, allowed map[int16]thrift.TType) error {
	seen := make(map[int16]bool, len(value.Fields))
	for _, field := range value.Fields {
		expected, ok := allowed[field.ID]
		if !ok || seen[field.ID] {
			return errors.New("square request contains an unknown or duplicate field")
		}
		actual, err := squareFieldType(field.Value)
		if err != nil || actual != expected {
			return fmt.Errorf("square request field %d has an invalid type", field.ID)
		}
		seen[field.ID] = true
	}
	return nil
}

func normalizeSquarePage(limit int, cursor string) (int, error) {
	if limit == 0 {
		limit = defaultSquarePageSize
	}
	if limit < 1 || limit > maxSquarePageSize {
		return 0, errors.New("square page limit must be between 1 and 100")
	}
	if len(cursor) > maxSquareCursorLength || !utf8.ValidString(cursor) || strings.ContainsFunc(cursor, unicode.IsControl) {
		return 0, errors.New("invalid Square pagination cursor")
	}
	return limit, nil
}

func validSquareIdentifier(value string) bool {
	return value != "" && len(value) <= 256 && utf8.ValidString(value) && !strings.ContainsFunc(value, unicode.IsControl)
}

func squareSuccess(response *SquareReadResponse, operation string) (SquareStruct, error) {
	if response == nil {
		return SquareStruct{}, errors.New("empty Square response")
	}
	var success SquareStruct
	hasSuccess, hasException := false, false
	for _, field := range response.Fields {
		switch field.ID {
		case 0:
			if hasSuccess || hasException {
				return SquareStruct{}, errors.New("square response has multiple result fields")
			}
			result, ok := field.Value.(SquareStruct)
			if !ok {
				return SquareStruct{}, errors.New("square success result is not a struct")
			}
			success, hasSuccess = result, true
		case 1:
			if hasSuccess || hasException {
				return SquareStruct{}, errors.New("square response has multiple result fields")
			}
			if _, ok := field.Value.(SquareStruct); !ok {
				return SquareStruct{}, errors.New("square service exception is not a struct")
			}
			hasException = true
		default:
			return SquareStruct{}, fmt.Errorf("square response has unknown result field %d", field.ID)
		}
	}
	if hasException {
		return SquareStruct{}, &SquareServiceExceptionError{Operation: operation}
	}
	if hasSuccess {
		return success, nil
	}
	return SquareStruct{}, errors.New("square response has no success result")
}

func requiredSquareList(value SquareStruct, id int16) ([]any, error) {
	field, ok := squareField(value, id)
	if !ok {
		return nil, fmt.Errorf("missing required list field %d", id)
	}
	list, ok := field.Value.(SquareList)
	if !ok || list.ElementType != thrift.STRUCT {
		return nil, fmt.Errorf("field %d is not a struct list", id)
	}
	return list.Values, nil
}

func decodeJoinedSquare(value SquareStruct) (JoinedSquare, error) {
	mid, err := requiredSquareString(value, 1)
	if err != nil {
		return JoinedSquare{}, fmt.Errorf("decode joined Square MID: %w", err)
	}
	result := JoinedSquare{MID: mid}
	if result.Name, err = optionalSquareString(value, 2); err != nil {
		return JoinedSquare{}, err
	}
	if result.ProfileImageHash, err = optionalSquareString(value, 4); err != nil {
		return JoinedSquare{}, err
	}
	if result.Description, err = optionalSquareString(value, 5); err != nil {
		return JoinedSquare{}, err
	}
	if result.Type, err = optionalSquareI32(value, 7); err != nil {
		return JoinedSquare{}, err
	}
	return result, nil
}

func decodeJoinedSquareChat(value SquareStruct) (JoinedSquareChat, error) {
	mid, err := requiredSquareString(value, 1)
	if err != nil {
		return JoinedSquareChat{}, fmt.Errorf("decode joined Square chat MID: %w", err)
	}
	squareMID, err := requiredSquareString(value, 2)
	if err != nil {
		return JoinedSquareChat{}, fmt.Errorf("decode joined Square MID: %w", err)
	}
	result := JoinedSquareChat{MID: mid, SquareMID: squareMID}
	if result.Name, err = optionalSquareString(value, 4); err != nil {
		return JoinedSquareChat{}, err
	}
	if result.ProfileImageHash, err = optionalSquareString(value, 5); err != nil {
		return JoinedSquareChat{}, err
	}
	return result, nil
}

func decodeSquareMessageEvent(value SquareStruct) (SquareMessage, bool, error) {
	eventType, ok := squareFieldValue(value, 3).(int32)
	if !ok || (eventType != 0 && eventType != 1) {
		return SquareMessage{}, false, nil
	}
	payload, ok := squareFieldValue(value, 4).(SquareStruct)
	if !ok {
		return SquareMessage{}, false, errors.New("square message event has no payload")
	}
	payloadID := int16(1)
	displayNameID := int16(3)
	direction := "received"
	if eventType == 1 {
		payloadID = 2
		displayNameID = 4
		direction = "sent"
	}
	eventPayload, ok := squareFieldValue(payload, payloadID).(SquareStruct)
	if !ok {
		return SquareMessage{}, false, errors.New("square message event payload is malformed")
	}
	squareMessage, ok := squareFieldValue(eventPayload, 2).(SquareStruct)
	if !ok {
		return SquareMessage{}, false, errors.New("square message event has no SquareMessage")
	}
	message, ok := squareFieldValue(squareMessage, 1).(SquareStruct)
	if !ok {
		return SquareMessage{}, false, errors.New("square message event has no Message")
	}
	id, err := requiredSquareString(message, 4)
	if err != nil {
		return SquareMessage{}, false, fmt.Errorf("decode Square message ID: %w", err)
	}
	result := SquareMessage{ID: id, Direction: direction}
	if result.SenderMID, err = optionalSquareString(message, 1); err != nil {
		return SquareMessage{}, false, err
	}
	if result.Text, err = optionalSquareString(message, 10); err != nil {
		return SquareMessage{}, false, err
	}
	if result.CreatedAt, err = optionalSquareI64(message, 5); err != nil {
		return SquareMessage{}, false, err
	}
	if result.ContentType, err = optionalSquareI32(message, 15); err != nil {
		return SquareMessage{}, false, err
	}
	if result.SenderDisplayName, err = optionalSquareString(eventPayload, displayNameID); err != nil {
		return SquareMessage{}, false, err
	}
	if result.CreatedAt == 0 {
		result.CreatedAt, err = optionalSquareI64(value, 2)
		if err != nil {
			return SquareMessage{}, false, err
		}
	}
	return result, true, nil
}

func requiredSquareString(value SquareStruct, id int16) (string, error) {
	field, ok := squareField(value, id)
	if !ok {
		return "", fmt.Errorf("missing field %d", id)
	}
	result, ok := field.Value.(string)
	if !ok {
		return "", fmt.Errorf("field %d is not a string", id)
	}
	return result, nil
}

func optionalSquareString(value SquareStruct, id int16) (string, error) {
	field, ok := squareField(value, id)
	if !ok {
		return "", nil
	}
	result, ok := field.Value.(string)
	if !ok {
		return "", fmt.Errorf("field %d is not a string", id)
	}
	return result, nil
}

func optionalSquareI32(value SquareStruct, id int16) (int32, error) {
	field, ok := squareField(value, id)
	if !ok {
		return 0, nil
	}
	result, ok := field.Value.(int32)
	if !ok {
		return 0, fmt.Errorf("field %d is not i32", id)
	}
	return result, nil
}

func optionalSquareI64(value SquareStruct, id int16) (int64, error) {
	field, ok := squareField(value, id)
	if !ok {
		return 0, nil
	}
	result, ok := field.Value.(int64)
	if !ok {
		return 0, fmt.Errorf("field %d is not i64", id)
	}
	return result, nil
}

func squareField(value SquareStruct, id int16) (SquareField, bool) {
	for _, field := range value.Fields {
		if field.ID == id {
			return field, true
		}
	}
	return SquareField{}, false
}

func squareFieldValue(value SquareStruct, id int16) any {
	field, ok := squareField(value, id)
	if !ok {
		return nil
	}
	return field.Value
}
