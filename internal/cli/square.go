package cli

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/kongesque/line-cli/pkg/line"
)

func (a *App) squareListCommand(command string, args []string) error {
	fs := newFlagSet(command)
	timeout := fs.Duration("timeout", 0, "command deadline including preparation, e.g. 30s; 0 disables it")
	limit := fs.Int("limit", 100, "maximum rows to fetch (1–100)")
	cursor := fs.String("cursor", "", "continue from a previous page's next_cursor")
	jsonOutput := fs.Bool("json", false, "write JSON to stdout")
	about := "List the OpenChat communities you have joined."
	usage := "line squares [options]"
	examples := []string{"line squares", "line squares --json --limit 50"}
	groups := []flagGroup{{"History", []string{"limit", "cursor"}}, {"Output", []string{"json"}}, {"Request", []string{"timeout"}}}
	if command == "square-chats" {
		about = "List the OpenChat rooms you have joined. Use a room MID with line square-messages."
		usage = "line square-chats [options]"
		examples = []string{"line square-chats", "line square-chats --json"}
	}
	h := commandHelp{Command: command, Usage: []string{usage}, About: about, Groups: groups, Examples: examples}
	if shown, err := a.parseFlags(fs, h, args); shown || err != nil {
		return err
	}
	restore, err := a.commandDeadline(*timeout)
	if err != nil {
		return err
	}
	defer restore()
	if fs.NArg() != 0 {
		return errors.New("unexpected arguments")
	}
	if err := validateSquarePageFlags(*limit, *cursor); err != nil {
		return err
	}
	unlock, err := a.lock()
	if err != nil {
		return err
	}
	defer unlock()
	if command == "squares" {
		page, err := a.Manager.GetJoinedSquaresContext(a.loginContext(), *limit, *cursor)
		if err != nil {
			return err
		}
		if *jsonOutput {
			return a.json(page)
		}
		return renderJoinedSquares(a, page)
	}
	page, err := a.Manager.GetJoinedSquareChatsContext(a.loginContext(), *limit, *cursor)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return a.json(page)
	}
	return renderJoinedSquareChats(a, page)
}

func (a *App) squareMessagesCommand(args []string) error {
	var chatMID string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		chatMID, args = args[0], args[1:]
	}
	fs := newFlagSet("square-messages")
	timeout := fs.Duration("timeout", 0, "command deadline including preparation, e.g. 30s; 0 disables it")
	limit := fs.Int("limit", 100, "maximum messages to fetch (1–100)")
	cursor := fs.String("cursor", "", "continue from a previous page's next_cursor")
	showIDs := fs.Bool("show-ids", false, "show message IDs in human output")
	jsonOutput := fs.Bool("json", false, "write JSON to stdout")
	h := commandHelp{Command: "square-messages", Usage: []string{"line square-messages CHAT_MID [options]"},
		About:    "Read one page of message events from a joined OpenChat room. CHAT_MID comes from line square-chats.",
		Groups:   []flagGroup{{"History", []string{"limit", "cursor"}}, {"Output", []string{"json", "show-ids"}}, {"Request", []string{"timeout"}}},
		Examples: []string{"line square-messages CHAT_MID", "line square-messages CHAT_MID --json --limit 50", "line square-messages CHAT_MID --json --cursor 'NEXT_CURSOR'"}}
	if shown, err := a.parseFlags(fs, h, args); shown || err != nil {
		return err
	}
	restore, err := a.commandDeadline(*timeout)
	if err != nil {
		return err
	}
	defer restore()
	if chatMID == "" && fs.NArg() == 1 {
		chatMID = fs.Arg(0)
	} else if fs.NArg() != 0 {
		return errors.New("provide exactly one OpenChat room MID")
	}
	if !validSquareCLIIdentifier(chatMID) {
		return errors.New("provide a valid OpenChat room MID from line square-chats")
	}
	if err := validateSquarePageFlags(*limit, *cursor); err != nil {
		return err
	}
	unlock, err := a.lock()
	if err != nil {
		return err
	}
	defer unlock()
	page, err := a.Manager.FetchSquareChatMessagesContext(a.loginContext(), chatMID, *limit, *cursor)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return a.json(page)
	}
	return renderSquareMessages(a, page, *showIDs)
}

func validateSquarePageFlags(limit int, cursor string) error {
	if limit < 1 || limit > 100 {
		return errors.New("limit must be between 1 and 100")
	}
	if len(cursor) > 4096 || !utf8.ValidString(cursor) || strings.ContainsFunc(cursor, unicode.IsControl) {
		return errors.New("invalid pagination cursor")
	}
	return nil
}

func validSquareCLIIdentifier(value string) bool {
	return value != "" && len(value) <= 256 && utf8.ValidString(value) && strings.TrimSpace(value) == value &&
		!strings.ContainsFunc(value, unicode.IsControl)
}

func renderJoinedSquares(a *App, page *line.JoinedSquaresPage) error {
	if _, err := fmt.Fprintf(a.Out, "%-46s  %s\n", "SQUARE MID", "NAME"); err != nil {
		return err
	}
	for _, square := range page.Squares {
		if _, err := fmt.Fprintf(a.Out, "%-46s  %s\n", terminalText(square.MID), terminalText(square.Name)); err != nil {
			return err
		}
	}
	if len(page.Squares) == 0 {
		if _, err := fmt.Fprintln(a.Out, "No joined OpenChat communities found."); err != nil {
			return err
		}
	}
	return writeSquareNextCursor(a, page.NextCursor)
}

func renderJoinedSquareChats(a *App, page *line.JoinedSquareChatsPage) error {
	if _, err := fmt.Fprintf(a.Out, "%-46s  %-46s  %s\n", "CHAT MID", "SQUARE MID", "NAME"); err != nil {
		return err
	}
	for _, chat := range page.Chats {
		if _, err := fmt.Fprintf(a.Out, "%-46s  %-46s  %s\n", terminalText(chat.MID), terminalText(chat.SquareMID), terminalText(chat.Name)); err != nil {
			return err
		}
	}
	if len(page.Chats) == 0 {
		if _, err := fmt.Fprintln(a.Out, "No joined OpenChat rooms found."); err != nil {
			return err
		}
	}
	return writeSquareNextCursor(a, page.NextCursor)
}

func renderSquareMessages(a *App, page *line.SquareMessagesPage, showIDs bool) error {
	for _, message := range page.Messages {
		timestamp := "unknown time"
		if message.CreatedAt > 0 {
			timestamp = time.UnixMilli(message.CreatedAt).Local().Format("2006-01-02 15:04")
		}
		body := message.Text
		if body == "" {
			body = fmt.Sprintf("[non-text content type %d]", message.ContentType)
		}
		sender := message.SenderDisplayName
		if sender == "" {
			sender = message.SenderMID
		}
		if _, err := fmt.Fprintf(a.Out, "%s  %-8s  %s: %s\n", timestamp, message.Direction, terminalText(sender), terminalText(body)); err != nil {
			return err
		}
		if showIDs {
			if _, err := fmt.Fprintf(a.Out, "  ID: %s\n", terminalText(message.ID)); err != nil {
				return err
			}
		}
	}
	if len(page.Messages) == 0 {
		if _, err := fmt.Fprintln(a.Out, "No text or message events found on this page."); err != nil {
			return err
		}
	}
	return writeSquareNextCursor(a, page.NextCursor)
}

func writeSquareNextCursor(a *App, cursor string) error {
	if cursor == "" {
		return nil
	}
	_, err := fmt.Fprintf(a.Out, "\nNext cursor: %s\nPass it with --cursor for the next page.\n", terminalText(cursor))
	return err
}
