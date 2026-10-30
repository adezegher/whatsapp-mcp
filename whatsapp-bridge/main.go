package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// Message represents a chat message for our client
type Message struct {
	Time      time.Time
	Sender    string
	Content   string
	IsFromMe  bool
	MediaType string
	Filename  string
}

// Database handler for storing message history
type MessageStore struct {
	db *sql.DB
}

// storeDir holds messages.db and whatsapp.db (the session keys); the bridge
// never downloads media. WHATSAPP_STORE_DIR overrides the default ./store (relative to the
// working directory).
var storeDir = "store"

// ensureStoreDir creates the store directory owner-only (0700) and tightens
// it if it already exists: whatsapp.db is a session secret.
func ensureStoreDir() error {
	if err := os.MkdirAll(storeDir, 0700); err != nil {
		return err
	}
	return os.Chmod(storeDir, 0700)
}

// sqliteDSN is the go-sqlite3 DSN for a database file inside storeDir.
func sqliteDSN(name string) string {
	return "file:" + filepath.Join(storeDir, name) + "?_foreign_keys=on"
}

// privateFile sets an existing store file to 0600 (best effort).
func privateFile(name string) {
	_ = os.Chmod(filepath.Join(storeDir, name), 0600)
}

// Initialize message store
func NewMessageStore() (*MessageStore, error) {
	// Create directory for database if it doesn't exist
	if err := ensureStoreDir(); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %v", err)
	}

	// Open SQLite database for messages
	db, err := sql.Open("sqlite3", sqliteDSN("messages.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to open message database: %v", err)
	}

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			content TEXT,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN,
			media_type TEXT,
			filename TEXT,
			url TEXT,
			media_key BLOB,
			file_sha256 BLOB,
			file_enc_sha256 BLOB,
			file_length INTEGER,
			PRIMARY KEY (id, chat_jid),
			FOREIGN KEY (chat_jid) REFERENCES chats(jid)
		);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	return &MessageStore{db: db}, nil
}

// Close the database connection
func (store *MessageStore) Close() error {
	return store.db.Close()
}

// Store a chat in the database
func (store *MessageStore) StoreChat(jid, name string, lastMessageTime time.Time) error {
	_, err := store.db.Exec(
		"INSERT OR REPLACE INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)",
		jid, name, lastMessageTime,
	)
	return err
}

// Store a message in the database
func (store *MessageStore) StoreMessage(id, chatJID, sender, content string, timestamp time.Time, isFromMe bool,
	mediaType, filename, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	// Only store if there's actual content or media
	if content == "" && mediaType == "" {
		return nil
	}

	_, err := store.db.Exec(
		`INSERT OR REPLACE INTO messages 
		(id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, chatJID, sender, content, timestamp, isFromMe, mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
	)
	return err
}

// Get messages from a chat
func (store *MessageStore) GetMessages(chatJID string, limit int) ([]Message, error) {
	rows, err := store.db.Query(
		"SELECT sender, content, timestamp, is_from_me, media_type, filename FROM messages WHERE chat_jid = ? ORDER BY timestamp DESC LIMIT ?",
		chatJID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		var timestamp time.Time
		err := rows.Scan(&msg.Sender, &msg.Content, &timestamp, &msg.IsFromMe, &msg.MediaType, &msg.Filename)
		if err != nil {
			return nil, err
		}
		msg.Time = timestamp
		messages = append(messages, msg)
	}

	return messages, nil
}

// Get all chats
func (store *MessageStore) GetChats() (map[string]time.Time, error) {
	rows, err := store.db.Query("SELECT jid, last_message_time FROM chats ORDER BY last_message_time DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chats := make(map[string]time.Time)
	for rows.Next() {
		var jid string
		var lastMessageTime time.Time
		err := rows.Scan(&jid, &lastMessageTime)
		if err != nil {
			return nil, err
		}
		chats[jid] = lastMessageTime
	}

	return chats, nil
}

// Extract text content from a message
func extractTextContent(msg *waProto.Message) string {
	if msg == nil {
		return ""
	}

	// Try to get text content
	if text := msg.GetConversation(); text != "" {
		return text
	} else if extendedText := msg.GetExtendedTextMessage(); extendedText != nil {
		return extendedText.GetText()
	}

	// For now, we're ignoring non-text messages
	return ""
}

// Extract media info from a message
func extractMediaInfo(msg *waProto.Message) (mediaType string, filename string, url string, mediaKey []byte, fileSHA256 []byte, fileEncSHA256 []byte, fileLength uint64) {
	if msg == nil {
		return "", "", "", nil, nil, nil, 0
	}

	// Check for image message
	if img := msg.GetImageMessage(); img != nil {
		return "image", "image_" + time.Now().Format("20060102_150405") + ".jpg",
			img.GetURL(), img.GetMediaKey(), img.GetFileSHA256(), img.GetFileEncSHA256(), img.GetFileLength()
	}

	// Check for video message
	if vid := msg.GetVideoMessage(); vid != nil {
		return "video", "video_" + time.Now().Format("20060102_150405") + ".mp4",
			vid.GetURL(), vid.GetMediaKey(), vid.GetFileSHA256(), vid.GetFileEncSHA256(), vid.GetFileLength()
	}

	// Check for audio message
	if aud := msg.GetAudioMessage(); aud != nil {
		return "audio", "audio_" + time.Now().Format("20060102_150405") + ".ogg",
			aud.GetURL(), aud.GetMediaKey(), aud.GetFileSHA256(), aud.GetFileEncSHA256(), aud.GetFileLength()
	}

	// Check for document message
	if doc := msg.GetDocumentMessage(); doc != nil {
		filename := doc.GetFileName()
		if filename == "" {
			filename = "document_" + time.Now().Format("20060102_150405")
		}
		return "document", filename,
			doc.GetURL(), doc.GetMediaKey(), doc.GetFileSHA256(), doc.GetFileEncSHA256(), doc.GetFileLength()
	}

	return "", "", "", nil, nil, nil, 0
}

// Handle regular incoming messages with media support
func handleMessage(client *whatsmeow.Client, messageStore *MessageStore, msg *events.Message, logger waLog.Logger) {
	// Save message to database
	chatJID := msg.Info.Chat.String()
	sender := msg.Info.Sender.User

	// Get appropriate chat name (pass nil for conversation since we don't have one for regular messages)
	name := GetChatName(client, messageStore, msg.Info.Chat, chatJID, nil, sender, logger)

	// Update chat in database with the message timestamp (keeps last message time updated)
	err := messageStore.StoreChat(chatJID, name, msg.Info.Timestamp)
	if err != nil {
		logger.Warnf("Failed to store chat: %v", err)
	}

	// Extract text content
	content := extractTextContent(msg.Message)

	// Extract media info
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg.Message)

	// Skip if there's no content and no media
	if content == "" && mediaType == "" {
		return
	}

	// Store message in database
	err = messageStore.StoreMessage(
		msg.Info.ID,
		chatJID,
		sender,
		content,
		msg.Info.Timestamp,
		msg.Info.IsFromMe,
		mediaType,
		filename,
		url,
		mediaKey,
		fileSHA256,
		fileEncSHA256,
		fileLength,
	)

	if err != nil {
		logger.Warnf("Failed to store message: %v", err)
		return
	}
	// Metadata only: message text and sender never go to the log.
	logger.Debugf("Stored message %s in %s (from_me=%v, media=%q, %d chars)",
		msg.Info.ID, chatJID, msg.Info.IsFromMe, mediaType, len(content))
}

// Store additional media info in the database
func (store *MessageStore) StoreMediaInfo(id, chatJID, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	_, err := store.db.Exec(
		"UPDATE messages SET url = ?, media_key = ?, file_sha256 = ?, file_enc_sha256 = ?, file_length = ? WHERE id = ? AND chat_jid = ?",
		url, mediaKey, fileSHA256, fileEncSHA256, fileLength, id, chatJID,
	)
	return err
}

// Exit codes (launchd restarts the bridge after any exit; see ThrottleInterval).
const (
	exitOK         = 0
	exitUsage      = 2
	exitNotLinked  = 3 // no session in the store and no terminal to show a QR
	exitLoggedOut  = 4 // WhatsApp removed this linked device
	exitStoreError = 5
	exitConnect    = 6 // could not connect (network, or "Client outdated (405)": rebuild with a newer whatsmeow)
)

// notLinkedWait keeps an unlinked bridge under launchd from restarting in a
// tight loop: it logs once, waits, then exits exitNotLinked.
const notLinkedWait = 10 * time.Minute

// stamp is the timestamp prefix for plain log lines: local time with offset.
func stamp() string { return time.Now().Format("2006-01-02T15:04:05.000-07:00") }

// lineLogger is a whatsmeow logger for a launchd log file: full date and
// offset on every line, no colour codes, one line per entry.
type lineLogger struct {
	mod   string
	debug bool
}

func (l *lineLogger) out(level, msg string, args ...any) {
	text := strings.ReplaceAll(fmt.Sprintf(msg, args...), "\n", " ")
	fmt.Fprintf(os.Stdout, "%s [%s %s] %s\n", stamp(), l.mod, level, text)
}
func (l *lineLogger) Errorf(msg string, args ...any) { l.out("ERROR", msg, args...) }
func (l *lineLogger) Warnf(msg string, args ...any)  { l.out("WARN", msg, args...) }
func (l *lineLogger) Infof(msg string, args ...any)  { l.out("INFO", msg, args...) }
func (l *lineLogger) Debugf(msg string, args ...any) {
	if l.debug {
		l.out("DEBUG", msg, args...)
	}
}
func (l *lineLogger) Sub(mod string) waLog.Logger {
	return &lineLogger{mod: l.mod + "/" + mod, debug: l.debug}
}

// isTerminal reports whether stdout is a terminal (Terminal.app) rather than
// a launchd log file.
func isTerminal() bool {
	fi, err := os.Stdout.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

func main() {
	link := flag.Bool("link", false, "pair with the phone: print the QR code to this terminal if the store has no session")
	debug := flag.Bool("debug", false, "log debug lines (message ids and chat names, never message text)")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), `Usage: %s [-link] [-debug]

Read-only WhatsApp bridge for James. Links as a companion device, stores
chats and messages in $WHATSAPP_STORE_DIR/messages.db (default ./store).
It has no send path, never downloads media and opens no network listener.

Without a session it prints the pairing QR only when stdout is a terminal
or -link is given; under launchd it logs "not linked" and exits %d.

Exit codes: 0 ok, 2 usage, 3 not linked, 4 logged out, 5 store error, 6 connect failure.

`, os.Args[0], exitNotLinked)
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() > 0 {
		flag.Usage()
		os.Exit(exitUsage)
	}
	if dir := os.Getenv("WHATSAPP_STORE_DIR"); dir != "" {
		storeDir = dir
	}

	logger := &lineLogger{mod: "Client", debug: *debug}
	dbLog := &lineLogger{mod: "Database", debug: *debug}
	fail := func(code int, msg string, args ...any) {
		logger.Errorf(msg, args...)
		os.Exit(code)
	}
	logger.Infof("Starting WhatsApp bridge (read-only), store %s", storeDir)

	// Create directory for database if it doesn't exist
	if err := ensureStoreDir(); err != nil {
		fail(exitStoreError, "Failed to create store directory: %v", err)
	}

	container, err := sqlstore.New(context.Background(), "sqlite3", sqliteDSN("whatsapp.db"), dbLog)
	if err != nil {
		fail(exitStoreError, "Failed to open session database: %v", err)
	}
	privateFile("whatsapp.db")

	// Get device store - This contains session information
	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		if err == sql.ErrNoRows {
			deviceStore = container.NewDevice()
			logger.Infof("Created new device")
		} else {
			fail(exitStoreError, "Failed to get device: %v", err)
		}
	}

	// Refuse to sit unlinked under launchd: a QR printed into a log file is
	// useless, and nobody can scan it.
	if deviceStore.ID == nil && !*link && !isTerminal() {
		logger.Errorf("Not linked: run `bin/james-whatsapp link` in Terminal and scan the QR from WhatsApp > Linked devices. Waiting %s before exiting.", notLinkedWait)
		time.Sleep(notLinkedWait)
		os.Exit(exitNotLinked)
	}

	// Create client instance
	client := whatsmeow.NewClient(deviceStore, logger)
	if client == nil {
		fail(exitConnect, "Failed to create WhatsApp client")
	}

	// Initialize message store
	messageStore, err := NewMessageStore()
	if err != nil {
		fail(exitStoreError, "Failed to initialize message store: %v", err)
	}
	privateFile("messages.db")
	defer messageStore.Close()

	// Setup event handling for messages and history sync
	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Message:
			handleMessage(client, messageStore, v, logger)

		case *events.HistorySync:
			handleHistorySync(client, messageStore, v, logger)

		case *events.Connected:
			logger.Infof("Connected to WhatsApp")

		case *events.Disconnected:
			logger.Warnf("Disconnected from WhatsApp (auto-reconnect is on)")

		case *events.ClientOutdated:
			logger.Errorf("Client outdated (405): rebuild the bridge against a newer whatsmeow")

		case *events.StreamReplaced:
			logger.Errorf("Stream replaced: another process opened this session; run only one bridge per store")

		case *events.TemporaryBan:
			logger.Errorf("Temporary ban from WhatsApp: %v", v)

		case *events.LoggedOut:
			logger.Errorf("Device logged out by WhatsApp (reason %v); exiting. Re-link with `bin/james-whatsapp link`.", v.Reason)
			messageStore.Close()
			os.Exit(exitLoggedOut)
		}
	})

	// Create channel to track connection success
	connected := make(chan bool, 1)

	// Connect to WhatsApp
	if client.Store.ID == nil {
		// No ID stored, this is a new client, need to pair with phone
		qrChan, _ := client.GetQRChannel(context.Background())
		err = client.Connect()
		if err != nil {
			fail(exitConnect, "Failed to connect: %v", err)
		}

		// Print QR code for pairing with phone
		for evt := range qrChan {
			if evt.Event == "code" {
				fmt.Println("\nScan this QR code with your WhatsApp app (Settings > Linked devices > Link a device):")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
			} else if evt.Event == "success" {
				connected <- true
				break
			} else {
				logger.Infof("Pairing: %s", evt.Event)
			}
		}

		// Wait for connection
		select {
		case <-connected:
			logger.Infof("Linked and authenticated. History may take several minutes to arrive.")
		case <-time.After(3 * time.Minute):
			fail(exitNotLinked, "Timeout waiting for QR code scan")
		}
	} else {
		// Already logged in, just connect
		err = client.Connect()
		if err != nil {
			fail(exitConnect, "Failed to connect: %v", err)
		}
		connected <- true
	}

	// Wait a moment for connection to stabilize
	time.Sleep(2 * time.Second)

	if !client.IsConnected() {
		fail(exitConnect, "Failed to establish stable connection")
	}

	logger.Infof("Connected; storing messages in %s", filepath.Join(storeDir, "messages.db"))

	// Create a channel to keep the main goroutine alive
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for termination signal
	<-exitChan

	logger.Infof("Disconnecting")
	client.Disconnect()
	// returning runs the deferred messageStore.Close(); exit status exitOK
}

// GetChatName determines the appropriate name for a chat based on JID and other info
func GetChatName(client *whatsmeow.Client, messageStore *MessageStore, jid types.JID, chatJID string, conversation interface{}, sender string, logger waLog.Logger) string {
	// First, check if chat already exists in database with a name
	var existingName string
	err := messageStore.db.QueryRow("SELECT name FROM chats WHERE jid = ?", chatJID).Scan(&existingName)
	if err == nil && existingName != "" {
		// Chat exists with a name, use that
		logger.Debugf("Using existing chat name for %s: %s", chatJID, existingName)
		return existingName
	}

	// Need to determine chat name
	var name string

	if jid.Server == "g.us" {
		// This is a group chat
		logger.Debugf("Getting name for group: %s", chatJID)

		// Use conversation data if provided (from history sync)
		if conversation != nil {
			// Extract name from conversation if available
			// This uses type assertions to handle different possible types
			var displayName, convName *string
			// Try to extract the fields we care about regardless of the exact type
			v := reflect.ValueOf(conversation)
			if v.Kind() == reflect.Ptr && !v.IsNil() {
				v = v.Elem()

				// Try to find DisplayName field
				if displayNameField := v.FieldByName("DisplayName"); displayNameField.IsValid() && displayNameField.Kind() == reflect.Ptr && !displayNameField.IsNil() {
					dn := displayNameField.Elem().String()
					displayName = &dn
				}

				// Try to find Name field
				if nameField := v.FieldByName("Name"); nameField.IsValid() && nameField.Kind() == reflect.Ptr && !nameField.IsNil() {
					n := nameField.Elem().String()
					convName = &n
				}
			}

			// Use the name we found
			if displayName != nil && *displayName != "" {
				name = *displayName
			} else if convName != nil && *convName != "" {
				name = *convName
			}
		}

		// If we didn't get a name, try group info
		if name == "" {
			groupInfo, err := client.GetGroupInfo(context.Background(), jid)
			if err == nil && groupInfo.Name != "" {
				name = groupInfo.Name
			} else {
				// Fallback name for groups
				name = fmt.Sprintf("Group %s", jid.User)
			}
		}

		logger.Debugf("Using group name: %s", name)
	} else {
		// This is an individual contact
		logger.Debugf("Getting name for contact: %s", chatJID)

		// Just use contact info (full name)
		contact, err := client.Store.Contacts.GetContact(context.Background(), jid)
		if err == nil && contact.FullName != "" {
			name = contact.FullName
		} else if sender != "" {
			// Fallback to sender
			name = sender
		} else {
			// Last fallback to JID
			name = jid.User
		}

		logger.Debugf("Using contact name: %s", name)
	}

	return name
}

// Handle history sync events
func handleHistorySync(client *whatsmeow.Client, messageStore *MessageStore, historySync *events.HistorySync, logger waLog.Logger) {
	logger.Infof("History sync event with %d conversations", len(historySync.Data.Conversations))

	syncedCount := 0
	for _, conversation := range historySync.Data.Conversations {
		// Parse JID from the conversation
		if conversation.ID == nil {
			continue
		}

		chatJID := *conversation.ID

		// Try to parse the JID
		jid, err := types.ParseJID(chatJID)
		if err != nil {
			logger.Warnf("Failed to parse JID %s: %v", chatJID, err)
			continue
		}

		// Get appropriate chat name by passing the history sync conversation directly
		name := GetChatName(client, messageStore, jid, chatJID, conversation, "", logger)

		// Process messages
		messages := conversation.Messages
		if len(messages) > 0 {
			// Update chat with latest message timestamp
			latestMsg := messages[0]
			if latestMsg == nil || latestMsg.Message == nil {
				continue
			}

			// Get timestamp from message info
			timestamp := time.Time{}
			if ts := latestMsg.Message.GetMessageTimestamp(); ts != 0 {
				timestamp = time.Unix(int64(ts), 0)
			} else {
				continue
			}

			messageStore.StoreChat(chatJID, name, timestamp)

			// Store messages
			for _, msg := range messages {
				if msg == nil || msg.Message == nil {
					continue
				}

				// Extract text content
				var content string
				if msg.Message.Message != nil {
					if conv := msg.Message.Message.GetConversation(); conv != "" {
						content = conv
					} else if ext := msg.Message.Message.GetExtendedTextMessage(); ext != nil {
						content = ext.GetText()
					}
				}

				// Extract media info
				var mediaType, filename, url string
				var mediaKey, fileSHA256, fileEncSHA256 []byte
				var fileLength uint64

				if msg.Message.Message != nil {
					mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength = extractMediaInfo(msg.Message.Message)
				}

				// Skip messages with no content and no media
				if content == "" && mediaType == "" {
					continue
				}

				// Determine sender
				var sender string
				isFromMe := false
				if msg.Message.Key != nil {
					if msg.Message.Key.FromMe != nil {
						isFromMe = *msg.Message.Key.FromMe
					}
					if !isFromMe && msg.Message.Key.Participant != nil && *msg.Message.Key.Participant != "" {
						sender = *msg.Message.Key.Participant
					} else if isFromMe {
						sender = client.Store.ID.User
					} else {
						sender = jid.User
					}
				} else {
					sender = jid.User
				}

				// Store message
				msgID := ""
				if msg.Message.Key != nil && msg.Message.Key.ID != nil {
					msgID = *msg.Message.Key.ID
				}

				// Get message timestamp
				timestamp := time.Time{}
				if ts := msg.Message.GetMessageTimestamp(); ts != 0 {
					timestamp = time.Unix(int64(ts), 0)
				} else {
					continue
				}

				err = messageStore.StoreMessage(
					msgID,
					chatJID,
					sender,
					content,
					timestamp,
					isFromMe,
					mediaType,
					filename,
					url,
					mediaKey,
					fileSHA256,
					fileEncSHA256,
					fileLength,
				)
				if err != nil {
					logger.Warnf("Failed to store history message: %v", err)
				} else {
					syncedCount++
				}
			}
		}
	}

	logger.Infof("History sync complete. Stored %d messages.", syncedCount)
}
