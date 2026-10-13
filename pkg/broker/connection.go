// Package broker implements WebSocket connection lifecycle management, channel-based
// broadcasting, and asynchronous event dispatching using worker pools.
package broker

import (
	"context"
	"strings"
	"sync"

	"emperror.dev/errors"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// ConnectionMode specifies the access permission mode of a WebSocket connection.
type ConnectionMode string

const (
	// ModeRW allows both reading (subscribing to events) and writing (publishing events).
	ModeRW ConnectionMode = "rw"
	// ModeRO allows only reading (subscribing to events), rejecting publish attempts.
	ModeRO ConnectionMode = "ro"
	// ModeWO allows only writing (publishing events), bypassing all outgoing broadcast deliveries.
	ModeWO ConnectionMode = "wo"
)

// ParseConnectionMode parses and validates a string into a ConnectionMode.
func ParseConnectionMode(s string) (ConnectionMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "rw", "readwrite":
		return ModeRW, nil
	case "ro", "readonly":
		return ModeRO, nil
	case "wo", "w", "writeonly":
		return ModeWO, nil
	default:
		return "", errors.Errorf("invalid or missing connection mode: %s", s)
	}
}

// CanRead returns true if the connection mode allows receiving messages/broadcasts.
func (m ConnectionMode) CanRead() bool {
	return m != ModeWO
}

// CanWrite returns true if the connection mode allows publishing messages.
func (m ConnectionMode) CanWrite() bool {
	return m != ModeRO
}

// NewConnection creates and initializes a new Connection wrapper with default ModeRW.
func NewConnection(conn *websocket.Conn, name string, secure bool) *Connection {
	return NewConnectionWithMode(conn, name, secure, ModeRW)
}

// NewConnectionWithMode creates and initializes a new Connection wrapper with the specified ConnectionMode.
func NewConnectionWithMode(conn *websocket.Conn, name string, secure bool, mode ConnectionMode) *Connection {
	if mode == "" {
		mode = ModeRW
	}
	return &Connection{
		Secure: secure,
		Conn:   conn,
		Name:   name,
		Mode:   mode,
	}
}

// Connection wraps an active WebSocket connection with thread-safe write operations and metadata.
type Connection struct {
	Secure  bool
	Conn    *websocket.Conn
	Name    string
	Mode    ConnectionMode
	writeMu sync.Mutex
}

// WriteJSON serializes the provided value to JSON and writes it to the WebSocket connection.
// It is protected by writeMu to guarantee thread-safe writes across concurrent callers.
func (c *Connection) WriteJSON(ctx context.Context, v any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.Conn == nil {
		return errors.New("connection is nil")
	}
	return errors.WithStack(wsjson.Write(ctx, c.Conn, v))
}

// WriteMessage sends a raw typed message (Text or Binary) to the WebSocket connection.
func (c *Connection) WriteMessage(ctx context.Context, messageType websocket.MessageType, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.Conn == nil {
		return errors.New("connection is nil")
	}
	return errors.WithStack(c.Conn.Write(ctx, messageType, data))
}

// Close gracefully terminates the underlying WebSocket connection with a NormalClosure status.
func (c *Connection) Close() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.Conn != nil {
		return errors.WithStack(c.Conn.Close(websocket.StatusNormalClosure, "closing connection"))
	}
	return nil
}
