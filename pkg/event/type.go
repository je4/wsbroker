// Package event defines the event structures, serialization, and standard event types
// exchanged between clients, the broker, and external publishers.
package event

// EventType represents the category of event sent across WebSocket channels.
type EventType string

const (
	// TypeAttach is sent by a client to subscribe to one or more channels.
	TypeAttach EventType = "attach"
	// TypeDetach is sent by a client to unsubscribe from one or more channels.
	TypeDetach EventType = "detach"
	// TypeMessage represents a standard message carrying a data payload.
	TypeMessage EventType = "message"
	// TypeStringMessage is an alias for TypeMessage for simple string messages.
	TypeStringMessage EventType = "message"
	// TypePing represents a ping heartbeat or health-check event.
	TypePing EventType = "ping"
	// TypePong represents a pong heartbeat response event.
	TypePong EventType = "pong"
)
