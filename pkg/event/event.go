// Package event defines the event structures, serialization, and standard event types
// exchanged between clients, the broker, and external publishers.
package event

import (
	"encoding/json"
	"fmt"

	"emperror.dev/errors"
)

// DataInterface provides a contract for typed data objects that can supply their event type and string representation.
type DataInterface interface {
	String() string
	Type() EventType
}

// NewEvent constructs a new Event instance by JSON-serializing the given DataInterface.
func NewEvent(data DataInterface, target string, token string) (*Event, error) {
	jsonStr, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal event data: %v", data)
	}
	return &Event{
		Type:   data.Type(),
		Target: target,
		Token:  token,
		Data:   jsonStr,
	}, nil
}

// NewMessageEvent creates an Event of type TypeMessage carrying arbitrary data as JSON payload.
func NewMessageEvent(source, target string, data any) (*Event, error) {
	var raw json.RawMessage
	switch v := data.(type) {
	case json.RawMessage:
		raw = v
	case []byte:
		raw = json.RawMessage(v)
	case string:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot marshal string data: %v", v)
		}
		raw = b
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot marshal event data: %v", data)
		}
		raw = b
	}
	return &Event{
		Type:   TypeMessage,
		Source: source,
		Target: target,
		Data:   raw,
	}, nil
}

// Event represents a standardized message envelope transmitted across WebSockets and REST APIs.
type Event struct {
	Type   EventType       `json:"type"`
	Source string          `json:"source"`
	Target string          `json:"target"`
	Token  string          `json:"token,omitempty"`
	Data   json.RawMessage `json:"data"`
}

// String returns a human-readable summary of the Event.
func (e *Event) String() string {
	return fmt.Sprintf("%s [%s -> %s]", e.Type, e.Source, e.Target)
}

// GetType returns the event type.
func (e *Event) GetType() EventType {
	return e.Type
}

// GetSource returns the sender identifier.
func (e *Event) GetSource() string {
	return e.Source
}

// GetTarget returns the recipient (channel name or client identifier).
func (e *Event) GetTarget() string {
	return e.Target
}

// GetToken returns the optional authentication/authorization token.
func (e *Event) GetToken() string {
	return e.Token
}

// GetData unmarshals the JSON payload into either a primitive string or generic JSON structure.
func (e *Event) GetData() (interface{}, error) {
	if len(e.Data) == 0 {
		return nil, nil
	}
	var msg string
	if err := json.Unmarshal(e.Data, &msg); err == nil {
		return msg, nil
	}
	var raw any
	if err := json.Unmarshal(e.Data, &raw); err != nil {
		return nil, errors.Wrapf(err, "cannot unmarshal event data: %v", string(e.Data))
	}
	return raw, nil
}
