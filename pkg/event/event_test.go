package event

import (
	"encoding/json"
	"testing"
)

func TestEventCreationAndSerialization(t *testing.T) {
	msg := NewGenericStringMessage(TypeMessage, "hello world")
	evt, err := NewEvent(msg, "news-channel", "token-123")
	if err != nil {
		t.Fatalf("unexpected error creating event: %v", err)
	}

	if evt.GetType() != TypeMessage {
		t.Errorf("expected type %s, got %s", TypeMessage, evt.GetType())
	}
	if evt.GetTarget() != "news-channel" {
		t.Errorf("expected target 'news-channel', got %s", evt.GetTarget())
	}
	if evt.GetToken() != "token-123" {
		t.Errorf("expected token 'token-123', got %s", evt.GetToken())
	}

	data, err := evt.GetData()
	if err != nil {
		t.Fatalf("unexpected error getting data: %v", err)
	}
	if data != "hello world" {
		t.Errorf("expected data 'hello world', got %v", data)
	}

	jsonBytes, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	var unmarshaled Event
	if err := json.Unmarshal(jsonBytes, &unmarshaled); err != nil {
		t.Fatalf("failed to unmarshal event: %v", err)
	}

	if unmarshaled.Type != TypeMessage || unmarshaled.Target != "news-channel" {
		t.Errorf("unmarshaled event mismatch: %+v", unmarshaled)
	}
}

func TestNewMessageEvent(t *testing.T) {
	evt, err := NewMessageEvent("alice", "chat", map[string]string{"text": "hi"})
	if err != nil {
		t.Fatalf("failed to create message event: %v", err)
	}

	if evt.Source != "alice" || evt.Target != "chat" {
		t.Errorf("unexpected source/target: %s -> %s", evt.Source, evt.Target)
	}

	data, err := evt.GetData()
	if err != nil {
		t.Fatalf("failed to get data: %v", err)
	}
	dataMap, ok := data.(map[string]any)
	if !ok || dataMap["text"] != "hi" {
		t.Errorf("expected map with text=hi, got %v", data)
	}
}
