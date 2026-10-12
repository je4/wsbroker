package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/je4/wsbrokerclient/pkg/event"
	"github.com/rs/zerolog"
)

func TestWebSocketChannelsAndRestPublish(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "test-secret"

	ctrl, err := NewController(":0", "http://localhost:8765", nil, bearer, true, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}

	var wg sync.WaitGroup
	ctrl.broker.Start(4)
	defer ctrl.GracefulStop()

	server := httptest.NewServer(ctrl.router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Connect Client A via /ws/clientA?bearer=test-secret
	cA, _, err := websocket.Dial(ctx, wsURL+"/ws/clientA?bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("clientA dial failed: %v", err)
	}
	defer cA.Close(websocket.StatusNormalClosure, "")

	// 2. Connect Client B via /ws/clientB?channel=room1&bearer=test-secret
	cB, _, err := websocket.Dial(ctx, wsURL+"/ws/clientB?channel=room1&bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("clientB dial failed: %v", err)
	}
	defer cB.Close(websocket.StatusNormalClosure, "")

	// 3. Connect Client C via /ws/channel/room1?name=clientC&bearer=test-secret
	cC, _, err := websocket.Dial(ctx, wsURL+"/ws/channel/room1?name=clientC&bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("clientC dial failed: %v", err)
	}
	defer cC.Close(websocket.StatusNormalClosure, "")

	time.Sleep(50 * time.Millisecond)

	// Client A joins room1 by sending attach event
	attachEvt, err := event.NewMessageEvent("clientA", "room1", "room1")
	attachEvt.Type = event.TypeAttach
	if err := wsjson.Write(ctx, cA, attachEvt); err != nil {
		t.Fatalf("failed to send attach event from clientA: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Check channels via REST GET /api/v1/channels
	{
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/channels", nil)
		req.Header.Set("Authorization", "Bearer "+bearer)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET /channels failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 from /channels, got %d", resp.StatusCode)
		}
		var counts map[string]int
		json.NewDecoder(resp.Body).Decode(&counts)
		resp.Body.Close()

		if counts["room1"] != 3 {
			t.Errorf("expected 3 clients in room1, got %v", counts)
		}
	}

	// Channels clients via REST GET /api/v1/channel/room1/clients
	{
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/channel/room1/clients", nil)
		req.Header.Set("Authorization", "Bearer "+bearer)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET /channel/room1/clients failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		var members []string
		json.NewDecoder(resp.Body).Decode(&members)
		resp.Body.Close()

		if len(members) != 3 {
			t.Errorf("expected 3 members in room1, got %v", members)
		}
	}

	// Client A publishes message to room1 over WebSocket
	msgEvt, _ := event.NewMessageEvent("clientA", "room1", "hello from A")
	if err := wsjson.Write(ctx, cA, msgEvt); err != nil {
		t.Fatalf("clientA failed to publish message: %v", err)
	}

	// Verify Client B and Client C receive message from Client A
	for _, client := range []*websocket.Conn{cA, cB, cC} {
		var received event.Event
		readCtx, readCancel := context.WithTimeout(ctx, 1*time.Second)
		if err := wsjson.Read(readCtx, client, &received); err != nil {
			t.Errorf("failed to receive message on client: %v", err)
		} else {
			data, _ := received.GetData()
			if data != "hello from A" {
				t.Errorf("expected 'hello from A', got %v", data)
			}
		}
		readCancel()
	}

	// Publish message to room1 via REST POST /api/v1/channel/room1/publish
	{
		pubReq := PublishRequest{
			Source:  "rest-api",
			Message: "announcement via rest",
		}
		body, _ := json.Marshal(pubReq)
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/channel/room1/publish", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+bearer)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /channel/room1/publish failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		resp.Body.Close()

		// Verify clients receive REST announcement
		for _, client := range []*websocket.Conn{cB, cC} {
			var received event.Event
			readCtx, readCancel := context.WithTimeout(ctx, 1*time.Second)
			if err := wsjson.Read(readCtx, client, &received); err != nil {
				t.Errorf("failed to receive rest announcement on client: %v", err)
			} else {
				data, _ := received.GetData()
				if data != "announcement via rest" {
					t.Errorf("expected 'announcement via rest', got %v", data)
				}
			}
			readCancel()
		}
	}

	// Client C sends detach event from room1
	detachEvt, _ := event.NewMessageEvent("clientC", "room1", "room1")
	detachEvt.Type = event.TypeDetach
	if err := wsjson.Write(ctx, cC, detachEvt); err != nil {
		t.Fatalf("failed to send detach event: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Publish another message to room1 via REST POST /api/v1/publish
	{
		pubReq := PublishRequest{
			Source:  "rest-api",
			Target:  "room1",
			Message: "after detach",
		}
		body, _ := json.Marshal(pubReq)
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/publish", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+bearer)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST /publish failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		resp.Body.Close()

		// Client B should receive it
		var receivedB event.Event
		readCtxB, readCancelB := context.WithTimeout(ctx, 1*time.Second)
		if err := wsjson.Read(readCtxB, cB, &receivedB); err != nil {
			t.Errorf("clientB failed to receive: %v", err)
		} else {
			data, _ := receivedB.GetData()
			if data != "after detach" {
				t.Errorf("expected 'after detach', got %v", data)
			}
		}
		readCancelB()

		// Client C should NOT receive it
		var receivedC event.Event
		readCtxC, readCancelC := context.WithTimeout(ctx, 100*time.Millisecond)
		if err := wsjson.Read(readCtxC, cC, &receivedC); err == nil {
			t.Errorf("clientC should not have received message after detach, got: %+v", receivedC)
		}
		readCancelC()
	}
	_ = wg
}

func TestWebSocketAuthFailure(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "secure-token"

	ctrl, err := NewController(":0", "http://localhost:8765", nil, bearer, false, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	defer ctrl.GracefulStop()

	server := httptest.NewServer(ctrl.router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Dial with wrong token -> should fail
	c, resp, err := websocket.Dial(ctx, wsURL+"/ws/testClient?bearer=wrong-token", nil)
	if err == nil {
		c.Close(websocket.StatusNormalClosure, "")
		t.Errorf("expected dial to fail with invalid token")
	}
	if resp != nil && resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status 401 Unauthorized, got %d", resp.StatusCode)
	}
}
