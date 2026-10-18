package rest

import (
	"bytes"
	"context"
	"encoding/json/v2"
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

	// 1. Connect Client A via /ws/clientA/rw?bearer=test-secret
	cA, _, err := websocket.Dial(ctx, wsURL+"/ws/clientA/rw?bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("clientA dial failed: %v", err)
	}
	defer cA.Close(websocket.StatusNormalClosure, "")

	// 2. Connect Client B via /ws/clientB/rw?channel=room1&bearer=test-secret
	cB, _, err := websocket.Dial(ctx, wsURL+"/ws/clientB/rw?channel=room1&bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("clientB dial failed: %v", err)
	}
	defer cB.Close(websocket.StatusNormalClosure, "")

	// 3. Connect Client C via /ws/channel/room1/rw?name=clientC&bearer=test-secret
	cC, _, err := websocket.Dial(ctx, wsURL+"/ws/channel/room1/rw?name=clientC&bearer="+bearer, nil)
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
		if err := json.UnmarshalRead(resp.Body, &counts); err != nil {
			t.Fatalf("failed to decode counts: %v", err)
		}
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
		if err := json.UnmarshalRead(resp.Body, &members); err != nil {
			t.Fatalf("failed to decode members: %v", err)
		}
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
	c, resp, err := websocket.Dial(ctx, wsURL+"/ws/testClient/rw?bearer=wrong-token", nil)
	if err == nil {
		c.Close(websocket.StatusNormalClosure, "")
		t.Errorf("expected dial to fail with invalid token")
	}
	if resp != nil && resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status 401 Unauthorized, got %d", resp.StatusCode)
	}
}

func TestWebSocketModes_PermissionsAndSuppression(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "test-secret"

	ctrl, err := NewController(":0", "http://localhost:8765", nil, bearer, true, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}

	ctrl.broker.Start(4)
	defer ctrl.GracefulStop()

	server := httptest.NewServer(ctrl.router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Connect Read-Only client
	cRO, _, err := websocket.Dial(ctx, wsURL+"/ws/channel/sharedRoom/ro?name=clientRO&bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("clientRO dial failed: %v", err)
	}
	defer cRO.Close(websocket.StatusNormalClosure, "")

	// 2. Connect Write-Only client (using 'w')
	cWO, _, err := websocket.Dial(ctx, wsURL+"/ws/channel/sharedRoom/w?name=clientWO&bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("clientWO dial failed: %v", err)
	}
	defer cWO.Close(websocket.StatusNormalClosure, "")

	// 3. Connect Read-Write client
	cRW, _, err := websocket.Dial(ctx, wsURL+"/ws/channel/sharedRoom/rw?name=clientRW&bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("clientRW dial failed: %v", err)
	}
	defer cRW.Close(websocket.StatusNormalClosure, "")

	var receivedRO = make(chan *event.Event, 10)
	var receivedWO = make(chan *event.Event, 10)
	var receivedRW = make(chan *event.Event, 10)

	startReader := func(c *websocket.Conn, ch chan *event.Event) {
		go func() {
			for {
				var evt event.Event
				if err := wsjson.Read(ctx, c, &evt); err != nil {
					break
				}
				ch <- &evt
			}
		}()
	}

	startReader(cRO, receivedRO)
	startReader(cWO, receivedWO)
	startReader(cRW, receivedRW)

	time.Sleep(50 * time.Millisecond)

	// Step A: Write-Only client publishes message to sharedRoom
	woEvt, _ := event.NewMessageEvent("clientWO", "sharedRoom", "msg from WO")
	if err := wsjson.Write(ctx, cWO, woEvt); err != nil {
		t.Fatalf("failed to send event from clientWO: %v", err)
	}

	// clientRO should receive it
	select {
	case evt := <-receivedRO:
		d, _ := evt.GetData()
		if d != "msg from WO" {
			t.Errorf("clientRO expected 'msg from WO', got %v", d)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("clientRO timed out waiting for message from WO")
	}

	// clientRW should receive it
	select {
	case evt := <-receivedRW:
		d, _ := evt.GetData()
		if d != "msg from WO" {
			t.Errorf("clientRW expected 'msg from WO', got %v", d)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("clientRW timed out waiting for message from WO")
	}

	// clientWO should NOT receive its own or any broadcast
	select {
	case evt := <-receivedWO:
		t.Errorf("clientWO should NOT receive broadcast, got: %+v", evt)
	case <-time.After(150 * time.Millisecond):
		// Expected
	}

	// Step B: Read-Only client attempts to publish message to sharedRoom (should be dropped)
	roEvt, _ := event.NewMessageEvent("clientRO", "sharedRoom", "illegal msg from RO")
	if err := wsjson.Write(ctx, cRO, roEvt); err != nil {
		t.Fatalf("failed to send event from clientRO: %v", err)
	}

	// clientRW should NOT receive the message dropped from clientRO
	select {
	case evt := <-receivedRW:
		t.Errorf("clientRW should NOT receive dropped message from RO, got: %+v", evt)
	case <-time.After(150 * time.Millisecond):
		// Expected
	}

	// Step C: Read-Only client sends Ping -> should receive Pong back
	pingEvt, _ := event.NewMessageEvent("clientRO", "wsbroker", "ping-payload")
	pingEvt.Type = event.TypePing
	if err := wsjson.Write(ctx, cRO, pingEvt); err != nil {
		t.Fatalf("failed to send ping from clientRO: %v", err)
	}

	select {
	case evt := <-receivedRO:
		if evt.Type != event.TypePong {
			t.Errorf("clientRO expected Pong event, got %v", evt.Type)
		}
		d, _ := evt.GetData()
		if d != "ping-payload" {
			t.Errorf("clientRO expected ping-payload, got %v", d)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("clientRO timed out waiting for pong event")
	}

	// Step D: Read-Write client publishes message to sharedRoom
	rwEvt, _ := event.NewMessageEvent("clientRW", "sharedRoom", "msg from RW")
	if err := wsjson.Write(ctx, cRW, rwEvt); err != nil {
		t.Fatalf("failed to send event from clientRW: %v", err)
	}

	// clientRO should receive it
	select {
	case evt := <-receivedRO:
		d, _ := evt.GetData()
		if d != "msg from RW" {
			t.Errorf("clientRO expected 'msg from RW', got %v", d)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("clientRO timed out waiting for message from RW")
	}

	// clientWO should NOT receive it
	select {
	case evt := <-receivedWO:
		t.Errorf("clientWO should NOT receive broadcast from RW, got: %+v", evt)
	case <-time.After(150 * time.Millisecond):
		// Expected
	}
}

func TestWebSocketModes_InvalidAndMissingMode(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "test-secret"

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

	// 1. Missing mode on /ws/:name (returns 404)
	c, resp, err := websocket.Dial(ctx, wsURL+"/ws/clientWithoutMode?bearer="+bearer, nil)
	if err == nil {
		c.Close(websocket.StatusNormalClosure, "")
		t.Errorf("expected missing mode connection to fail")
	}
	if resp != nil && resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404 Not Found for unparameterized route, got %d", resp.StatusCode)
	}

	// 2. Missing mode on /ws/channel/:channel (returns 404)
	c, resp, err = websocket.Dial(ctx, wsURL+"/ws/channel/roomWithoutMode?bearer="+bearer, nil)
	if err == nil {
		c.Close(websocket.StatusNormalClosure, "")
		t.Errorf("expected missing mode connection to fail")
	}
	if resp != nil && resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404 Not Found for unparameterized channel route, got %d", resp.StatusCode)
	}

	// 3. Invalid mode string on /ws/:name/:mode (returns 400 Bad Request)
	c, resp, err = websocket.Dial(ctx, wsURL+"/ws/clientInvalidMode/invalid?bearer="+bearer, nil)
	if err == nil {
		c.Close(websocket.StatusNormalClosure, "")
		t.Errorf("expected invalid mode connection to fail")
	}
	if resp != nil && resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400 Bad Request for invalid mode, got %d", resp.StatusCode)
	}

	// 4. Invalid mode string on /ws/channel/:channel/:mode (returns 400 Bad Request)
	c, resp, err = websocket.Dial(ctx, wsURL+"/ws/channel/room1/invalid?bearer="+bearer, nil)
	if err == nil {
		c.Close(websocket.StatusNormalClosure, "")
		t.Errorf("expected invalid mode connection to fail")
	}
	if resp != nil && resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400 Bad Request for invalid mode on channel, got %d", resp.StatusCode)
	}
}

func TestWebSocket_SourceValidation(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "test-secret"

	ctrl, err := NewController(":0", "http://localhost:8765", nil, bearer, true, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}

	ctrl.broker.Start(4)
	defer ctrl.GracefulStop()

	server := httptest.NewServer(ctrl.router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect sender client1
	cSender, _, err := websocket.Dial(ctx, wsURL+"/ws/client1/rw?channel=testchan&bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("cSender dial failed: %v", err)
	}
	defer cSender.Close(websocket.StatusNormalClosure, "")

	// Connect receiver client2
	cReceiver, _, err := websocket.Dial(ctx, wsURL+"/ws/client2/rw?channel=testchan&bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("cReceiver dial failed: %v", err)
	}
	defer cReceiver.Close(websocket.StatusNormalClosure, "")

	received := make(chan *event.Event, 10)
	go func() {
		for {
			var evt event.Event
			if err := wsjson.Read(ctx, cReceiver, &evt); err != nil {
				return
			}
			received <- &evt
		}
	}()

	time.Sleep(50 * time.Millisecond)

	// Case 1: Empty Source -> Fallback to client name ("client1")
	emptySrcEvt, _ := event.NewMessageEvent("", "testchan", "msg-fallback")
	if err := wsjson.Write(ctx, cSender, emptySrcEvt); err != nil {
		t.Fatalf("failed to write empty source event: %v", err)
	}

	select {
	case evt := <-received:
		if evt.Source != "client1" {
			t.Errorf("expected source 'client1' from fallback, got %q", evt.Source)
		}
		d, _ := evt.GetData()
		if d != "msg-fallback" {
			t.Errorf("expected payload 'msg-fallback', got %v", d)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for fallback source event")
	}

	// Case 2: Matching Source ("client1") -> Valid and accepted
	validSrcEvt, _ := event.NewMessageEvent("client1", "testchan", "msg-valid")
	if err := wsjson.Write(ctx, cSender, validSrcEvt); err != nil {
		t.Fatalf("failed to write valid source event: %v", err)
	}

	select {
	case evt := <-received:
		if evt.Source != "client1" {
			t.Errorf("expected source 'client1', got %q", evt.Source)
		}
		d, _ := evt.GetData()
		if d != "msg-valid" {
			t.Errorf("expected payload 'msg-valid', got %v", d)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for valid source event")
	}

	// Case 3: Mismatched/Spoofed Source ("spoofedClient") -> Dropped strictly
	spoofedEvt, _ := event.NewMessageEvent("spoofedClient", "testchan", "msg-spoofed")
	if err := wsjson.Write(ctx, cSender, spoofedEvt); err != nil {
		t.Fatalf("failed to write spoofed event: %v", err)
	}

	select {
	case evt := <-received:
		t.Errorf("unexpected event received for spoofed source: %+v", evt)
	case <-time.After(200 * time.Millisecond):
		// Expected: event was dropped
	}

	// Case 4: Dashboard client ("dashboard") is allowed to specify any source
	cDashboard, _, err := websocket.Dial(ctx, wsURL+"/ws/dashboard/rw?channel=testchan&bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("cDashboard dial failed: %v", err)
	}
	defer cDashboard.Close(websocket.StatusNormalClosure, "")

	time.Sleep(50 * time.Millisecond)

	dashEvt, _ := event.NewMessageEvent("custom-dash-source", "testchan", "msg-from-dash")
	if err := wsjson.Write(ctx, cDashboard, dashEvt); err != nil {
		t.Fatalf("failed to write dashboard event: %v", err)
	}

	select {
	case evt := <-received:
		if evt.Source != "custom-dash-source" {
			t.Errorf("expected source 'custom-dash-source' from dashboard, got %q", evt.Source)
		}
		d, _ := evt.GetData()
		if d != "msg-from-dash" {
			t.Errorf("expected payload 'msg-from-dash', got %v", d)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for dashboard event")
	}
}
