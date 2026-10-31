package rest

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"slices"
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

	// Verify Client B and Client C receive message from Client A with ID and channel populated
	for _, client := range []*websocket.Conn{cA, cB, cC} {
		var received event.RawEvent
		readCtx, readCancel := context.WithTimeout(ctx, 1*time.Second)
		if err := wsjson.Read(readCtx, client, &received); err != nil {
			t.Errorf("failed to receive message on client: %v", err)
		} else {
			data, _ := received.GetData()
			if data != "hello from A" {
				t.Errorf("expected 'hello from A', got %v", data)
			}
			if received.ID == "" {
				t.Errorf("expected non-empty event ID on received channel event")
			}
			if received.Channel != "room1" {
				t.Errorf("expected channel 'room1', got %q", received.Channel)
			}
		}
		readCancel()
	}

	// Publish message to room1 via REST POST /api/v1/channel/room1/publish
	{
		pubReq := PublishRequest{
			ID:      "rest-custom-id-123",
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
		var pubResp PublishResponse
		if err := json.UnmarshalRead(resp.Body, &pubResp); err != nil {
			t.Fatalf("failed to decode publish response: %v", err)
		}
		resp.Body.Close()

		if pubResp.ID != "rest-custom-id-123" {
			t.Errorf("expected publish response ID 'rest-custom-id-123', got %q", pubResp.ID)
		}
		if pubResp.Channel != "room1" {
			t.Errorf("expected publish response Channel 'room1', got %q", pubResp.Channel)
		}

		// Verify clients receive REST announcement with matching ID and channel
		for _, client := range []*websocket.Conn{cB, cC} {
			var received event.RawEvent
			readCtx, readCancel := context.WithTimeout(ctx, 1*time.Second)
			if err := wsjson.Read(readCtx, client, &received); err != nil {
				t.Errorf("failed to receive rest announcement on client: %v", err)
			} else {
				data, _ := received.GetData()
				if data != "announcement via rest" {
					t.Errorf("expected 'announcement via rest', got %v", data)
				}
				if received.ID != "rest-custom-id-123" {
					t.Errorf("expected event ID 'rest-custom-id-123', got %q", received.ID)
				}
				if received.Channel != "room1" {
					t.Errorf("expected channel 'room1', got %q", received.Channel)
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
		var receivedB event.RawEvent
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
		var receivedC event.RawEvent
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

	var receivedRO = make(chan *event.RawEvent, 10)
	var receivedWO = make(chan *event.RawEvent, 10)
	var receivedRW = make(chan *event.RawEvent, 10)

	startReader := func(c *websocket.Conn, ch chan *event.RawEvent) {
		go func() {
			for {
				var evt event.RawEvent
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
		if evt.RefID != pingEvt.ID {
			t.Errorf("expected pong RefID %s, got %s", pingEvt.ID, evt.RefID)
		}
		if evt.ID == "" || evt.ID == pingEvt.ID {
			t.Errorf("expected unique pong ID, got %s (ping ID: %s)", evt.ID, pingEvt.ID)
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

	received := make(chan *event.RawEvent, 10)
	go func() {
		for {
			var evt event.RawEvent
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

func TestWebSocket_LargePayload(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "test-secret"

	ctrl, err := NewController(":0", "http://localhost:8765", nil, bearer, false, &logger)
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

	// 1. Connect Client Receiver and set read limit on client connection
	cReceiver, _, err := websocket.Dial(ctx, wsURL+"/ws/receiver/rw?channel=screenshots&bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("cReceiver dial failed: %v", err)
	}
	defer cReceiver.Close(websocket.StatusNormalClosure, "")
	cReceiver.SetReadLimit(16 * 1024 * 1024)

	// 2. Connect Client Sender
	cSender, _, err := websocket.Dial(ctx, wsURL+"/ws/sender/rw?bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("cSender dial failed: %v", err)
	}
	defer cSender.Close(websocket.StatusNormalClosure, "")
	cSender.SetReadLimit(16 * 1024 * 1024)

	time.Sleep(50 * time.Millisecond)

	// 3. Sender sends large file event (> 64 KiB, e.g. 1 MiB payload)
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	fileMsg := event.NewFileMessage("display_screenshot.jpg", "image/jpeg", largeData)
	fileEvt, err := event.NewRawEvent(event.TypeFile, "sender", "screenshots", fileMsg)
	if err != nil {
		t.Fatalf("failed to create file event: %v", err)
	}

	if err := wsjson.Write(ctx, cSender, fileEvt); err != nil {
		t.Fatalf("failed to write large file event: %v", err)
	}

	// 4. Receiver reads event
	var recEvt event.RawEvent
	if err := wsjson.Read(ctx, cReceiver, &recEvt); err != nil {
		t.Fatalf("receiver failed to read large file event: %v", err)
	}

	data, err := recEvt.GetData()
	if err != nil {
		t.Fatalf("failed to get data from event: %v", err)
	}
	recFile, ok := data.(*event.FileMessage)
	if !ok {
		t.Fatalf("expected *event.FileMessage, got %T", data)
	}
	if len(recFile.Data) != len(largeData) {
		t.Errorf("expected data length %d, got %d", len(largeData), len(recFile.Data))
	}
	if recFile.Filename != "display_screenshot.jpg" {
		t.Errorf("expected filename display_screenshot.jpg, got %s", recFile.Filename)
	}
}

func TestWebSocket_CustomReadLimitExceeded(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "test-secret"

	ctrl, err := NewController(":0", "http://localhost:8765", nil, bearer, false, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	// Configure custom read limit (100 KiB)
	ctrl.SetReadLimit(100 * 1024)

	ctrl.broker.Start(4)
	defer ctrl.GracefulStop()

	server := httptest.NewServer(ctrl.router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Connect Client Receiver
	cReceiver, _, err := websocket.Dial(ctx, wsURL+"/ws/receiver/rw?channel=testchan&bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("cReceiver dial failed: %v", err)
	}
	defer cReceiver.Close(websocket.StatusNormalClosure, "")
	cReceiver.SetReadLimit(16 * 1024 * 1024)

	// 2. Connect Client Sender
	cSender, _, err := websocket.Dial(ctx, wsURL+"/ws/sender/rw?bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("cSender dial failed: %v", err)
	}
	defer cSender.Close(websocket.StatusNormalClosure, "")
	cSender.SetReadLimit(16 * 1024 * 1024)

	time.Sleep(50 * time.Millisecond)

	// 3. Sender sends payload within limit (20 KiB raw -> ~27 KiB JSON)
	data20k := make([]byte, 20*1024)
	for i := range data20k {
		data20k[i] = byte(i % 256)
	}
	msg20k := event.NewFileMessage("small.bin", "application/octet-stream", data20k)
	evt20k, err := event.NewRawEvent(event.TypeFile, "sender", "testchan", msg20k)
	if err != nil {
		t.Fatalf("failed to create file event: %v", err)
	}
	if err := wsjson.Write(ctx, cSender, evt20k); err != nil {
		t.Fatalf("failed to send 20k payload: %v", err)
	}

	var recEvt20k event.RawEvent
	if err := wsjson.Read(ctx, cReceiver, &recEvt20k); err != nil {
		t.Fatalf("failed to receive 20k payload: %v", err)
	}

	// 4. Sender sends payload exceeding limit (150 KiB raw -> ~200 KiB JSON)
	data150k := make([]byte, 150*1024)
	for i := range data150k {
		data150k[i] = byte(i % 256)
	}
	msg150k := event.NewFileMessage("large.bin", "application/octet-stream", data150k)
	evt150k, err := event.NewRawEvent(event.TypeFile, "sender", "testchan", msg150k)
	if err != nil {
		t.Fatalf("failed to create file event: %v", err)
	}
	if err := wsjson.Write(ctx, cSender, evt150k); err != nil {
		t.Logf("write 150k payload result: %v", err)
	}

	// Server should close connection due to read limit exceeded
	// Wait and verify reading on cSender fails with StatusMessageTooBig or close status
	var dummyEvt event.RawEvent
	readErr := wsjson.Read(ctx, cSender, &dummyEvt)
	if readErr == nil {
		t.Errorf("expected server to close connection due to read limit, but read succeeded")
	} else {
		status := websocket.CloseStatus(readErr)
		if status != websocket.StatusMessageTooBig && !strings.Contains(readErr.Error(), "read limit") && !strings.Contains(readErr.Error(), "StatusMessageTooBig") {
			t.Logf("connection closed with error: %v (status: %v)", readErr, status)
		}
	}
}

func TestWebSocket_ChannelInfoQuery(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "test-secret"

	ctrl, err := NewController(":0", "http://localhost:8765", nil, bearer, false, &logger)
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

	// 1. Connect Client A to room1
	cA, _, err := websocket.Dial(ctx, wsURL+"/ws/clientA/rw?channel=room1&bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("clientA dial failed: %v", err)
	}
	defer cA.Close(websocket.StatusNormalClosure, "")

	// 2. Connect Client B to room1
	cB, _, err := websocket.Dial(ctx, wsURL+"/ws/clientB/rw?channel=room1&bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("clientB dial failed: %v", err)
	}
	defer cB.Close(websocket.StatusNormalClosure, "")

	// 3. Connect Client C to room2
	cC, _, err := websocket.Dial(ctx, wsURL+"/ws/clientC/rw?channel=room2&bearer="+bearer, nil)
	if err != nil {
		t.Fatalf("clientC dial failed: %v", err)
	}
	defer cC.Close(websocket.StatusNormalClosure, "")

	time.Sleep(50 * time.Millisecond)

	// Scenario 1: Client A queries room1 info (target in target field)
	{
		queryEvt, err := event.NewRawEvent(event.TypeChannelInfoQuery, "clientA", "room1", "room1")
		if err != nil {
			t.Fatalf("failed to create query event: %v", err)
		}
		if err := wsjson.Write(ctx, cA, queryEvt); err != nil {
			t.Fatalf("failed to write query event: %v", err)
		}

		var respEvt event.RawEvent
		if err := wsjson.Read(ctx, cA, &respEvt); err != nil {
			t.Fatalf("failed to read channel info response: %v", err)
		}
		if respEvt.Type != event.TypeChannelInfoResponse {
			t.Fatalf("expected TypeChannelInfoResponse, got %s", respEvt.Type)
		}
		if respEvt.RefID != queryEvt.ID {
			t.Errorf("expected RefID %s, got %s", queryEvt.ID, respEvt.RefID)
		}
		if respEvt.ID == "" || respEvt.ID == queryEvt.ID {
			t.Errorf("expected unique response ID, got %s (query ID: %s)", respEvt.ID, queryEvt.ID)
		}
		if respEvt.Target != "clientA" {
			t.Errorf("expected target clientA, got %s", respEvt.Target)
		}

		var info ChannelInfo
		if err := respEvt.UnmarshalData(&info); err != nil {
			t.Fatalf("failed to unmarshal channel info: %v", err)
		}
		if info.Name != "room1" {
			t.Errorf("expected channel name room1, got %s", info.Name)
		}
		if info.Subscribers != 2 {
			t.Errorf("expected 2 subscribers, got %d", info.Subscribers)
		}
		if !slices.Contains(info.Clients, "clientA") || !slices.Contains(info.Clients, "clientB") {
			t.Errorf("expected clients to contain clientA and clientB, got %v", info.Clients)
		}
	}

	// Scenario 2: Client A queries room2 info via data payload
	{
		dataBytes, _ := json.Marshal("room2")
		queryEvt := &event.RawEvent{
			BaseEvent: event.BaseEvent{
				ID:     "ch-query-2",
				Type:   event.TypeChannelInfoQuery,
				Source: "clientA",
			},
			Data: jsontext.Value(dataBytes),
		}
		if err := wsjson.Write(ctx, cA, queryEvt); err != nil {
			t.Fatalf("failed to write query event: %v", err)
		}

		var respEvt event.RawEvent
		if err := wsjson.Read(ctx, cA, &respEvt); err != nil {
			t.Fatalf("failed to read channel info response: %v", err)
		}
		if respEvt.Type != event.TypeChannelInfoResponse {
			t.Fatalf("expected TypeChannelInfoResponse, got %s", respEvt.Type)
		}
		if respEvt.RefID != queryEvt.ID {
			t.Errorf("expected RefID %s, got %s", queryEvt.ID, respEvt.RefID)
		}
		if respEvt.ID == "" || respEvt.ID == queryEvt.ID {
			t.Errorf("expected unique response ID, got %s (query ID: %s)", respEvt.ID, queryEvt.ID)
		}

		var info ChannelInfo
		if err := respEvt.UnmarshalData(&info); err != nil {
			t.Fatalf("failed to unmarshal channel info: %v", err)
		}
		if info.Name != "room2" {
			t.Errorf("expected channel name room2, got %s", info.Name)
		}
		if info.Subscribers != 1 {
			t.Errorf("expected 1 subscriber in room2, got %d", info.Subscribers)
		}
		if len(info.Clients) != 1 || info.Clients[0] != "clientC" {
			t.Errorf("expected clientC in room2, got %v", info.Clients)
		}
	}

	// Scenario 3: Non-existent / empty channel query
	{
		queryEvt, err := event.NewRawEvent(event.TypeChannelInfoQuery, "clientA", "non-existent-channel", "non-existent-channel")
		if err != nil {
			t.Fatalf("failed to create query event: %v", err)
		}
		if err := wsjson.Write(ctx, cA, queryEvt); err != nil {
			t.Fatalf("failed to write query event: %v", err)
		}

		var respEvt event.RawEvent
		if err := wsjson.Read(ctx, cA, &respEvt); err != nil {
			t.Fatalf("failed to read channel info response: %v", err)
		}
		if respEvt.Type != event.TypeChannelInfoResponse {
			t.Fatalf("expected TypeChannelInfoResponse, got %s", respEvt.Type)
		}
		if respEvt.RefID != queryEvt.ID {
			t.Errorf("expected RefID %s, got %s", queryEvt.ID, respEvt.RefID)
		}
		if respEvt.ID == "" || respEvt.ID == queryEvt.ID {
			t.Errorf("expected unique response ID, got %s (query ID: %s)", respEvt.ID, queryEvt.ID)
		}

		var info ChannelInfo
		if err := respEvt.UnmarshalData(&info); err != nil {
			t.Fatalf("failed to unmarshal channel info: %v", err)
		}
		if info.Name != "non-existent-channel" {
			t.Errorf("expected name non-existent-channel, got %s", info.Name)
		}
		if info.Subscribers != 0 {
			t.Errorf("expected 0 subscribers, got %d", info.Subscribers)
		}
		if len(info.Clients) != 0 {
			t.Errorf("expected empty clients list, got %v", info.Clients)
		}
	}

	// Scenario 4: Missing channel name
	{
		queryEvt := &event.RawEvent{
			BaseEvent: event.BaseEvent{
				ID:     "ch-query-4",
				Type:   event.TypeChannelInfoQuery,
				Source: "clientA",
			},
		}
		if err := wsjson.Write(ctx, cA, queryEvt); err != nil {
			t.Fatalf("failed to write query event: %v", err)
		}

		var respEvt event.RawEvent
		if err := wsjson.Read(ctx, cA, &respEvt); err != nil {
			t.Fatalf("failed to read channel info error response: %v", err)
		}
		if respEvt.Type != event.TypeChannelInfoError {
			t.Fatalf("expected TypeChannelInfoError, got %s", respEvt.Type)
		}
		if respEvt.RefID != queryEvt.ID {
			t.Errorf("expected RefID %s, got %s", queryEvt.ID, respEvt.RefID)
		}
		if respEvt.ID == "" || respEvt.ID == queryEvt.ID {
			t.Errorf("expected unique response ID, got %s (query ID: %s)", respEvt.ID, queryEvt.ID)
		}
		var errMsg string
		if err := respEvt.UnmarshalData(&errMsg); err != nil {
			t.Fatalf("failed to unmarshal error msg: %v", err)
		}
		if !strings.Contains(errMsg, "channel name required") {
			t.Errorf("expected 'channel name required', got '%s'", errMsg)
		}
	}

	// Scenario 5: Spoofed source
	{
		queryEvt := &event.RawEvent{
			BaseEvent: event.BaseEvent{
				ID:     "ch-query-5",
				Type:   event.TypeChannelInfoQuery,
				Source: "spoofedSource",
				Target: "room1",
			},
		}
		if err := wsjson.Write(ctx, cA, queryEvt); err != nil {
			t.Fatalf("failed to write query event: %v", err)
		}

		var respEvt event.RawEvent
		if err := wsjson.Read(ctx, cA, &respEvt); err != nil {
			t.Fatalf("failed to read channel info error response: %v", err)
		}
		if respEvt.Type != event.TypeChannelInfoError {
			t.Fatalf("expected TypeChannelInfoError on spoofed source, got %s", respEvt.Type)
		}
		if respEvt.RefID != queryEvt.ID {
			t.Errorf("expected RefID %s, got %s", queryEvt.ID, respEvt.RefID)
		}
		if respEvt.ID == "" || respEvt.ID == queryEvt.ID {
			t.Errorf("expected unique response ID, got %s (query ID: %s)", respEvt.ID, queryEvt.ID)
		}
	}

	// Scenario 6: Omitted query ID (broker generates event ID and sets RefID on response)
	{
		queryEvt := &event.RawEvent{
			BaseEvent: event.BaseEvent{
				Type:   event.TypeChannelInfoQuery,
				Source: "clientA",
				Target: "room1",
			},
		}
		if err := wsjson.Write(ctx, cA, queryEvt); err != nil {
			t.Fatalf("failed to write query event: %v", err)
		}

		var respEvt event.RawEvent
		if err := wsjson.Read(ctx, cA, &respEvt); err != nil {
			t.Fatalf("failed to read channel info response: %v", err)
		}
		if respEvt.Type != event.TypeChannelInfoResponse {
			t.Fatalf("expected TypeChannelInfoResponse, got %s", respEvt.Type)
		}
		if respEvt.RefID == "" {
			t.Errorf("expected non-empty RefID from generated query ID")
		}
		if respEvt.ID == "" || respEvt.ID == respEvt.RefID {
			t.Errorf("expected unique response ID, got ID=%s RefID=%s", respEvt.ID, respEvt.RefID)
		}
	}
}

func TestDecoupledRESTAndWebSocketAuth(t *testing.T) {
	logger := zerolog.Nop()

	// Scenario 1: Distinct REST Bearer and WS Bearer tokens
	{
		restSecret := "rest-secret-token"
		wsSecret := "ws-secret-token"

		ctrl, err := NewController(":0", "http://localhost:8765", nil, "", true, &logger)
		if err != nil {
			t.Fatalf("failed to create controller: %v", err)
		}
		ctrl.SetRESTBearer(restSecret)
		ctrl.SetWSBearer(wsSecret)
		ctrl.SetWSAuthType("bearer")

		ctrl.broker.Start(2)
		defer ctrl.GracefulStop()

		server := httptest.NewServer(ctrl.router)
		defer server.Close()
		wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 1.a WebSocket dial with valid wsSecret succeeds
		wsConn, _, err := websocket.Dial(ctx, wsURL+"/ws/client1/rw?bearer="+wsSecret, nil)
		if err != nil {
			t.Fatalf("expected WS connection to succeed with wsSecret, got error: %v", err)
		}
		wsConn.Close(websocket.StatusNormalClosure, "")

		// 1.b WebSocket dial with restSecret fails (401)
		_, wsResp, err := websocket.Dial(ctx, wsURL+"/ws/client2/rw?bearer="+restSecret, nil)
		if err == nil {
			t.Fatalf("expected WS connection to fail with restSecret, but succeeded")
		}
		if wsResp != nil && wsResp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 for WS connection with restSecret, got %d", wsResp.StatusCode)
		}

		// 1.c REST POST /api/v1/publish with restSecret succeeds
		reqRestOk, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/publish", strings.NewReader(`{"target":"chan1","message":"hello"}`))
		reqRestOk.Header.Set("Content-Type", "application/json")
		reqRestOk.Header.Set("Authorization", "Bearer "+restSecret)
		respRestOk, err := http.DefaultClient.Do(reqRestOk)
		if err != nil {
			t.Fatalf("REST request with restSecret failed: %v", err)
		}
		if respRestOk.StatusCode != http.StatusOK {
			t.Errorf("expected 200 for REST with restSecret, got %d", respRestOk.StatusCode)
		}
		respRestOk.Body.Close()

		// 1.d REST POST /api/v1/publish with wsSecret fails (401)
		reqRestFail, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/publish", strings.NewReader(`{"target":"chan1","message":"hello"}`))
		reqRestFail.Header.Set("Content-Type", "application/json")
		reqRestFail.Header.Set("Authorization", "Bearer "+wsSecret)
		respRestFail, err := http.DefaultClient.Do(reqRestFail)
		if err != nil {
			t.Fatalf("REST request with wsSecret failed: %v", err)
		}
		if respRestFail.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 for REST with wsSecret, got %d", respRestFail.StatusCode)
		}
		respRestFail.Body.Close()
	}

	// Scenario 2: WebSocket MTLS mode combined with REST Bearer token
	{
		restSecret := "rest-only-secret"

		ctrl, err := NewController(":0", "http://localhost:8765", nil, "", true, &logger)
		if err != nil {
			t.Fatalf("failed to create controller: %v", err)
		}
		ctrl.SetRESTBearer(restSecret)
		ctrl.SetWSAuthType("mtls")

		ctrl.broker.Start(2)
		defer ctrl.GracefulStop()

		server := httptest.NewServer(ctrl.router)
		defer server.Close()
		wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 2.a REST endpoint works with REST Bearer
		reqRest, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/channels", nil)
		reqRest.Header.Set("Authorization", "Bearer "+restSecret)
		respRest, err := http.DefaultClient.Do(reqRest)
		if err != nil {
			t.Fatalf("REST request failed: %v", err)
		}
		if respRest.StatusCode != http.StatusOK {
			t.Errorf("expected 200 for REST with restSecret in MTLS mode, got %d", respRest.StatusCode)
		}
		respRest.Body.Close()

		// 2.b WebSocket connection without client cert fails (401) even if restSecret is provided
		_, wsResp, err := websocket.Dial(ctx, wsURL+"/ws/clientMTLS/rw?bearer="+restSecret, nil)
		if err == nil {
			t.Fatalf("expected WS connection to fail in MTLS mode without cert, but succeeded")
		}
		if wsResp != nil && wsResp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 for WS connection in MTLS mode without cert, got %d", wsResp.StatusCode)
		}
	}

	// Scenario 3: WebSocket 'both' mode with independent tokens
	{
		restSecret := "rest-both-secret"
		wsSecret := "ws-both-secret"

		ctrl, err := NewController(":0", "http://localhost:8765", nil, "", true, &logger)
		if err != nil {
			t.Fatalf("failed to create controller: %v", err)
		}
		ctrl.SetRESTBearer(restSecret)
		ctrl.SetWSBearer(wsSecret)
		ctrl.SetWSAuthType("both")

		ctrl.broker.Start(2)
		defer ctrl.GracefulStop()

		server := httptest.NewServer(ctrl.router)
		defer server.Close()
		wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 3.a WebSocket succeeds with wsSecret
		wsConn, _, err := websocket.Dial(ctx, wsURL+"/ws/clientBoth/rw?bearer="+wsSecret, nil)
		if err != nil {
			t.Fatalf("expected WS connection to succeed with wsSecret in 'both' mode, got error: %v", err)
		}
		wsConn.Close(websocket.StatusNormalClosure, "")

		// 3.b WebSocket fails with restSecret
		_, wsResp, err := websocket.Dial(ctx, wsURL+"/ws/clientBoth2/rw?bearer="+restSecret, nil)
		if err == nil {
			t.Fatalf("expected WS connection to fail with restSecret in 'both' mode without cert, but succeeded")
		}
		if wsResp != nil && wsResp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 for WS connection with restSecret in 'both' mode, got %d", wsResp.StatusCode)
		}
	}

	// Scenario 4: Backwards compatibility with legacy single bearer token
	{
		legacySecret := "common-legacy-secret"

		ctrl, err := NewController(":0", "http://localhost:8765", nil, legacySecret, true, &logger)
		if err != nil {
			t.Fatalf("failed to create controller: %v", err)
		}

		ctrl.broker.Start(2)
		defer ctrl.GracefulStop()

		server := httptest.NewServer(ctrl.router)
		defer server.Close()
		wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 4.a WebSocket succeeds with legacySecret
		wsConn, _, err := websocket.Dial(ctx, wsURL+"/ws/clientLegacy/rw?bearer="+legacySecret, nil)
		if err != nil {
			t.Fatalf("expected WS connection to succeed with legacySecret, got error: %v", err)
		}
		wsConn.Close(websocket.StatusNormalClosure, "")

		// 4.b REST succeeds with legacySecret
		reqRest, _ := http.NewRequest(http.MethodGet, server.URL+"/api/v1/channels", nil)
		reqRest.Header.Set("Authorization", "Bearer "+legacySecret)
		respRest, err := http.DefaultClient.Do(reqRest)
		if err != nil {
			t.Fatalf("REST request with legacySecret failed: %v", err)
		}
		if respRest.StatusCode != http.StatusOK {
			t.Errorf("expected 200 for REST with legacySecret, got %d", respRest.StatusCode)
		}
		respRest.Body.Close()
	}
}
