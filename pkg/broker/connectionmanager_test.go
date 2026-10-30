package broker

import (
	"context"
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

func TestConnectionManagerGroupsAndBroadcasting(t *testing.T) {
	logger := zerolog.Nop()
	cm := NewConnectionManager(false, &logger)
	cm.Start(4)
	defer cm.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientName := r.URL.Query().Get("name")
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
			OriginPatterns:     []string{"*"},
		})
		if err != nil {
			t.Errorf("accept failed: %v", err)
			return
		}
		conn := NewConnection(ws, clientName, false)
		if err := cm.AddWSConn(conn); err != nil {
			t.Errorf("addWSConn failed: %v", err)
			return
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c1, _, err := websocket.Dial(ctx, wsURL+"?name=client1", nil)
	if err != nil {
		t.Fatalf("client1 dial failed: %v", err)
	}
	defer c1.Close(websocket.StatusNormalClosure, "")

	c2, _, err := websocket.Dial(ctx, wsURL+"?name=client2", nil)
	if err != nil {
		t.Fatalf("client2 dial failed: %v", err)
	}
	defer c2.Close(websocket.StatusNormalClosure, "")

	c3, _, err := websocket.Dial(ctx, wsURL+"?name=client3", nil)
	if err != nil {
		t.Fatalf("client3 dial failed: %v", err)
	}
	defer c3.Close(websocket.StatusNormalClosure, "")

	var receivedClient1 = make(chan *event.RawEvent, 10)
	var receivedClient2 = make(chan *event.RawEvent, 10)
	var receivedClient3 = make(chan *event.RawEvent, 10)

	var wg sync.WaitGroup
	startReader := func(c *websocket.Conn, ch chan *event.RawEvent) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				var evt event.RawEvent
				if err := wsjson.Read(ctx, c, &evt); err != nil {
					break
				}
				ch <- &evt
			}
		}()
	}

	startReader(c1, receivedClient1)
	startReader(c2, receivedClient2)
	startReader(c3, receivedClient3)

	time.Sleep(50 * time.Millisecond)

	// Add client1 and client2 to channel "sports"
	cm.AddToGroup("client1", "sports")
	cm.AddToGroup("client2", "sports")
	// Add client2 and client3 to channel "news"
	cm.AddToGroup("client2", "news")
	cm.AddToGroup("client3", "news")

	channels := cm.GetChannels()
	if len(channels) != 2 {
		t.Errorf("expected 2 channels, got %v", channels)
	}

	sportsMembers := cm.GetChannelMembers("sports")
	if len(sportsMembers) != 2 {
		t.Errorf("expected 2 sports members, got %v", sportsMembers)
	}

	// Send message to "sports" channel
	sportsEvt, _ := event.NewMessageEvent("broadcaster", "sports", "goal!")
	if err := cm.Send(sportsEvt); err != nil {
		t.Fatalf("failed to send sports event: %v", err)
	}

	// Verify client1 and client2 receive "goal!" with event ID and channel name
	select {
	case evt := <-receivedClient1:
		d, _ := evt.GetData()
		if d != "goal!" {
			t.Errorf("client1 received wrong data: %v", d)
		}
		if evt.ID == "" {
			t.Errorf("client1 expected non-empty event ID")
		}
		if evt.Channel != "sports" {
			t.Errorf("client1 expected channel 'sports', got %q", evt.Channel)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("client1 timed out waiting for sports message")
	}

	select {
	case evt := <-receivedClient2:
		d, _ := evt.GetData()
		if d != "goal!" {
			t.Errorf("client2 received wrong data: %v", d)
		}
		if evt.ID == "" {
			t.Errorf("client2 expected non-empty event ID")
		}
		if evt.Channel != "sports" {
			t.Errorf("client2 expected channel 'sports', got %q", evt.Channel)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("client2 timed out waiting for sports message")
	}

	// Verify client3 did NOT receive sports message
	select {
	case evt := <-receivedClient3:
		t.Errorf("client3 should not have received sports message, got: %+v", evt)
	case <-time.After(100 * time.Millisecond):
		// expected
	}

	// Send message to "news" channel
	newsEvt, _ := event.NewMessageEvent("broadcaster", "news", "breaking news!")
	if err := cm.Send(newsEvt); err != nil {
		t.Fatalf("failed to send news event: %v", err)
	}

	// Verify client2 and client3 receive "breaking news!"
	select {
	case evt := <-receivedClient2:
		d, _ := evt.GetData()
		if d != "breaking news!" {
			t.Errorf("client2 received wrong data: %v", d)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("client2 timed out waiting for news message")
	}

	select {
	case evt := <-receivedClient3:
		d, _ := evt.GetData()
		if d != "breaking news!" {
			t.Errorf("client3 received wrong data: %v", d)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("client3 timed out waiting for news message")
	}

	// Test remove from group
	cm.RemoveFromGroup("client1", "sports")
	if len(cm.GetChannelMembers("sports")) != 1 {
		t.Errorf("expected 1 sports member after removal, got %v", cm.GetChannelMembers("sports"))
	}

	// Test Direct Point-to-Point message with custom ID preserves ID and keeps Channel empty
	directEvt, _ := event.NewMessageEvent("client1", "client3", "hello direct")
	directEvt.SetID("custom-direct-id-999")
	if err := cm.Send(directEvt); err != nil {
		t.Fatalf("failed to send direct event: %v", err)
	}

	select {
	case evt := <-receivedClient3:
		d, _ := evt.GetData()
		if d != "hello direct" {
			t.Errorf("client3 received wrong direct data: %v", d)
		}
		if evt.ID != "custom-direct-id-999" {
			t.Errorf("expected preserved event ID 'custom-direct-id-999', got %q", evt.ID)
		}
		if evt.Channel != "" {
			t.Errorf("expected empty channel for direct point-to-point event, got %q", evt.Channel)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("client3 timed out waiting for direct message")
	}
}

func TestParseConnectionMode(t *testing.T) {
	tests := []struct {
		input    string
		expected ConnectionMode
		wantErr  bool
	}{
		{"rw", ModeRW, false},
		{"RW", ModeRW, false},
		{"readwrite", ModeRW, false},
		{"ro", ModeRO, false},
		{"RO", ModeRO, false},
		{"readonly", ModeRO, false},
		{"wo", ModeWO, false},
		{"WO", ModeWO, false},
		{"w", ModeWO, false},
		{"W", ModeWO, false},
		{"writeonly", ModeWO, false},
		{"invalid", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		mode, err := ParseConnectionMode(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseConnectionMode(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if mode != tt.expected {
			t.Errorf("ParseConnectionMode(%q) = %v, want %v", tt.input, mode, tt.expected)
		}
	}
}

func TestConnectionModePermissions(t *testing.T) {
	if !ModeRW.CanRead() || !ModeRW.CanWrite() {
		t.Errorf("ModeRW should allow both read and write")
	}
	if !ModeRO.CanRead() || ModeRO.CanWrite() {
		t.Errorf("ModeRO should allow read only")
	}
	if ModeWO.CanRead() || !ModeWO.CanWrite() {
		t.Errorf("ModeWO should allow write only")
	}
}

func TestConnectionManagerWriteOnlySuppression(t *testing.T) {
	logger := zerolog.Nop()
	cm := NewConnectionManager(false, &logger)
	cm.Start(4)
	defer cm.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientName := r.URL.Query().Get("name")
		modeStr := r.URL.Query().Get("mode")
		mode, _ := ParseConnectionMode(modeStr)
		ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
			OriginPatterns:     []string{"*"},
		})
		if err != nil {
			t.Errorf("accept failed: %v", err)
			return
		}
		conn := NewConnectionWithMode(ws, clientName, false, mode)
		if err := cm.AddWSConn(conn); err != nil {
			t.Errorf("addWSConn failed: %v", err)
			return
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cRO, _, err := websocket.Dial(ctx, wsURL+"?name=reader&mode=ro", nil)
	if err != nil {
		t.Fatalf("reader dial failed: %v", err)
	}
	defer cRO.Close(websocket.StatusNormalClosure, "")

	cWO, _, err := websocket.Dial(ctx, wsURL+"?name=writer&mode=wo", nil)
	if err != nil {
		t.Fatalf("writer dial failed: %v", err)
	}
	defer cWO.Close(websocket.StatusNormalClosure, "")

	cRW, _, err := websocket.Dial(ctx, wsURL+"?name=duplex&mode=rw", nil)
	if err != nil {
		t.Fatalf("duplex dial failed: %v", err)
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

	// Add all three to channel "updates"
	cm.AddToGroup("reader", "updates")
	cm.AddToGroup("writer", "updates")
	cm.AddToGroup("duplex", "updates")

	// Send message to "updates" channel
	channelEvt, _ := event.NewMessageEvent("source", "updates", "broadcast message")
	if err := cm.Send(channelEvt); err != nil {
		t.Fatalf("failed to send channel event: %v", err)
	}

	// Verify reader receives message
	select {
	case evt := <-receivedRO:
		d, _ := evt.GetData()
		if d != "broadcast message" {
			t.Errorf("reader received wrong data: %v", d)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("reader timed out waiting for channel message")
	}

	// Verify duplex receives message
	select {
	case evt := <-receivedRW:
		d, _ := evt.GetData()
		if d != "broadcast message" {
			t.Errorf("duplex received wrong data: %v", d)
		}
	case <-time.After(1 * time.Second):
		t.Errorf("duplex timed out waiting for channel message")
	}

	// Verify writer (wo) does NOT receive broadcast message
	select {
	case evt := <-receivedWO:
		t.Errorf("writer should NOT receive broadcast message, got: %+v", evt)
	case <-time.After(150 * time.Millisecond):
		// Expected
	}

	// Direct message to writer (wo) should also be suppressed
	directEvt, _ := event.NewMessageEvent("source", "writer", "direct to writer")
	if err := cm.Send(directEvt); err != nil {
		t.Fatalf("failed to send direct event: %v", err)
	}

	select {
	case evt := <-receivedWO:
		t.Errorf("writer should NOT receive direct message, got: %+v", evt)
	case <-time.After(150 * time.Millisecond):
		// Expected
	}
}
