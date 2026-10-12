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

	var receivedClient1 = make(chan *event.Event, 10)
	var receivedClient2 = make(chan *event.Event, 10)
	var receivedClient3 = make(chan *event.Event, 10)

	var wg sync.WaitGroup
	startReader := func(c *websocket.Conn, ch chan *event.Event) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				var evt event.Event
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

	// Verify client1 and client2 receive "goal!"
	select {
	case evt := <-receivedClient1:
		d, _ := evt.GetData()
		if d != "goal!" {
			t.Errorf("client1 received wrong data: %v", d)
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
}
