package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/je4/wsbrokerclient/pkg/event"
	"github.com/rs/zerolog"
)

func TestDashboardPageAndPartials(t *testing.T) {
	logger := zerolog.Nop()
	ctrl, err := NewController("localhost:0", "http://localhost:8080", nil, "", true, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}

	server := httptest.NewServer(ctrl.router)
	defer server.Close()

	// 1. GET /dashboard
	resp, err := http.Get(server.URL + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body := make([]byte, 100000)
	n, _ := resp.Body.Read(body)
	resp.Body.Close()
	html := string(body[:n])

	if !strings.Contains(html, "https://cdn.tailwindcss.com") {
		t.Errorf("expected dashboard HTML to include Tailwind CDN")
	}
	if !strings.Contains(html, "https://unpkg.com/htmx.org@2.0.11") {
		t.Errorf("expected dashboard HTML to include HTMX script")
	}
	if !strings.Contains(html, "WSBroker") || !strings.Contains(html, "Dashboard") {
		t.Errorf("expected dashboard HTML to contain title")
	}

	// 2. GET /dashboard/stats
	respStats, err := http.Get(server.URL + "/dashboard/stats")
	if err != nil {
		t.Fatalf("GET /dashboard/stats failed: %v", err)
	}
	if respStats.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", respStats.StatusCode)
	}
	respStats.Body.Close()

	// 3. GET /dashboard/connections
	respConn, err := http.Get(server.URL + "/dashboard/connections")
	if err != nil {
		t.Fatalf("GET /dashboard/connections failed: %v", err)
	}
	if respConn.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", respConn.StatusCode)
	}
	respConn.Body.Close()

	// 4. GET /dashboard/channels
	respCh, err := http.Get(server.URL + "/dashboard/channels")
	if err != nil {
		t.Fatalf("GET /dashboard/channels failed: %v", err)
	}
	if respCh.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", respCh.StatusCode)
	}
	respCh.Body.Close()
}

func TestDashboardConnectionManagementAndPublish(t *testing.T) {
	logger := zerolog.Nop()
	ctrl, err := NewController("localhost:0", "http://localhost:8080", nil, "", true, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	ctrl.broker.Start(2)
	defer ctrl.GracefulStop()

	server := httptest.NewServer(ctrl.router)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect Client A subscribed to channel 'general'
	connA, _, err := websocket.Dial(ctx, wsURL+"/ws/client-dash-A?channel=general", nil)
	if err != nil {
		t.Fatalf("failed to dial clientA: %v", err)
	}
	defer connA.Close(websocket.StatusNormalClosure, "")

	// Connect Client B subscribed to channel 'alerts'
	connB, _, err := websocket.Dial(ctx, wsURL+"/ws/client-dash-B?channel=alerts", nil)
	if err != nil {
		t.Fatalf("failed to dial clientB: %v", err)
	}
	defer connB.Close(websocket.StatusNormalClosure, "")

	time.Sleep(50 * time.Millisecond)

	// Check /dashboard/connections contains client-dash-A and client-dash-B
	respConn, err := http.Get(server.URL + "/dashboard/connections")
	if err != nil {
		t.Fatalf("GET /dashboard/connections failed: %v", err)
	}
	buf := make([]byte, 100000)
	n, _ := respConn.Body.Read(buf)
	respConn.Body.Close()
	connHTML := string(buf[:n])

	if !strings.Contains(connHTML, "client-dash-A") || !strings.Contains(connHTML, "client-dash-B") {
		t.Errorf("expected connections HTML to contain client-dash-A and client-dash-B, got: %s", connHTML)
	}
	if !strings.Contains(connHTML, "#general") || !strings.Contains(connHTML, "#alerts") {
		t.Errorf("expected connections HTML to contain channel tags, got: %s", connHTML)
	}

	// Check /dashboard/channels contains general and alerts
	respCh, err := http.Get(server.URL + "/dashboard/channels")
	if err != nil {
		t.Fatalf("GET /dashboard/channels failed: %v", err)
	}
	n, _ = respCh.Body.Read(buf)
	respCh.Body.Close()
	chHTML := string(buf[:n])

	if !strings.Contains(chHTML, "general") || !strings.Contains(chHTML, "alerts") {
		t.Errorf("expected channels HTML to contain channels, got: %s", chHTML)
	}

	// Publish message to channel 'general' via POST /dashboard/publish
	form := url.Values{}
	form.Set("target", "general")
	form.Set("type", "message")
	form.Set("source", "dashboard-tester")
	form.Set("message", "Hello everyone from Dashboard!")

	respPub, err := http.Post(server.URL+"/dashboard/publish", "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("POST /dashboard/publish failed: %v", err)
	}
	if respPub.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from publish, got %d", respPub.StatusCode)
	}
	n, _ = respPub.Body.Read(buf)
	respPub.Body.Close()
	pubHTML := string(buf[:n])

	if !strings.Contains(pubHTML, "Message sent successfully") {
		t.Errorf("expected publish response to indicate success, got: %s", pubHTML)
	}

	// Verify Client A receives the broadcast
	var receivedA event.Event
	readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
	if err := wsjson.Read(readCtx, connA, &receivedA); err != nil {
		t.Errorf("clientA failed to receive published message: %v", err)
	} else {
		data, _ := receivedA.GetData()
		if data != "Hello everyone from Dashboard!" {
			t.Errorf("expected 'Hello everyone from Dashboard!', got %v", data)
		}
	}
	readCancel()

	// Disconnect Client A via DELETE /dashboard/connections/client-dash-A
	reqDel, _ := http.NewRequest(http.MethodDelete, server.URL+"/dashboard/connections/client-dash-A", nil)
	respDel, err := http.DefaultClient.Do(reqDel)
	if err != nil {
		t.Fatalf("DELETE /dashboard/connections/client-dash-A failed: %v", err)
	}
	if respDel.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from disconnect, got %d", respDel.StatusCode)
	}
	n, _ = respDel.Body.Read(buf)
	respDel.Body.Close()
	delHTML := string(buf[:n])

	if strings.Contains(delHTML, "client-dash-A") {
		t.Errorf("expected client-dash-A to be removed after disconnect, got: %s", delHTML)
	}

	time.Sleep(50 * time.Millisecond)

	// Verify client-dash-A is no longer in broker
	clients := ctrl.broker.GetClients()
	for _, c := range clients {
		if c == "client-dash-A" {
			t.Errorf("client-dash-A still registered in broker")
		}
	}

	// Disconnect Client B via REST DELETE /api/v1/client/client-dash-B
	reqRestDel, _ := http.NewRequest(http.MethodDelete, server.URL+"/api/v1/client/client-dash-B", nil)
	respRestDel, err := http.DefaultClient.Do(reqRestDel)
	if err != nil {
		t.Fatalf("REST DELETE /api/v1/client/client-dash-B failed: %v", err)
	}
	if respRestDel.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from REST disconnect, got %d", respRestDel.StatusCode)
	}
	respRestDel.Body.Close()
}

func TestDashboardDisabled(t *testing.T) {
	logger := zerolog.Nop()
	ctrl, err := NewController("localhost:0", "http://localhost:8080", nil, "", false, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}

	server := httptest.NewServer(ctrl.router)
	defer server.Close()

	// Endpoints that must not exist when dashboard is false
	endpoints := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/dashboard"},
		{http.MethodGet, "/dashboard/stats"},
		{http.MethodGet, "/dashboard/connections"},
		{http.MethodGet, "/dashboard/channels"},
		{http.MethodPost, "/dashboard/publish"},
		{http.MethodGet, "/api/v1/channels"},
		{http.MethodGet, "/api/v1/clients"},
		{http.MethodDelete, "/api/v1/client/test"},
	}

	for _, ep := range endpoints {
		req, _ := http.NewRequest(ep.method, server.URL+ep.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s failed: %v", ep.method, ep.path, err)
		}
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected 404 for %s %s when dashboard is disabled, got %d", ep.method, ep.path, resp.StatusCode)
		}
		resp.Body.Close()
	}
}
