package rest

import (
	"context"
	"io"
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
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	html := string(body)

	if !strings.Contains(html, "https://cdn.tailwindcss.com") {
		t.Errorf("expected dashboard HTML to include Tailwind CDN")
	}
	if !strings.Contains(html, "https://unpkg.com/htmx.org@2.0.11") {
		t.Errorf("expected dashboard HTML to include HTMX script")
	}
	if !strings.Contains(html, "WSBroker") || !strings.Contains(html, "Dashboard") {
		t.Errorf("expected dashboard HTML to contain title")
	}
	if !strings.Contains(html, "prefers-color-scheme: dark") || !strings.Contains(html, "classList.add('dark')") {
		t.Errorf("expected dashboard HTML to contain theme synchronization script")
	}
	if !strings.Contains(html, "dark:bg-slate-950") {
		t.Errorf("expected dashboard HTML to contain dark mode background utility classes")
	}

	// 2. GET /dashboard/stats
	respStats, err := http.Get(server.URL + "/dashboard/stats")
	if err != nil {
		t.Fatalf("GET /dashboard/stats failed: %v", err)
	}
	if respStats.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", respStats.StatusCode)
	}
	statsBody, err := io.ReadAll(respStats.Body)
	respStats.Body.Close()
	if err != nil {
		t.Fatalf("failed to read stats body: %v", err)
	}
	statsHTML := string(statsBody)
	if !strings.Contains(statsHTML, "bg-white") || !strings.Contains(statsHTML, "dark:bg-slate-900") {
		t.Errorf("expected stats partial to contain dual light/dark background classes, got: %s", statsHTML)
	}

	// 3. GET /dashboard/connections
	respConn, err := http.Get(server.URL + "/dashboard/connections")
	if err != nil {
		t.Fatalf("GET /dashboard/connections failed: %v", err)
	}
	if respConn.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", respConn.StatusCode)
	}
	connBody, err := io.ReadAll(respConn.Body)
	respConn.Body.Close()
	if err != nil {
		t.Fatalf("failed to read connections body: %v", err)
	}
	connHTML := string(connBody)
	if !strings.Contains(connHTML, "dark:text-slate-400") || !strings.Contains(connHTML, "dark:bg-slate-950") {
		t.Errorf("expected connections partial to contain dual-theme classes, got: %s", connHTML)
	}

	// 4. GET /dashboard/channels
	respCh, err := http.Get(server.URL + "/dashboard/channels")
	if err != nil {
		t.Fatalf("GET /dashboard/channels failed: %v", err)
	}
	if respCh.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", respCh.StatusCode)
	}
	chBody, err := io.ReadAll(respCh.Body)
	respCh.Body.Close()
	if err != nil {
		t.Fatalf("failed to read channels body: %v", err)
	}
	chHTML := string(chBody)
	if !strings.Contains(chHTML, "dark:text-slate-400") || !strings.Contains(chHTML, "dark:bg-slate-950") {
		t.Errorf("expected channels partial to contain dual-theme classes, got: %s", chHTML)
	}
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
	connA, _, err := websocket.Dial(ctx, wsURL+"/ws/client-dash-A/rw?channel=general", nil)
	if err != nil {
		t.Fatalf("failed to dial clientA: %v", err)
	}
	defer connA.Close(websocket.StatusNormalClosure, "")

	// Connect Client B subscribed to channel 'alerts'
	connB, _, err := websocket.Dial(ctx, wsURL+"/ws/client-dash-B/rw?channel=alerts", nil)
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
	connBytes, err := io.ReadAll(respConn.Body)
	respConn.Body.Close()
	if err != nil {
		t.Fatalf("failed to read connections body: %v", err)
	}
	connHTML := string(connBytes)

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
	chBytes, err := io.ReadAll(respCh.Body)
	respCh.Body.Close()
	if err != nil {
		t.Fatalf("failed to read channels body: %v", err)
	}
	chHTML := string(chBytes)

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
	pubBytes, err := io.ReadAll(respPub.Body)
	respPub.Body.Close()
	if err != nil {
		t.Fatalf("failed to read publish body: %v", err)
	}
	pubHTML := string(pubBytes)

	if !strings.Contains(pubHTML, "Message sent successfully") {
		t.Errorf("expected publish response to indicate success, got: %s", pubHTML)
	}

	// Verify Client A receives the broadcast
	var receivedA event.RawEvent
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
	delBytes, err := io.ReadAll(respDel.Body)
	respDel.Body.Close()
	if err != nil {
		t.Fatalf("failed to read disconnect body: %v", err)
	}
	delHTML := string(delBytes)

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

func TestDashboardAuthentication(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "test-bearer-secret"
	ctrl, err := NewController("localhost:0", "http://localhost:8080", nil, bearer, true, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	ctrl.SetDashboardAuth("admin", "secret-password", true)

	server := httptest.NewServer(ctrl.router)
	defer server.Close()

	// 1. Unauthenticated requests must return 401 Unauthorized with WWW-Authenticate header and Security Headers
	endpoints := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/dashboard"},
		{http.MethodGet, "/dashboard/stats"},
		{http.MethodGet, "/dashboard/connections"},
		{http.MethodGet, "/dashboard/channels"},
		{http.MethodPost, "/dashboard/publish"},
		{http.MethodDelete, "/dashboard/connections/dummy-client"},
		{http.MethodPost, "/dashboard/connections/dummy-client/disconnect"},
	}

	for _, ep := range endpoints {
		req, _ := http.NewRequest(ep.method, server.URL+ep.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s failed: %v", ep.method, ep.path, err)
		}
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 for unauthenticated %s %s, got %d", ep.method, ep.path, resp.StatusCode)
		}
		if authHeader := resp.Header.Get("WWW-Authenticate"); !strings.Contains(authHeader, `Basic realm="WSBroker Dashboard"`) {
			t.Errorf("expected WWW-Authenticate header for %s %s, got '%s'", ep.method, ep.path, authHeader)
		}
		if xfo := resp.Header.Get("X-Frame-Options"); xfo != "DENY" {
			t.Errorf("expected X-Frame-Options: DENY for %s %s, got '%s'", ep.method, ep.path, xfo)
		}
		if xcto := resp.Header.Get("X-Content-Type-Options"); xcto != "nosniff" {
			t.Errorf("expected X-Content-Type-Options: nosniff for %s %s, got '%s'", ep.method, ep.path, xcto)
		}
		if csp := resp.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'self'") {
			t.Errorf("expected Content-Security-Policy header for %s %s, got '%s'", ep.method, ep.path, csp)
		}
		resp.Body.Close()
	}

	// 2. Invalid Basic Auth credentials must return 401
	{
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/dashboard", nil)
		req.SetBasicAuth("admin", "wrong-password")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET /dashboard with wrong credentials failed: %v", err)
		}
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected 401 with wrong password, got %d", resp.StatusCode)
		}
		resp.Body.Close()
	}

	// 3. Valid Basic Auth credentials must return 200 OK
	{
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/dashboard", nil)
		req.SetBasicAuth("admin", "secret-password")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET /dashboard with valid credentials failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200 with valid basic auth, got %d", resp.StatusCode)
		}
		resp.Body.Close()

		// Test HTMX partial /dashboard/stats with Basic Auth
		reqStats, _ := http.NewRequest(http.MethodGet, server.URL+"/dashboard/stats", nil)
		reqStats.SetBasicAuth("admin", "secret-password")
		respStats, err := http.DefaultClient.Do(reqStats)
		if err != nil {
			t.Fatalf("GET /dashboard/stats failed: %v", err)
		}
		if respStats.StatusCode != http.StatusOK {
			t.Errorf("expected 200 for /dashboard/stats with valid basic auth, got %d", respStats.StatusCode)
		}
		respStats.Body.Close()
	}

	// 4. Valid Bearer Token fallback (Authorization header and Query param)
	{
		reqBearer, _ := http.NewRequest(http.MethodGet, server.URL+"/dashboard", nil)
		reqBearer.Header.Set("Authorization", "Bearer "+bearer)
		respBearer, err := http.DefaultClient.Do(reqBearer)
		if err != nil {
			t.Fatalf("GET /dashboard with bearer token failed: %v", err)
		}
		if respBearer.StatusCode != http.StatusOK {
			t.Errorf("expected 200 with bearer token header, got %d", respBearer.StatusCode)
		}
		respBearer.Body.Close()

		reqQuery, _ := http.NewRequest(http.MethodGet, server.URL+"/dashboard?token="+bearer, nil)
		respQuery, err := http.DefaultClient.Do(reqQuery)
		if err != nil {
			t.Fatalf("GET /dashboard with token query failed: %v", err)
		}
		if respQuery.StatusCode != http.StatusOK {
			t.Errorf("expected 200 with token query param, got %d", respQuery.StatusCode)
		}
		respQuery.Body.Close()
	}

	// 5. CSRF / Origin Validation for state-modifying requests
	{
		// Malicious Cross-Origin POST request must return 403 Forbidden
		form := url.Values{}
		form.Set("target", "general")
		form.Set("message", "cross-site attack")

		reqAttack, _ := http.NewRequest(http.MethodPost, server.URL+"/dashboard/publish", strings.NewReader(form.Encode()))
		reqAttack.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		reqAttack.Header.Set("Origin", "https://attacker.evil.com")
		reqAttack.SetBasicAuth("admin", "secret-password")

		respAttack, err := http.DefaultClient.Do(reqAttack)
		if err != nil {
			t.Fatalf("POST /dashboard/publish cross-origin failed: %v", err)
		}
		if respAttack.StatusCode != http.StatusForbidden {
			t.Errorf("expected 403 Forbidden for cross-origin POST, got %d", respAttack.StatusCode)
		}
		respAttack.Body.Close()
	}
}
