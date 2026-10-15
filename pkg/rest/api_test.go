package rest

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

func TestSwaggerAndPing(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "secret-token"

	ctrl, err := NewController(":0", "http://localhost:8080/wsbroker", nil, bearer, false, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}

	// Test 1: GET /api/v1/ping (unauthenticated, should succeed without Authorization header)
	{
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/api/v1/ping", nil)
		ctrl.router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200 for ping, got %d: %s", w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "pong") {
			t.Errorf("expected ping response to contain 'pong', got '%s'", w.Body.String())
		}
	}

	// Test 2: GET /swagger/index.html
	{
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/swagger/index.html", nil)
		ctrl.router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200 for swagger index.html, got %d: %s", w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "WSBroker API") {
			t.Errorf("expected swagger UI to contain title 'WSBroker API', got '%s'", w.Body.String())
		}
	}

	// Test 3: GET /swagger/doc.json (OpenAPI 3.1 spec)
	{
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/swagger/doc.json", nil)
		ctrl.router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200 for swagger doc.json, got %d: %s", w.Code, w.Body.String())
		}

		var spec map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &spec); err != nil {
			t.Fatalf("failed to unmarshal swagger doc JSON: %v", err)
		}
		if openapi, ok := spec["openapi"].(string); !ok || !strings.HasPrefix(openapi, "3.1") {
			t.Errorf("expected openapi 3.1.x version, got %v", spec["openapi"])
		}

		servers, ok := spec["servers"].([]any)
		if !ok || len(servers) == 0 {
			t.Fatalf("expected servers array in swagger doc, got %v", spec["servers"])
		}
		serverObj, ok := servers[0].(map[string]any)
		if !ok {
			t.Fatalf("expected server object in servers array, got %v", servers[0])
		}
		if serverObj["url"] != "/wsbroker/api/v1" {
			t.Errorf("expected server url '/wsbroker/api/v1', got %v", serverObj["url"])
		}
	}

	// Test 4: Default base path without subpath (/api/v1)
	{
		ctrlDefault, err := NewController(":0", "https://localhost:8765", nil, bearer, false, &logger)
		if err != nil {
			t.Fatalf("failed to create default controller: %v", err)
		}

		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/swagger/doc.json", nil)
		ctrlDefault.router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200 for swagger doc.json, got %d: %s", w.Code, w.Body.String())
		}

		var spec map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &spec); err != nil {
			t.Fatalf("failed to unmarshal swagger doc JSON: %v", err)
		}
		servers, ok := spec["servers"].([]any)
		if !ok || len(servers) == 0 {
			t.Fatalf("expected servers array in swagger doc, got %v", spec["servers"])
		}
		serverObj, ok := servers[0].(map[string]any)
		if !ok {
			t.Fatalf("expected server object in servers array, got %v", servers[0])
		}
		if serverObj["url"] != "/api/v1" {
			t.Errorf("expected server url '/api/v1', got %v", serverObj["url"])
		}
	}

	// Test 5: Verify auth middleware on protected routes
	{
		// register a dummy protected endpoint on v1
		ctrl.router.Group("/api/v1").GET("/protected", func(c *gin.Context) {
			c.String(http.StatusOK, "secret-data")
		})

		// Unauthenticated request to /ping still succeeds
		wPing := httptest.NewRecorder()
		reqPing, _ := http.NewRequest(http.MethodGet, "/api/v1/ping", nil)
		ctrl.router.ServeHTTP(wPing, reqPing)
		if wPing.Code != http.StatusOK {
			t.Errorf("expected 200 for ping without auth, got %d", wPing.Code)
		}

		// Unauthenticated request to /api/v1/publish returns 401
		wPub := httptest.NewRecorder()
		reqPub, _ := http.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(`{"target":"test","message":"hi"}`))
		reqPub.Header.Set("Content-Type", "application/json")
		ctrl.router.ServeHTTP(wPub, reqPub)
		if wPub.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 for unauthenticated /api/v1/publish, got %d", wPub.Code)
		}

		// Authenticated request to /api/v1/protected succeeds with Bearer token
		wProt := httptest.NewRecorder()
		reqProt, _ := http.NewRequest(http.MethodGet, "/api/v1/protected", nil)
		reqProt.Header.Set("Authorization", "Bearer "+bearer)
		ctrl.router.ServeHTTP(wProt, reqProt)
		if wProt.Code != http.StatusOK || wProt.Body.String() != "secret-data" {
			t.Errorf("expected 200 secret-data for authenticated /api/v1/protected, got %d: %s", wProt.Code, wProt.Body.String())
		}
	}
}
