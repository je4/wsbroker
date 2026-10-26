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

func TestRestPublishRefIDCorrelation(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "test-secret"

	ctrl, err := NewController(":0", "http://localhost:8080", nil, bearer, false, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	ctrl.broker.Start(4)
	defer ctrl.GracefulStop()

	// 1. POST /api/v1/publish with explicit id and ref_id
	{
		body := `{"id":"evt-custom-1","ref_id":"evt-parent-1","target":"alerts","type":"alert","message":"warning"}`
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+bearer)
		ctrl.router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp PublishResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if resp.ID != "evt-custom-1" {
			t.Errorf("expected ID 'evt-custom-1', got '%s'", resp.ID)
		}
		if resp.RefID != "evt-parent-1" {
			t.Errorf("expected RefID 'evt-parent-1', got '%s'", resp.RefID)
		}
		if resp.Target != "alerts" {
			t.Errorf("expected target 'alerts', got '%s'", resp.Target)
		}
	}

	// 2. POST /api/v1/publish without id (auto-generate id, keep ref_id)
	{
		body := `{"ref_id":"evt-parent-2","target":"alerts","message":"info"}`
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+bearer)
		ctrl.router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp PublishResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if resp.ID == "" {
			t.Errorf("expected auto-generated non-empty ID, got empty")
		}
		if resp.RefID != "evt-parent-2" {
			t.Errorf("expected RefID 'evt-parent-2', got '%s'", resp.RefID)
		}
	}

	// 3. POST /api/v1/channel/:channel/publish with explicit id and ref_id
	{
		body := `{"id":"evt-custom-chan-1","ref_id":"evt-parent-chan-1","message":"chan warning"}`
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/api/v1/channel/alerts/publish", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+bearer)
		ctrl.router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp PublishResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if resp.ID != "evt-custom-chan-1" {
			t.Errorf("expected ID 'evt-custom-chan-1', got '%s'", resp.ID)
		}
		if resp.RefID != "evt-parent-chan-1" {
			t.Errorf("expected RefID 'evt-parent-chan-1', got '%s'", resp.RefID)
		}
		if resp.Target != "alerts" {
			t.Errorf("expected target 'alerts', got '%s'", resp.Target)
		}
	}

	// 4. POST /api/v1/channel/:channel/publish without id (auto-generate id, keep ref_id)
	{
		body := `{"ref_id":"evt-parent-chan-2","message":"chan info"}`
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/api/v1/channel/alerts/publish", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+bearer)
		ctrl.router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp PublishResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to unmarshal response: %v", err)
		}
		if resp.ID == "" {
			t.Errorf("expected auto-generated non-empty ID, got empty")
		}
		if resp.RefID != "evt-parent-chan-2" {
			t.Errorf("expected RefID 'evt-parent-chan-2', got '%s'", resp.RefID)
		}
	}
}

func TestRestBearerAuthenticationSuite(t *testing.T) {
	logger := zerolog.Nop()
	validToken := "secure-api-key-12345"

	ctrl, err := NewController(":0", "http://localhost:8080", nil, validToken, true, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	ctrl.broker.Start(2)
	defer ctrl.GracefulStop()

	// 1. Public Endpoint Exemptions (should return 200 without credentials)
	{
		// GET /api/v1/ping
		wPing := httptest.NewRecorder()
		reqPing, _ := http.NewRequest(http.MethodGet, "/api/v1/ping", nil)
		ctrl.router.ServeHTTP(wPing, reqPing)
		if wPing.Code != http.StatusOK || !strings.Contains(wPing.Body.String(), "pong") {
			t.Errorf("expected 200 'pong' for public ping, got %d: %s", wPing.Code, wPing.Body.String())
		}

		// GET /swagger/index.html
		wSwag := httptest.NewRecorder()
		reqSwag, _ := http.NewRequest(http.MethodGet, "/swagger/index.html", nil)
		ctrl.router.ServeHTTP(wSwag, reqSwag)
		if wSwag.Code != http.StatusOK {
			t.Errorf("expected 200 for public swagger ui, got %d", wSwag.Code)
		}

		// GET /swagger/doc.json
		wDoc := httptest.NewRecorder()
		reqDoc, _ := http.NewRequest(http.MethodGet, "/swagger/doc.json", nil)
		ctrl.router.ServeHTTP(wDoc, reqDoc)
		if wDoc.Code != http.StatusOK {
			t.Errorf("expected 200 for public swagger doc, got %d", wDoc.Code)
		}
	}

	// 2. Missing Authentication on protected endpoints (should return 401 JSON)
	protectedRequests := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/api/v1/publish", `{"target":"test","message":"hello"}`},
		{http.MethodPost, "/api/v1/channel/testchan/publish", `{"message":"hello"}`},
		{http.MethodGet, "/api/v1/channels", ""},
		{http.MethodGet, "/api/v1/channel/testchan/clients", ""},
		{http.MethodGet, "/api/v1/clients", ""},
		{http.MethodDelete, "/api/v1/client/testclient", ""},
		{http.MethodDelete, "/api/v1/clients/testclient", ""},
	}

	for _, tc := range protectedRequests {
		w := httptest.NewRecorder()
		var bodyReader *strings.Reader
		if tc.body != "" {
			bodyReader = strings.NewReader(tc.body)
		} else {
			bodyReader = strings.NewReader("")
		}
		req, _ := http.NewRequest(tc.method, tc.path, bodyReader)
		if tc.body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		ctrl.router.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 for missing auth on %s %s, got %d: %s", tc.method, tc.path, w.Code, w.Body.String())
		}

		var resMsg HTTPResultMessage
		if err := json.Unmarshal(w.Body.Bytes(), &resMsg); err != nil {
			t.Errorf("failed to unmarshal 401 response for %s %s: %v", tc.method, tc.path, err)
		} else if resMsg.Code != http.StatusUnauthorized || resMsg.Message != "invalid bearer token" {
			t.Errorf("expected 401 'invalid bearer token' on %s %s, got: %+v", tc.method, tc.path, resMsg)
		}
	}

	// 3. Invalid Authentication (wrong token in header, ?token, ?bearer)
	invalidAuthCases := []struct {
		name       string
		authHeader string
		query      string
	}{
		{"wrong bearer header", "Bearer wrong-token", ""},
		{"malformed auth header", "Basic dXNlcjpwYXNz", ""},
		{"wrong query token", "", "?token=wrong-token"},
		{"wrong query bearer", "", "?bearer=wrong-token"},
	}

	for _, tc := range invalidAuthCases {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/api/v1/channels"+tc.query, nil)
		if tc.authHeader != "" {
			req.Header.Set("Authorization", tc.authHeader)
		}
		ctrl.router.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 for invalid auth (%s), got %d: %s", tc.name, w.Code, w.Body.String())
		}
	}

	// 4. Valid Authentication via Header
	{
		// POST /api/v1/publish
		wPub := httptest.NewRecorder()
		reqPub, _ := http.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(`{"target":"chan1","message":"test"}`))
		reqPub.Header.Set("Content-Type", "application/json")
		reqPub.Header.Set("Authorization", "Bearer "+validToken)
		ctrl.router.ServeHTTP(wPub, reqPub)
		if wPub.Code != http.StatusOK {
			t.Errorf("expected 200 for valid header on POST /api/v1/publish, got %d: %s", wPub.Code, wPub.Body.String())
		}

		// GET /api/v1/channels
		wChan := httptest.NewRecorder()
		reqChan, _ := http.NewRequest(http.MethodGet, "/api/v1/channels", nil)
		reqChan.Header.Set("Authorization", "Bearer "+validToken)
		ctrl.router.ServeHTTP(wChan, reqChan)
		if wChan.Code != http.StatusOK {
			t.Errorf("expected 200 for valid header on GET /api/v1/channels, got %d: %s", wChan.Code, wChan.Body.String())
		}

		// GET /api/v1/clients
		wCli := httptest.NewRecorder()
		reqCli, _ := http.NewRequest(http.MethodGet, "/api/v1/clients", nil)
		reqCli.Header.Set("Authorization", "Bearer "+validToken)
		ctrl.router.ServeHTTP(wCli, reqCli)
		if wCli.Code != http.StatusOK {
			t.Errorf("expected 200 for valid header on GET /api/v1/clients, got %d: %s", wCli.Code, wCli.Body.String())
		}
	}

	// 5. Valid Authentication via Query Parameters (?token= and ?bearer=)
	{
		// POST /api/v1/channel/:channel/publish?token=...
		wToken := httptest.NewRecorder()
		reqToken, _ := http.NewRequest(http.MethodPost, "/api/v1/channel/alerts/publish?token="+validToken, strings.NewReader(`{"message":"test token"}`))
		reqToken.Header.Set("Content-Type", "application/json")
		ctrl.router.ServeHTTP(wToken, reqToken)
		if wToken.Code != http.StatusOK {
			t.Errorf("expected 200 for valid ?token= parameter, got %d: %s", wToken.Code, wToken.Body.String())
		}

		// GET /api/v1/channels?bearer=...
		wBearer := httptest.NewRecorder()
		reqBearer, _ := http.NewRequest(http.MethodGet, "/api/v1/channels?bearer="+validToken, nil)
		ctrl.router.ServeHTTP(wBearer, reqBearer)
		if wBearer.Code != http.StatusOK {
			t.Errorf("expected 200 for valid ?bearer= parameter, got %d: %s", wBearer.Code, wBearer.Body.String())
		}
	}

	// 6. Controller with Empty Bearer Token (Unauthenticated Mode)
	{
		ctrlNoAuth, err := NewController(":0", "http://localhost:8080", nil, "", true, &logger)
		if err != nil {
			t.Fatalf("failed to create unauthenticated controller: %v", err)
		}
		ctrlNoAuth.broker.Start(2)
		defer ctrlNoAuth.GracefulStop()

		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(`{"target":"alerts","message":"public msg"}`))
		req.Header.Set("Content-Type", "application/json")
		ctrlNoAuth.router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200 when bearer is unconfigured, got %d: %s", w.Code, w.Body.String())
		}
	}

	// 7. Decoupled REST Bearer vs WS Bearer
	{
		restSecret := "rest-secret-key-777"
		wsSecret := "ws-secret-key-888"

		ctrlDecoupled, err := NewController(":0", "http://localhost:8080", nil, "", true, &logger)
		if err != nil {
			t.Fatalf("failed to create decoupled controller: %v", err)
		}
		ctrlDecoupled.SetRESTBearer(restSecret)
		ctrlDecoupled.SetWSBearer(wsSecret)
		ctrlDecoupled.broker.Start(2)
		defer ctrlDecoupled.GracefulStop()

		// Verify REST endpoint with restSecret succeeds
		wRestOk := httptest.NewRecorder()
		reqRestOk, _ := http.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(`{"target":"alerts","message":"hello rest"}`))
		reqRestOk.Header.Set("Content-Type", "application/json")
		reqRestOk.Header.Set("Authorization", "Bearer "+restSecret)
		ctrlDecoupled.router.ServeHTTP(wRestOk, reqRestOk)
		if wRestOk.Code != http.StatusOK {
			t.Errorf("expected 200 with restSecret on POST /api/v1/publish, got %d: %s", wRestOk.Code, wRestOk.Body.String())
		}

		// Verify REST endpoint with wsSecret fails (401)
		wRestFail := httptest.NewRecorder()
		reqRestFail, _ := http.NewRequest(http.MethodPost, "/api/v1/publish", strings.NewReader(`{"target":"alerts","message":"hello rest"}`))
		reqRestFail.Header.Set("Content-Type", "application/json")
		reqRestFail.Header.Set("Authorization", "Bearer "+wsSecret)
		ctrlDecoupled.router.ServeHTTP(wRestFail, reqRestFail)
		if wRestFail.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 with wsSecret on POST /api/v1/publish, got %d: %s", wRestFail.Code, wRestFail.Body.String())
		}
	}
}
