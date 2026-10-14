package rest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
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

type mtlsTestEnv struct {
	caCert    *x509.Certificate
	caPrivKey *ecdsa.PrivateKey
	caPool    *x509.CertPool
	serverTLS *tls.Config
}

func setupMTLSEnv(t *testing.T) *mtlsTestEnv {
	t.Helper()

	caPrivKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "Test mTLS CA",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caBytes, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		t.Fatalf("failed to create CA certificate: %v", err)
	}
	caCert, err := x509.ParseCertificate(caBytes)
	if err != nil {
		t.Fatalf("failed to parse CA certificate: %v", err)
	}
	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	serverPrivKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate server key: %v", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		DNSNames:    []string{"localhost"},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverBytes, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverPrivKey.PublicKey, caPrivKey)
	if err != nil {
		t.Fatalf("failed to create server cert: %v", err)
	}
	serverTLSCert := tls.Certificate{
		Certificate: [][]byte{serverBytes},
		PrivateKey:  serverPrivKey,
	}

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{serverTLSCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS13,
	}

	return &mtlsTestEnv{
		caCert:    caCert,
		caPrivKey: caPrivKey,
		caPool:    caPool,
		serverTLS: serverTLS,
	}
}

func (env *mtlsTestEnv) makeClientTLS(t *testing.T, commonName string, uris ...string) *tls.Config {
	t.Helper()
	clientPrivKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate client key: %v", err)
	}

	var parsedURIs []*url.URL
	for _, u := range uris {
		parsed, err := url.Parse(u)
		if err != nil {
			t.Fatalf("invalid uri %s: %v", u, err)
		}
		parsedURIs = append(parsedURIs, parsed)
	}

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		URIs:        parsedURIs,
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientBytes, err := x509.CreateCertificate(rand.Reader, clientTemplate, env.caCert, &clientPrivKey.PublicKey, env.caPrivKey)
	if err != nil {
		t.Fatalf("failed to create client cert: %v", err)
	}

	clientTLSCert := tls.Certificate{
		Certificate: [][]byte{clientBytes},
		PrivateKey:  clientPrivKey,
	}

	return &tls.Config{
		RootCAs:      env.caPool,
		Certificates: []tls.Certificate{clientTLSCert},
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS13,
	}
}

func (env *mtlsTestEnv) makeClientTLSWithoutCert() *tls.Config {
	return &tls.Config{
		RootCAs:    env.caPool,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS13,
	}
}

func TestMTLSCertURIValidation_ClientAndChannelHandshake(t *testing.T) {
	env := setupMTLSEnv(t)
	logger := zerolog.Nop()
	bearer := "test-bearer"

	ctrl, err := NewController(":0", "https://localhost:8765", nil, bearer, true, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	ctrl.broker.Start(2)
	defer ctrl.GracefulStop()

	server := httptest.NewUnstartedServer(ctrl.router)
	server.TLS = env.serverTLS
	server.StartTLS()
	defer server.Close()

	wsURL := "wss" + strings.TrimPrefix(server.URL, "https")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Success case: Matching client and channel permissions (rw)
	clientTLS := env.makeClientTLS(t, "sensor-1",
		"wsbroker:client:sensor-1",
		"wsbroker:channel:telemetry:rw",
	)
	dialOpts := &websocket.DialOptions{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: clientTLS,
			},
		},
	}

	conn, resp, err := websocket.Dial(ctx, wsURL+"/ws/sensor-1/rw?channel=telemetry&bearer="+bearer, dialOpts)
	if err != nil {
		t.Fatalf("expected successful connection, got error: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected 101 Switching Protocols, got %d", resp.StatusCode)
	}
	conn.Close(websocket.StatusNormalClosure, "")

	// 2. Mode downgrade allowed: RW in cert connecting with RO or WO
	connRO, _, err := websocket.Dial(ctx, wsURL+"/ws/sensor-1/ro?channel=telemetry&bearer="+bearer, dialOpts)
	if err != nil {
		t.Fatalf("expected successful RO downgrade connection, got error: %v", err)
	}
	connRO.Close(websocket.StatusNormalClosure, "")

	connWO, _, err := websocket.Dial(ctx, wsURL+"/ws/sensor-1/wo?channel=telemetry&bearer="+bearer, dialOpts)
	if err != nil {
		t.Fatalf("expected successful WO downgrade connection, got error: %v", err)
	}
	connWO.Close(websocket.StatusNormalClosure, "")

	// 3. Mode escalation forbidden: RO in cert connecting with RW or WO
	roClientTLS := env.makeClientTLS(t, "sensor-ro",
		"wsbroker:client:sensor-ro",
		"wsbroker:channel:telemetry:ro",
	)
	roDialOpts := &websocket.DialOptions{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: roClientTLS,
			},
		},
	}

	_, resp, err = websocket.Dial(ctx, wsURL+"/ws/sensor-ro/rw?channel=telemetry&bearer="+bearer, roDialOpts)
	if err == nil {
		t.Fatalf("expected error on mode escalation, got success")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden on mode escalation, got resp: %v", resp)
	}

	// 4. Client name mismatch: cert has sensor-1, client connects as sensor-2
	_, resp, err = websocket.Dial(ctx, wsURL+"/ws/sensor-2/rw?channel=telemetry&bearer="+bearer, dialOpts)
	if err == nil {
		t.Fatalf("expected error on client name mismatch, got success")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden on client mismatch, got resp: %v", resp)
	}

	// 5. Unauthorized channel query parameter
	_, resp, err = websocket.Dial(ctx, wsURL+"/ws/sensor-1/rw?channel=admin-control&bearer="+bearer, dialOpts)
	if err == nil {
		t.Fatalf("expected error on unauthorized query channel, got success")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden on unauthorized channel, got resp: %v", resp)
	}

	// 6. Direct channel endpoint /ws/channel/:channel/:mode
	channelConn, _, err := websocket.Dial(ctx, wsURL+"/ws/channel/telemetry/rw?name=sensor-1&bearer="+bearer, dialOpts)
	if err != nil {
		t.Fatalf("expected successful connection to /ws/channel/telemetry/rw, got error: %v", err)
	}
	channelConn.Close(websocket.StatusNormalClosure, "")

	// 7. Direct channel endpoint with unauthorized channel
	_, resp, err = websocket.Dial(ctx, wsURL+"/ws/channel/secret/rw?name=sensor-1&bearer="+bearer, dialOpts)
	if err == nil {
		t.Fatalf("expected error on unauthorized channel endpoint, got success")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden, got resp: %v", resp)
	}
}

func TestMTLSCertURIValidation_DynamicAttach(t *testing.T) {
	env := setupMTLSEnv(t)
	logger := zerolog.Nop()
	bearer := "test-bearer"

	ctrl, err := NewController(":0", "https://localhost:8765", nil, bearer, true, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	ctrl.broker.Start(2)
	defer ctrl.GracefulStop()

	server := httptest.NewUnstartedServer(ctrl.router)
	server.TLS = env.serverTLS
	server.StartTLS()
	defer server.Close()

	wsURL := "wss" + strings.TrimPrefix(server.URL, "https")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientTLS := env.makeClientTLS(t, "sensor-dyn",
		"wsbroker:client:sensor-dyn",
		"wsbroker:channel:allowed-channel:rw",
	)
	dialOpts := &websocket.DialOptions{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: clientTLS,
			},
		},
	}

	conn, _, err := websocket.Dial(ctx, wsURL+"/ws/sensor-dyn/rw?bearer="+bearer, dialOpts)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// 1. Dynamic attach to allowed channel
	attachAllowed, err := event.NewMessageEvent("sensor-dyn", "allowed-channel", "allowed-channel")
	if err != nil {
		t.Fatalf("failed to create attach event: %v", err)
	}
	attachAllowed.Type = event.TypeAttach
	if err := wsjson.Write(ctx, conn, attachAllowed); err != nil {
		t.Fatalf("failed to send attach to allowed channel: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	members := ctrl.broker.GetChannelMembers("allowed-channel")
	if len(members) != 1 || members[0] != "sensor-dyn" {
		t.Errorf("expected sensor-dyn to be in allowed-channel, got %v", members)
	}

	// 2. Dynamic attach to forbidden channel
	attachForbidden, err := event.NewMessageEvent("sensor-dyn", "forbidden-channel", "forbidden-channel")
	if err != nil {
		t.Fatalf("failed to create attach event: %v", err)
	}
	attachForbidden.Type = event.TypeAttach
	if err := wsjson.Write(ctx, conn, attachForbidden); err != nil {
		t.Fatalf("failed to send attach to forbidden channel: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	membersForbidden := ctrl.broker.GetChannelMembers("forbidden-channel")
	if len(membersForbidden) != 0 {
		t.Errorf("expected forbidden-channel to have 0 members, got %v", membersForbidden)
	}
}

func TestAuthType_ConfigurationNormalization(t *testing.T) {
	logger := zerolog.Nop()
	ctrl, err := NewController(":0", "https://localhost:8765", nil, "test-bearer", false, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}

	tests := []struct {
		input    string
		expected AuthType
	}{
		{"bearer", AuthTypeBearer},
		{"BEARER", AuthTypeBearer},
		{" Bearer ", AuthTypeBearer},
		{"mtls", AuthTypeMTLS},
		{"mTLS", AuthTypeMTLS},
		{"MTLS", AuthTypeMTLS},
		{"both", AuthTypeBoth},
		{"Both", AuthTypeBoth},
		{"BOTH", AuthTypeBoth},
		{"", AuthTypeBearer},
		{"unknown", AuthTypeBearer},
		{"invalid-auth", AuthTypeBearer},
	}

	for _, tt := range tests {
		ctrl.SetAuthType(tt.input)
		if got := ctrl.GetAuthType(); got != tt.expected {
			t.Errorf("SetAuthType(%q): expected %q, got %q", tt.input, tt.expected, got)
		}
	}
}

func TestAuthType_BothMode(t *testing.T) {
	env := setupMTLSEnv(t)
	env.serverTLS.ClientAuth = tls.VerifyClientCertIfGiven
	logger := zerolog.Nop()
	bearer := "test-secret-bearer"

	ctrl, err := NewController(":0", "https://localhost:8765", nil, bearer, false, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	ctrl.SetAuthType("both")
	ctrl.broker.Start(2)
	defer ctrl.GracefulStop()

	server := httptest.NewUnstartedServer(ctrl.router)
	server.TLS = env.serverTLS
	server.StartTLS()
	defer server.Close()

	wsURL := "wss" + strings.TrimPrefix(server.URL, "https")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	noCertTLS := env.makeClientTLSWithoutCert()
	noCertDialOpts := &websocket.DialOptions{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: noCertTLS,
			},
		},
	}

	validCertTLS := env.makeClientTLS(t, "cert-client",
		"wsbroker:client:cert-client",
		"wsbroker:channel:sensor-data:rw",
	)
	validCertDialOpts := &websocket.DialOptions{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: validCertTLS,
			},
		},
	}

	// 1. Success with Bearer token (no client cert)
	conn1, resp1, err := websocket.Dial(ctx, wsURL+"/ws/client-bearer/rw?bearer="+bearer, noCertDialOpts)
	if err != nil {
		t.Fatalf("expected successful bearer connection without cert, got err: %v", err)
	}
	if resp1.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected 101, got %d", resp1.StatusCode)
	}
	conn1.Close(websocket.StatusNormalClosure, "")

	// 2. Success with token query parameter (no client cert)
	conn2, resp2, err := websocket.Dial(ctx, wsURL+"/ws/client-bearer/rw?token="+bearer, noCertDialOpts)
	if err != nil {
		t.Fatalf("expected successful token connection without cert, got err: %v", err)
	}
	if resp2.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected 101, got %d", resp2.StatusCode)
	}
	conn2.Close(websocket.StatusNormalClosure, "")

	// 3. Success with Authorization header (no client cert)
	headerDialOpts := &websocket.DialOptions{
		HTTPHeader: http.Header{
			"Authorization": []string{"Bearer " + bearer},
		},
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: noCertTLS,
			},
		},
	}
	conn3, resp3, err := websocket.Dial(ctx, wsURL+"/ws/client-bearer/rw", headerDialOpts)
	if err != nil {
		t.Fatalf("expected successful auth header connection without cert, got err: %v", err)
	}
	if resp3.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected 101, got %d", resp3.StatusCode)
	}
	conn3.Close(websocket.StatusNormalClosure, "")

	// 4. Success with client cert fallback (no bearer token)
	conn4, resp4, err := websocket.Dial(ctx, wsURL+"/ws/cert-client/rw?channel=sensor-data", validCertDialOpts)
	if err != nil {
		t.Fatalf("expected successful mTLS fallback connection, got err: %v", err)
	}
	if resp4.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected 101, got %d", resp4.StatusCode)
	}
	conn4.Close(websocket.StatusNormalClosure, "")

	// 5. Invalid Bearer token with client cert must fail immediately (401, no silent fallback to mTLS)
	_, resp5, err := websocket.Dial(ctx, wsURL+"/ws/cert-client/rw?bearer=wrong-token", validCertDialOpts)
	if err == nil {
		t.Fatalf("expected error on invalid bearer token, got success")
	}
	if resp5 == nil || resp5.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for invalid bearer, got resp: %v", resp5)
	}

	// 6. Neither Bearer token nor client cert -> 401 Unauthorized
	_, resp6, err := websocket.Dial(ctx, wsURL+"/ws/anon-client/rw", noCertDialOpts)
	if err == nil {
		t.Fatalf("expected error when neither bearer nor cert is provided, got success")
	}
	if resp6 == nil || resp6.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got resp: %v", resp6)
	}

	// 7. Client cert with unauthorized channel (fallback mTLS) -> 403 Forbidden
	_, resp7, err := websocket.Dial(ctx, wsURL+"/ws/cert-client/rw?channel=forbidden-channel", validCertDialOpts)
	if err == nil {
		t.Fatalf("expected error on unauthorized channel via cert, got success")
	}
	if resp7 == nil || resp7.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden, got resp: %v", resp7)
	}

	// 8. Client cert with unauthorized client name (fallback mTLS) -> 403 Forbidden
	_, resp8, err := websocket.Dial(ctx, wsURL+"/ws/different-client/rw", validCertDialOpts)
	if err == nil {
		t.Fatalf("expected error on unauthorized client name via cert, got success")
	}
	if resp8 == nil || resp8.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden, got resp: %v", resp8)
	}

	// 9. Direct channel endpoint /ws/channel/:channel/:mode with Bearer (no cert)
	chConn1, respCh1, err := websocket.Dial(ctx, wsURL+"/ws/channel/any-channel/rw?bearer="+bearer, noCertDialOpts)
	if err != nil {
		t.Fatalf("expected direct channel endpoint success with bearer, got err: %v", err)
	}
	if respCh1.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected 101, got %d", respCh1.StatusCode)
	}
	chConn1.Close(websocket.StatusNormalClosure, "")

	// 10. Direct channel endpoint /ws/channel/:channel/:mode with cert fallback
	chConn2, respCh2, err := websocket.Dial(ctx, wsURL+"/ws/channel/sensor-data/rw?name=cert-client", validCertDialOpts)
	if err != nil {
		t.Fatalf("expected direct channel endpoint success with cert fallback, got err: %v", err)
	}
	if respCh2.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected 101, got %d", respCh2.StatusCode)
	}
	chConn2.Close(websocket.StatusNormalClosure, "")
}

func TestAuthType_MTLSMode(t *testing.T) {
	env := setupMTLSEnv(t)
	env.serverTLS.ClientAuth = tls.VerifyClientCertIfGiven
	logger := zerolog.Nop()
	bearer := "test-secret-bearer"

	ctrl, err := NewController(":0", "https://localhost:8765", nil, bearer, false, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	ctrl.SetAuthType("mtls")
	ctrl.broker.Start(2)
	defer ctrl.GracefulStop()

	server := httptest.NewUnstartedServer(ctrl.router)
	server.TLS = env.serverTLS
	server.StartTLS()
	defer server.Close()

	wsURL := "wss" + strings.TrimPrefix(server.URL, "https")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	noCertTLS := env.makeClientTLSWithoutCert()
	noCertDialOpts := &websocket.DialOptions{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: noCertTLS,
			},
		},
	}

	validCertTLS := env.makeClientTLS(t, "mtls-client",
		"wsbroker:client:mtls-client",
		"wsbroker:channel:metrics:rw",
	)
	validCertDialOpts := &websocket.DialOptions{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: validCertTLS,
			},
		},
	}

	// 1. Bearer provided but no client cert in mtls mode -> 401 Unauthorized
	_, resp1, err := websocket.Dial(ctx, wsURL+"/ws/mtls-client/rw?bearer="+bearer, noCertDialOpts)
	if err == nil {
		t.Fatalf("expected 401 when connecting without client cert in mtls mode, got success")
	}
	if resp1 == nil || resp1.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got resp: %v", resp1)
	}

	// 2. Valid client cert -> Success
	conn2, resp2, err := websocket.Dial(ctx, wsURL+"/ws/mtls-client/rw?channel=metrics", validCertDialOpts)
	if err != nil {
		t.Fatalf("expected successful mtls connection, got err: %v", err)
	}
	if resp2.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected 101, got %d", resp2.StatusCode)
	}
	conn2.Close(websocket.StatusNormalClosure, "")

	// 3. Client name mismatch -> 403 Forbidden
	_, resp3, err := websocket.Dial(ctx, wsURL+"/ws/wrong-client-name/rw?channel=metrics", validCertDialOpts)
	if err == nil {
		t.Fatalf("expected error on client name mismatch, got success")
	}
	if resp3 == nil || resp3.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden, got resp: %v", resp3)
	}

	// 4. Unauthorized channel -> 403 Forbidden
	_, resp4, err := websocket.Dial(ctx, wsURL+"/ws/mtls-client/rw?channel=secret-channel", validCertDialOpts)
	if err == nil {
		t.Fatalf("expected error on unauthorized channel, got success")
	}
	if resp4 == nil || resp4.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden, got resp: %v", resp4)
	}
}

func TestAuthType_BearerMode(t *testing.T) {
	env := setupMTLSEnv(t)
	env.serverTLS.ClientAuth = tls.VerifyClientCertIfGiven
	logger := zerolog.Nop()
	bearer := "test-secret-bearer"

	ctrl, err := NewController(":0", "https://localhost:8765", nil, bearer, false, &logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	ctrl.SetAuthType("bearer")
	ctrl.broker.Start(2)
	defer ctrl.GracefulStop()

	server := httptest.NewUnstartedServer(ctrl.router)
	server.TLS = env.serverTLS
	server.StartTLS()
	defer server.Close()

	wsURL := "wss" + strings.TrimPrefix(server.URL, "https")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	noCertTLS := env.makeClientTLSWithoutCert()
	noCertDialOpts := &websocket.DialOptions{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: noCertTLS,
			},
		},
	}

	validCertTLS := env.makeClientTLS(t, "bearer-client",
		"wsbroker:client:bearer-client",
		"wsbroker:channel:metrics:rw",
	)
	validCertDialOpts := &websocket.DialOptions{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: validCertTLS,
			},
		},
	}

	// 1. Valid bearer token without client cert -> Success
	conn1, resp1, err := websocket.Dial(ctx, wsURL+"/ws/bearer-client/rw?bearer="+bearer, noCertDialOpts)
	if err != nil {
		t.Fatalf("expected successful bearer connection, got err: %v", err)
	}
	if resp1.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected 101, got %d", resp1.StatusCode)
	}
	conn1.Close(websocket.StatusNormalClosure, "")

	// 2. Invalid bearer token -> 401 Unauthorized
	_, resp2, err := websocket.Dial(ctx, wsURL+"/ws/bearer-client/rw?bearer=invalid-token", noCertDialOpts)
	if err == nil {
		t.Fatalf("expected error on invalid bearer token, got success")
	}
	if resp2 == nil || resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got resp: %v", resp2)
	}

	// 3. No bearer token provided (even with valid client cert) -> 401 Unauthorized
	_, resp3, err := websocket.Dial(ctx, wsURL+"/ws/bearer-client/rw", validCertDialOpts)
	if err == nil {
		t.Fatalf("expected error when no bearer token is supplied in bearer mode, got success")
	}
	if resp3 == nil || resp3.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got resp: %v", resp3)
	}
}
