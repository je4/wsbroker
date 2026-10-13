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
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/je4/wsbrokerclient"
	"github.com/rs/zerolog"
)

func generateTestMTLSConfig(t *testing.T) (*tls.Config, *tls.Config, *tls.Config) {
	t.Helper()

	// 1. Root CA
	caPrivKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "Test CA",
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

	// 2. Server Certificate
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

	// 3. Client Certificate (trusted)
	clientPrivKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate client key: %v", err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			CommonName: "client-mtls",
		},
		DNSNames:    []string{"client-mtls.local"},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientBytes, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientPrivKey.PublicKey, caPrivKey)
	if err != nil {
		t.Fatalf("failed to create client cert: %v", err)
	}
	clientTLSCert := tls.Certificate{
		Certificate: [][]byte{clientBytes},
		PrivateKey:  clientPrivKey,
	}

	// 4. Untrusted Client Certificate (signed by a separate rogue CA)
	otherCAPrivKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate rogue CA key: %v", err)
	}
	otherCATemplate := &x509.Certificate{
		SerialNumber: big.NewInt(100),
		Subject: pkix.Name{
			CommonName: "Untrusted Rogue CA",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	otherCABytes, err := x509.CreateCertificate(rand.Reader, otherCATemplate, otherCATemplate, &otherCAPrivKey.PublicKey, otherCAPrivKey)
	if err != nil {
		t.Fatalf("failed to create rogue CA cert: %v", err)
	}
	otherCACert, err := x509.ParseCertificate(otherCABytes)
	if err != nil {
		t.Fatalf("failed to parse rogue CA cert: %v", err)
	}

	untrustedPrivKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate untrusted client key: %v", err)
	}
	untrustedTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(101),
		Subject: pkix.Name{
			CommonName: "untrusted-client",
		},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	untrustedBytes, err := x509.CreateCertificate(rand.Reader, untrustedTemplate, otherCACert, &untrustedPrivKey.PublicKey, otherCAPrivKey)
	if err != nil {
		t.Fatalf("failed to create untrusted client cert: %v", err)
	}
	untrustedTLSCert := tls.Certificate{
		Certificate: [][]byte{untrustedBytes},
		PrivateKey:  untrustedPrivKey,
	}

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{serverTLSCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	clientTLS := &tls.Config{
		Certificates: []tls.Certificate{clientTLSCert},
		RootCAs:      caPool,
	}
	untrustedClientTLS := &tls.Config{
		Certificates: []tls.Certificate{untrustedTLSCert},
		RootCAs:      caPool,
	}

	return serverTLS, clientTLS, untrustedClientTLS
}

func TestWSBrokerClient_Integration(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "test-secret-token"

	// 1. Initialisiere WSBroker Controller und starte Broker Worker Pool
	ctrl, err := NewController(":0", "http://localhost:8765", nil, bearer, true, &logger)
	if err != nil {
		t.Fatalf("failed to create rest controller: %v", err)
	}

	ctrl.broker.Start(4)
	defer ctrl.GracefulStop()

	server := httptest.NewServer(ctrl.router)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 2. Initialisiere RESTClient
	restCli, err := wsbrokerclient.NewRESTClient(wsbrokerclient.RESTConfig{
		BaseURL: server.URL,
		Bearer:  bearer,
	})
	if err != nil {
		t.Fatalf("failed to create REST client: %v", err)
	}
	defer restCli.Close()

	// Test REST Ping
	pong, err := restCli.Ping(ctx)
	if err != nil {
		t.Fatalf("REST Ping failed: %v", err)
	}
	if pong != "pong" {
		t.Errorf("expected 'pong', got '%s'", pong)
	}

	// 3. Initialisiere WebSocket Clients
	cliAlpha, err := wsbrokerclient.New(wsbrokerclient.DefaultConfig(),
		wsbrokerclient.WithEndpoint(server.URL),
		wsbrokerclient.WithMode(wsbrokerclient.ModeRW),
		wsbrokerclient.WithClientName("client-alpha"),
		wsbrokerclient.WithBearerToken(bearer),
		wsbrokerclient.WithInitialChannels("ch-main"),
	)
	if err != nil {
		t.Fatalf("failed to create client-alpha: %v", err)
	}
	defer cliAlpha.Close()

	cliBeta, err := wsbrokerclient.New(wsbrokerclient.DefaultConfig(),
		wsbrokerclient.WithEndpoint(server.URL),
		wsbrokerclient.WithMode(wsbrokerclient.ModeRW),
		wsbrokerclient.WithClientName("client-beta"),
		wsbrokerclient.WithBearerToken(bearer),
		wsbrokerclient.WithInitialChannels("ch-main", "ch-alerts"),
	)
	if err != nil {
		t.Fatalf("failed to create client-beta: %v", err)
	}
	defer cliBeta.Close()

	// Subscriptions einrichten
	subAlphaMain, err := cliAlpha.Subscribe(ctx, "ch-main")
	if err != nil {
		t.Fatalf("client-alpha failed to subscribe to ch-main: %v", err)
	}
	defer subAlphaMain.Unsubscribe()

	subBetaMain, err := cliBeta.Subscribe(ctx, "ch-main")
	if err != nil {
		t.Fatalf("client-beta failed to subscribe to ch-main: %v", err)
	}
	defer subBetaMain.Unsubscribe()

	// Direct-Message Handler für Beta einrichten
	betaDirectReceived := make(chan *wsbrokerclient.Event, 5)
	unsubBetaDirect := cliBeta.OnDirect(func(evt *wsbrokerclient.Event) {
		betaDirectReceived <- evt
	})
	defer unsubBetaDirect()

	// Verbindungen herstellen
	if err := cliAlpha.ConnectSync(ctx); err != nil {
		t.Fatalf("client-alpha failed to connect: %v", err)
	}
	if err := cliBeta.ConnectSync(ctx); err != nil {
		t.Fatalf("client-beta failed to connect: %v", err)
	}

	// 4. REST Abfrage: Verbundene Clients und Kanäle prüfen
	clients, err := restCli.GetClients(ctx)
	if err != nil {
		t.Fatalf("GetClients failed: %v", err)
	}
	if !slices.Contains(clients, "client-alpha") || !slices.Contains(clients, "client-beta") {
		t.Errorf("expected client-alpha and client-beta in clients list, got: %v", clients)
	}

	mainMembers, err := restCli.GetChannelClients(ctx, "ch-main")
	if err != nil {
		t.Fatalf("GetChannelClients(ch-main) failed: %v", err)
	}
	if !slices.Contains(mainMembers, "client-alpha") || !slices.Contains(mainMembers, "client-beta") {
		t.Errorf("expected both clients in ch-main, got: %v", mainMembers)
	}

	// 5. Test: Broadcast-Nachricht von Client Alpha an 'ch-main'
	broadcastData := map[string]string{"greeting": "hello ch-main from alpha"}
	if err := cliAlpha.Broadcast(ctx, "ch-main", broadcastData); err != nil {
		t.Fatalf("client-alpha broadcast failed: %v", err)
	}

	select {
	case evt := <-subBetaMain.Events():
		var payload map[string]string
		if err := evt.UnmarshalData(&payload); err != nil {
			t.Fatalf("failed to unmarshal broadcast payload for beta: %v", err)
		}
		if payload["greeting"] != "hello ch-main from alpha" {
			t.Errorf("unexpected broadcast payload on beta: %v", payload)
		}
		if evt.Source != "client-alpha" {
			t.Errorf("expected source 'client-alpha', got '%s'", evt.Source)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for broadcast event on client-beta")
	}

	select {
	case evt := <-subAlphaMain.Events():
		var payload map[string]string
		if err := evt.UnmarshalData(&payload); err != nil {
			t.Fatalf("failed to unmarshal broadcast payload for alpha: %v", err)
		}
		if payload["greeting"] != "hello ch-main from alpha" {
			t.Errorf("unexpected broadcast payload on alpha: %v", payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for broadcast event on client-alpha")
	}

	// 6. Test: Direct Message von Alpha an Beta
	directData := map[string]string{"secret": "for-beta-eyes-only"}
	if err := cliAlpha.SendDirect(ctx, "client-beta", directData); err != nil {
		t.Fatalf("client-alpha SendDirect failed: %v", err)
	}

	select {
	case evt := <-betaDirectReceived:
		var payload map[string]string
		if err := evt.UnmarshalData(&payload); err != nil {
			t.Fatalf("failed to unmarshal direct payload: %v", err)
		}
		if payload["secret"] != "for-beta-eyes-only" {
			t.Errorf("unexpected direct payload: %v", payload)
		}
		if evt.Source != "client-alpha" {
			t.Errorf("expected source 'client-alpha', got '%s'", evt.Source)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for direct event on client-beta")
	}

	// 7. Test: Dynamisches Attach & Detach
	subAlphaAlerts, err := cliAlpha.Subscribe(ctx, "ch-alerts")
	if err != nil {
		t.Fatalf("client-alpha failed to subscribe to ch-alerts: %v", err)
	}
	defer subAlphaAlerts.Unsubscribe()

	if err := cliAlpha.Attach(ctx, "ch-alerts"); err != nil {
		t.Fatalf("client-alpha failed to attach to ch-alerts: %v", err)
	}

	// Kurz warten bis Attach verarbeitet ist
	time.Sleep(100 * time.Millisecond)

	alertsMembers, err := restCli.GetChannelClients(ctx, "ch-alerts")
	if err != nil {
		t.Fatalf("GetChannelClients(ch-alerts) failed: %v", err)
	}
	if !slices.Contains(alertsMembers, "client-alpha") {
		t.Errorf("expected client-alpha in ch-alerts after attach, got: %v", alertsMembers)
	}

	// Nachricht auf ch-alerts senden -> Alpha empfängt
	if err := cliBeta.Broadcast(ctx, "ch-alerts", "alert-level-1"); err != nil {
		t.Fatalf("client-beta broadcast to ch-alerts failed: %v", err)
	}

	select {
	case evt := <-subAlphaAlerts.Events():
		var msg string
		if err := evt.UnmarshalData(&msg); err != nil {
			t.Fatalf("failed to unmarshal alert payload: %v", err)
		}
		if msg != "alert-level-1" {
			t.Errorf("expected 'alert-level-1', got '%s'", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for alerts event on client-alpha")
	}

	// Detach von ch-alerts
	if err := cliAlpha.Detach(ctx, "ch-alerts"); err != nil {
		t.Fatalf("client-alpha failed to detach from ch-alerts: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	alertsMembersAfter, err := restCli.GetChannelClients(ctx, "ch-alerts")
	if err != nil {
		t.Fatalf("GetChannelClients(ch-alerts) after detach failed: %v", err)
	}
	if slices.Contains(alertsMembersAfter, "client-alpha") {
		t.Errorf("client-alpha still present in ch-alerts after detach: %v", alertsMembersAfter)
	}

	// 8. Test: REST Publish zu Channel
	restPubResp, err := restCli.PublishToChannel(ctx, "ch-main", "rest-admin", map[string]string{"notice": "scheduled maintenance"})
	if err != nil {
		t.Fatalf("REST PublishToChannel failed: %v", err)
	}
	if !restPubResp.Sent || restPubResp.Subscribers != 2 {
		t.Errorf("unexpected REST publish response: %+v", restPubResp)
	}

	// Beide WebSocket Clients empfangen den REST Publish
	select {
	case evt := <-subAlphaMain.Events():
		var data map[string]string
		if err := evt.UnmarshalData(&data); err != nil {
			t.Fatalf("failed to unmarshal REST publish event for alpha: %v", err)
		}
		if data["notice"] != "scheduled maintenance" || evt.Source != "rest-admin" {
			t.Errorf("unexpected event content on alpha: %+v", evt)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for REST publish on client-alpha")
	}

	select {
	case evt := <-subBetaMain.Events():
		var data map[string]string
		if err := evt.UnmarshalData(&data); err != nil {
			t.Fatalf("failed to unmarshal REST publish event for beta: %v", err)
		}
		if data["notice"] != "scheduled maintenance" || evt.Source != "rest-admin" {
			t.Errorf("unexpected event content on beta: %+v", evt)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for REST publish on client-beta")
	}

	// 9. Test: Client via REST trennen
	if err := restCli.DisconnectClient(ctx, "client-beta"); err != nil {
		t.Fatalf("DisconnectClient failed: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	clientsAfterDisconnect, err := restCli.GetClients(ctx)
	if err != nil {
		t.Fatalf("GetClients after disconnect failed: %v", err)
	}
	if slices.Contains(clientsAfterDisconnect, "client-beta") {
		t.Errorf("client-beta should no longer be listed after disconnect: %v", clientsAfterDisconnect)
	}
}

func TestWSBrokerClient_CallbacksAndEventPublish(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "cb-secret-token"

	ctrl, err := NewController(":0", "http://localhost:8765", nil, bearer, true, &logger)
	if err != nil {
		t.Fatalf("failed to create rest controller: %v", err)
	}

	ctrl.broker.Start(2)
	defer ctrl.GracefulStop()

	server := httptest.NewServer(ctrl.router)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	restCli, err := wsbrokerclient.NewRESTClient(wsbrokerclient.RESTConfig{
		BaseURL: server.URL,
		Bearer:  bearer,
	})
	if err != nil {
		t.Fatalf("failed to create REST client: %v", err)
	}
	defer restCli.Close()

	cli, err := wsbrokerclient.New(wsbrokerclient.DefaultConfig(),
		wsbrokerclient.WithEndpoint(server.URL),
		wsbrokerclient.WithMode(wsbrokerclient.ModeRW),
		wsbrokerclient.WithClientName("listener-client"),
		wsbrokerclient.WithBearerToken(bearer),
		wsbrokerclient.WithInitialChannels("metrics"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	channelEvents := make(chan *wsbrokerclient.Event, 5)
	allEvents := make(chan *wsbrokerclient.Event, 5)

	unsubChannel := cli.OnChannel("metrics", func(evt *wsbrokerclient.Event) {
		channelEvents <- evt
	})
	defer unsubChannel()

	unsubAll := cli.OnMessage(func(evt *wsbrokerclient.Event) {
		allEvents <- evt
	})
	defer unsubAll()

	if err := cli.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect listener client: %v", err)
	}

	if !cli.IsConnected() {
		t.Errorf("expected client to be connected")
	}

	// REST Publish Event
	metricEvt, err := wsbrokerclient.NewMessageEvent("producer-service", "metrics", map[string]int{"cpu": 42})
	if err != nil {
		t.Fatalf("failed to create message event: %v", err)
	}

	pubResp, err := restCli.Publish(ctx, metricEvt)
	if err != nil {
		t.Fatalf("restCli.Publish failed: %v", err)
	}
	if !pubResp.Sent || pubResp.Subscribers != 1 {
		t.Errorf("unexpected publish response: %+v", pubResp)
	}

	select {
	case evt := <-channelEvents:
		var data map[string]int
		if err := evt.UnmarshalData(&data); err != nil {
			t.Fatalf("failed to unmarshal channel event: %v", err)
		}
		if data["cpu"] != 42 {
			t.Errorf("expected cpu=42, got %v", data["cpu"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for OnChannel event")
	}

	select {
	case evt := <-allEvents:
		if evt.Target != "metrics" {
			t.Errorf("expected target 'metrics', got '%s'", evt.Target)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for OnMessage event")
	}
}

func TestWSBrokerClient_AuthFailure(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "correct-secret-token"

	ctrl, err := NewController(":0", "http://localhost:8765", nil, bearer, true, &logger)
	if err != nil {
		t.Fatalf("failed to create rest controller: %v", err)
	}

	ctrl.broker.Start(2)
	defer ctrl.GracefulStop()

	server := httptest.NewServer(ctrl.router)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Client mit falschem Token konfigurieren
	cliInvalid, err := wsbrokerclient.New(wsbrokerclient.DefaultConfig(),
		wsbrokerclient.WithEndpoint(server.URL),
		wsbrokerclient.WithMode(wsbrokerclient.ModeRW),
		wsbrokerclient.WithClientName("unauthorized-client"),
		wsbrokerclient.WithBearerToken("wrong-token"),
	)
	if err != nil {
		t.Fatalf("failed to create unauthorized client: %v", err)
	}
	defer cliInvalid.Close()

	if err := cliInvalid.ConnectSync(ctx); err == nil {
		t.Fatal("expected ConnectSync with invalid token, but it succeeded")
	}
}

func TestWSBrokerClient_MTLSIntegration(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "mtls-secret-token"

	serverTLS, clientTLS, untrustedClientTLS := generateTestMTLSConfig(t)

	ctrl, err := NewController(":0", "https://localhost:8765", serverTLS, bearer, true, &logger)
	if err != nil {
		t.Fatalf("failed to create rest controller: %v", err)
	}

	ctrl.broker.Start(2)
	defer ctrl.GracefulStop()

	server := httptest.NewUnstartedServer(ctrl.router)
	server.TLS = serverTLS
	server.StartTLS()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. REST Client über mTLS
	restCli, err := wsbrokerclient.NewRESTClient(wsbrokerclient.RESTConfig{
		BaseURL:   server.URL,
		Bearer:    bearer,
		TLSConfig: clientTLS,
	})
	if err != nil {
		t.Fatalf("failed to create mTLS REST client: %v", err)
	}
	defer restCli.Close()

	pong, err := restCli.Ping(ctx)
	if err != nil {
		t.Fatalf("mTLS REST Ping failed: %v", err)
	}
	if pong != "pong" {
		t.Errorf("expected 'pong', got '%s'", pong)
	}

	// 2. WebSocket Client über mTLS
	cliTrusted, err := wsbrokerclient.New(wsbrokerclient.DefaultConfig(),
		wsbrokerclient.WithEndpoint(server.URL),
		wsbrokerclient.WithMode(wsbrokerclient.ModeRW),
		wsbrokerclient.WithClientName("mtls-trusted-client"),
		wsbrokerclient.WithBearerToken(bearer),
		wsbrokerclient.WithTLSConfig(clientTLS),
		wsbrokerclient.WithInitialChannels("secure-room"),
	)
	if err != nil {
		t.Fatalf("failed to create trusted mTLS client: %v", err)
	}
	defer cliTrusted.Close()

	subSecure, err := cliTrusted.Subscribe(ctx, "secure-room")
	if err != nil {
		t.Fatalf("failed to subscribe on secure-room: %v", err)
	}
	defer subSecure.Unsubscribe()

	if err := cliTrusted.ConnectSync(ctx); err != nil {
		t.Fatalf("trusted mTLS client failed to connect: %v", err)
	}

	if !cliTrusted.IsConnected() {
		t.Errorf("expected trusted client to be connected")
	}

	// Verifiziere Broadcast über mTLS
	payload := map[string]string{"secret_info": "mtls encrypted message"}
	if err := cliTrusted.Broadcast(ctx, "secure-room", payload); err != nil {
		t.Fatalf("trusted client broadcast failed: %v", err)
	}

	select {
	case evt := <-subSecure.Events():
		var data map[string]string
		if err := evt.UnmarshalData(&data); err != nil {
			t.Fatalf("failed to unmarshal secure event: %v", err)
		}
		if data["secret_info"] != "mtls encrypted message" {
			t.Errorf("unexpected secure payload: %v", data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for secure event")
	}

	// 3. Negativtest: Client mit nicht vertrauenswürdigem Client-Zertifikat wird abgewiesen
	cliUntrusted, err := wsbrokerclient.New(wsbrokerclient.DefaultConfig(),
		wsbrokerclient.WithEndpoint(server.URL),
		wsbrokerclient.WithMode(wsbrokerclient.ModeRW),
		wsbrokerclient.WithClientName("mtls-rogue-client"),
		wsbrokerclient.WithBearerToken(bearer),
		wsbrokerclient.WithTLSConfig(untrustedClientTLS),
	)
	if err != nil {
		t.Fatalf("failed to create rogue client: %v", err)
	}
	defer cliUntrusted.Close()

	if err := cliUntrusted.ConnectSync(ctx); err == nil {
		t.Fatal("expected ConnectSync with untrusted client certificate to fail, but it succeeded")
	}

	// 4. Negativtest: Client ohne Client-Zertifikat wird abgewiesen
	noCertTLS := &tls.Config{
		RootCAs: clientTLS.RootCAs,
	}
	cliNoCert, err := wsbrokerclient.New(wsbrokerclient.DefaultConfig(),
		wsbrokerclient.WithEndpoint(server.URL),
		wsbrokerclient.WithMode(wsbrokerclient.ModeRW),
		wsbrokerclient.WithClientName("mtls-no-cert-client"),
		wsbrokerclient.WithBearerToken(bearer),
		wsbrokerclient.WithTLSConfig(noCertTLS),
	)
	if err != nil {
		t.Fatalf("failed to create no-cert client: %v", err)
	}
	defer cliNoCert.Close()

	if err := cliNoCert.ConnectSync(ctx); err == nil {
		t.Fatal("expected ConnectSync without client certificate to fail, but it succeeded")
	}
}

func TestWSBrokerClient_ConnectionModes(t *testing.T) {
	logger := zerolog.Nop()
	bearer := "modes-secret-token"

	ctrl, err := NewController(":0", "http://localhost:8765", nil, bearer, true, &logger)
	if err != nil {
		t.Fatalf("failed to create rest controller: %v", err)
	}

	ctrl.broker.Start(4)
	defer ctrl.GracefulStop()

	server := httptest.NewServer(ctrl.router)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Initialisiere Clients mit unterschiedlichen Verbindungs-Modi:
	// - clientRO (ModeRO): Read-Only
	// - clientWO (ModeWO): Write-Only
	// - clientRW (ModeRW): Read-Write
	cliRO, err := wsbrokerclient.New(wsbrokerclient.DefaultConfig(),
		wsbrokerclient.WithEndpoint(server.URL),
		wsbrokerclient.WithMode(wsbrokerclient.ModeRO),
		wsbrokerclient.WithClientName("client-ro"),
		wsbrokerclient.WithBearerToken(bearer),
		wsbrokerclient.WithInitialChannels("mode-test-channel"),
	)
	if err != nil {
		t.Fatalf("failed to create client-ro: %v", err)
	}
	defer cliRO.Close()

	cliWO, err := wsbrokerclient.New(wsbrokerclient.DefaultConfig(),
		wsbrokerclient.WithEndpoint(server.URL),
		wsbrokerclient.WithMode(wsbrokerclient.ModeWO),
		wsbrokerclient.WithClientName("client-wo"),
		wsbrokerclient.WithBearerToken(bearer),
		wsbrokerclient.WithInitialChannels("mode-test-channel"),
	)
	if err != nil {
		t.Fatalf("failed to create client-wo: %v", err)
	}
	defer cliWO.Close()

	cliRW, err := wsbrokerclient.New(wsbrokerclient.DefaultConfig(),
		wsbrokerclient.WithEndpoint(server.URL),
		wsbrokerclient.WithMode(wsbrokerclient.ModeRW),
		wsbrokerclient.WithClientName("client-rw"),
		wsbrokerclient.WithBearerToken(bearer),
		wsbrokerclient.WithInitialChannels("mode-test-channel"),
	)
	if err != nil {
		t.Fatalf("failed to create client-rw: %v", err)
	}
	defer cliRW.Close()

	// Subscriptions auf mode-test-channel einrichten
	subRO, err := cliRO.Subscribe(ctx, "mode-test-channel")
	if err != nil {
		t.Fatalf("client-ro failed to subscribe: %v", err)
	}
	defer subRO.Unsubscribe()

	subWO, err := cliWO.Subscribe(ctx, "mode-test-channel")
	if err != nil {
		t.Fatalf("client-wo failed to subscribe: %v", err)
	}
	defer subWO.Unsubscribe()

	subRW, err := cliRW.Subscribe(ctx, "mode-test-channel")
	if err != nil {
		t.Fatalf("client-rw failed to subscribe: %v", err)
	}
	defer subRW.Unsubscribe()

	// Direct-Message Handler einrichten
	roDirectReceived := make(chan *wsbrokerclient.Event, 5)
	unsubRODirect := cliRO.OnDirect(func(evt *wsbrokerclient.Event) {
		roDirectReceived <- evt
	})
	defer unsubRODirect()

	woDirectReceived := make(chan *wsbrokerclient.Event, 5)
	unsubWODirect := cliWO.OnDirect(func(evt *wsbrokerclient.Event) {
		woDirectReceived <- evt
	})
	defer unsubWODirect()

	rwDirectReceived := make(chan *wsbrokerclient.Event, 5)
	unsubRWDirect := cliRW.OnDirect(func(evt *wsbrokerclient.Event) {
		rwDirectReceived <- evt
	})
	defer unsubRWDirect()

	// Alle Clients synchron verbinden
	if err := cliRO.ConnectSync(ctx); err != nil {
		t.Fatalf("client-ro failed to connect: %v", err)
	}
	if err := cliWO.ConnectSync(ctx); err != nil {
		t.Fatalf("client-wo failed to connect: %v", err)
	}
	if err := cliRW.ConnectSync(ctx); err != nil {
		t.Fatalf("client-rw failed to connect: %v", err)
	}

	// Modi an den Client-Objekten verifizieren
	if cliRO.Mode() != wsbrokerclient.ModeRO {
		t.Errorf("expected clientRO mode to be %s, got %s", wsbrokerclient.ModeRO, cliRO.Mode())
	}
	if cliWO.Mode() != wsbrokerclient.ModeWO {
		t.Errorf("expected clientWO mode to be %s, got %s", wsbrokerclient.ModeWO, cliWO.Mode())
	}
	if cliRW.Mode() != wsbrokerclient.ModeRW {
		t.Errorf("expected clientRW mode to be %s, got %s", wsbrokerclient.ModeRW, cliRW.Mode())
	}

	// === Test-Szenario 1: ModeWO (Write-Only) sendet Broadcast an Kanal ===
	// Wenn WO an Kanal sendet:
	// - RO empfängt es
	// - RW empfängt es
	// - WO empfängt es NICHT
	woPayload := map[string]string{"msg": "broadcast from write-only client"}
	if err := cliWO.Broadcast(ctx, "mode-test-channel", woPayload); err != nil {
		t.Fatalf("client-wo failed to broadcast: %v", err)
	}

	select {
	case evt := <-subRO.Events():
		var data map[string]string
		if err := evt.UnmarshalData(&data); err != nil {
			t.Fatalf("failed to unmarshal WO broadcast on RO: %v", err)
		}
		if data["msg"] != "broadcast from write-only client" {
			t.Errorf("unexpected payload on RO: %v", data)
		}
		if evt.Source != "client-wo" {
			t.Errorf("expected source client-wo, got %s", evt.Source)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for WO broadcast on client-ro")
	}

	select {
	case evt := <-subRW.Events():
		var data map[string]string
		if err := evt.UnmarshalData(&data); err != nil {
			t.Fatalf("failed to unmarshal WO broadcast on RW: %v", err)
		}
		if data["msg"] != "broadcast from write-only client" {
			t.Errorf("unexpected payload on RW: %v", data)
		}
		if evt.Source != "client-wo" {
			t.Errorf("expected source client-wo, got %s", evt.Source)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for WO broadcast on client-rw")
	}

	select {
	case evt := <-subWO.Events():
		t.Fatalf("client-wo (ModeWO) should not have received broadcast event: %+v", evt)
	case <-time.After(300 * time.Millisecond):
		// Erwartet: WO empfängt nichts
	}

	// === Test-Szenario 2: ModeWO sendet Direct Message an ModeRO ===
	// Wenn WO Direktnachricht an RO sendet:
	// - RO empfängt Direktnachricht
	woDirectPayload := map[string]string{"secret": "direct from wo to ro"}
	if err := cliWO.SendDirect(ctx, "client-ro", woDirectPayload); err != nil {
		t.Fatalf("client-wo failed to send direct to client-ro: %v", err)
	}

	select {
	case evt := <-roDirectReceived:
		var data map[string]string
		if err := evt.UnmarshalData(&data); err != nil {
			t.Fatalf("failed to unmarshal direct payload on RO: %v", err)
		}
		if data["secret"] != "direct from wo to ro" {
			t.Errorf("unexpected direct payload on RO: %v", data)
		}
		if evt.Source != "client-wo" {
			t.Errorf("expected source client-wo, got %s", evt.Source)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for direct message on client-ro")
	}

	// === Test-Szenario 3: ModeRW sendet Broadcast an Kanal ===
	// Wenn RW an Kanal sendet:
	// - RO empfängt es
	// - RW empfängt es
	// - WO empfängt es NICHT
	rwPayload := map[string]string{"msg": "broadcast from read-write client"}
	if err := cliRW.Broadcast(ctx, "mode-test-channel", rwPayload); err != nil {
		t.Fatalf("client-rw failed to broadcast: %v", err)
	}

	select {
	case evt := <-subRO.Events():
		var data map[string]string
		if err := evt.UnmarshalData(&data); err != nil {
			t.Fatalf("failed to unmarshal RW broadcast on RO: %v", err)
		}
		if data["msg"] != "broadcast from read-write client" {
			t.Errorf("unexpected payload on RO: %v", data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for RW broadcast on client-ro")
	}

	select {
	case evt := <-subRW.Events():
		var data map[string]string
		if err := evt.UnmarshalData(&data); err != nil {
			t.Fatalf("failed to unmarshal RW broadcast on RW: %v", err)
		}
		if data["msg"] != "broadcast from read-write client" {
			t.Errorf("unexpected payload on RW: %v", data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for RW broadcast on client-rw")
	}

	select {
	case evt := <-subWO.Events():
		t.Fatalf("client-wo (ModeWO) should not receive channel broadcasts: %+v", evt)
	case <-time.After(300 * time.Millisecond):
		// Erwartet: WO empfängt nichts
	}

	// === Test-Szenario 4: Direktnachricht an ModeWO Client ===
	// Wenn RW Direktnachricht an WO sendet:
	// - Broker droppt Nachricht, da WO CanRead() == false
	rwToWOPayload := map[string]string{"secret": "direct to wo client"}
	if err := cliRW.SendDirect(ctx, "client-wo", rwToWOPayload); err != nil {
		t.Fatalf("client-rw SendDirect to client-wo returned error: %v", err)
	}

	select {
	case evt := <-woDirectReceived:
		t.Fatalf("client-wo (ModeWO) should not receive direct messages: %+v", evt)
	case <-time.After(300 * time.Millisecond):
		// Erwartet: WO empfängt nichts
	}

	// === Test-Szenario 5: ModeRO (Read-Only) versucht Nachrichten zu senden ===
	// Wenn RO versucht, Broadcast an Kanal zu senden:
	// - Broker Server empfängt Event, erkennt !CanWrite() und verwirft das Event
	// - Weder RW noch RO empfangen das verworfene Event
	roPayload := map[string]string{"msg": "broadcast from read-only client"}
	if err := cliRO.Broadcast(ctx, "mode-test-channel", roPayload); err != nil {
		t.Fatalf("client-ro Broadcast call returned unexpected error: %v", err)
	}

	select {
	case evt := <-subRW.Events():
		t.Fatalf("client-rw should NOT receive broadcast from read-only client: %+v", evt)
	case evt := <-subRO.Events():
		t.Fatalf("client-ro should NOT receive its own dropped broadcast: %+v", evt)
	case <-time.After(500 * time.Millisecond):
		// Erwartet: Broadcast wurde vom Server verworfen
	}

	// Wenn RO versucht, Direktnachricht an RW zu senden:
	// - Broker verwirft das Event
	roDirectData := map[string]string{"secret": "direct from read-only client"}
	if err := cliRO.SendDirect(ctx, "client-rw", roDirectData); err != nil {
		t.Fatalf("client-ro SendDirect call returned unexpected error: %v", err)
	}

	select {
	case evt := <-rwDirectReceived:
		t.Fatalf("client-rw should NOT receive direct message from read-only client: %+v", evt)
	case <-time.After(500 * time.Millisecond):
		// Erwartet: Direktnachricht wurde vom Server verworfen
	}
}
