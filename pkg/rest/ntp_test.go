package rest_test

import (
	"context"
	"net"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/je4/wsbroker/pkg/rest"
	"github.com/je4/wsbrokerclient/pkg/client"
	"github.com/je4/wsbrokerclient/pkg/event"
	"github.com/rs/zerolog"
)

// setupMockUDPServer starts a local UDP echo-like mock NTP server.
func setupMockUDPServer(t *testing.T, handler func(query []byte) []byte) (string, func()) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock UDP server: %v", err)
	}

	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		buf := make([]byte, 2048)
		for {
			_ = pc.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				select {
				case <-stopCh:
					return
				default:
					continue
				}
			}

			resp := handler(buf[:n])
			if resp != nil {
				_, _ = pc.WriteTo(resp, addr)
			}
		}
	}()

	addr := pc.LocalAddr().String()
	cleanup := func() {
		close(stopCh)
		wg.Wait()
		_ = pc.Close()
	}

	return addr, cleanup
}

func TestNTPForwarder_DirectUDP(t *testing.T) {
	// 1. Mock NTP UDP server returning 48 bytes
	mockAddr, cleanup := setupMockUDPServer(t, func(query []byte) []byte {
		if len(query) != 48 {
			t.Errorf("expected 48 query bytes, got %d", len(query))
		}
		resp := make([]byte, 48)
		copy(resp, query)
		resp[0] = 0x24 // Server response mode
		return resp
	})
	defer cleanup()

	host, port, _ := net.SplitHostPort(mockAddr)
	forwarder := rest.NewNTPConnection(host, "udp", port, "", 0, 2*time.Second)

	query := make([]byte, 48)
	query[0] = 0x1b // Client query mode

	resp, err := forwarder(query)
	if err != nil {
		t.Fatalf("NTP forwarder failed: %v", err)
	}

	if len(resp) != 48 {
		t.Fatalf("expected 48 bytes response, got %d", len(resp))
	}
	if resp[0] != 0x24 {
		t.Errorf("expected response first byte 0x24, got 0x%02x", resp[0])
	}
}

func TestNTPWebSocket_EndToEnd(t *testing.T) {
	nopLogger := zerolog.Nop()
	var logger zLogger.ZLogger = &nopLogger

	ctrl, err := rest.NewController("127.0.0.1:0", "http://127.0.0.1:0", nil, "test-token", false, logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}

	ctrl.GetBroker().Start(4)
	defer ctrl.GracefulStop()

	// Mock forwarder
	ctrl.SetNTPForwarder(func(data []byte) ([]byte, error) {
		resp := make([]byte, 48)
		copy(resp, data)
		resp[0] = 0x24
		resp[1] = 0x01 // Stratum 1
		return resp, nil
	})

	server := httptest.NewServer(ctrl.GetRouter())
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http://", "ws://", 1) + "/ws/client-ntp/rw?token=test-token"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("failed to connect websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	// Send NTP query
	queryBytes := make([]byte, 48)
	queryBytes[0] = 0x1b
	queryEvt, err := event.NewRawEvent(event.TypeNTPQuery, "client-ntp", "wsbroker", queryBytes)
	if err != nil {
		t.Fatalf("failed to create ntp query event: %v", err)
	}

	if err := wsjson.Write(ctx, conn, queryEvt); err != nil {
		t.Fatalf("failed to write ntp query event: %v", err)
	}

	// Read response
	var respEvt event.RawEvent
	if err := wsjson.Read(ctx, conn, &respEvt); err != nil {
		t.Fatalf("failed to read event: %v", err)
	}

	if respEvt.Type != event.TypeNTPResponse {
		t.Fatalf("expected event type %s, got %s", event.TypeNTPResponse, respEvt.Type)
	}
	if respEvt.RefID != queryEvt.ID {
		t.Errorf("expected RefID %s, got %s", queryEvt.ID, respEvt.RefID)
	}
	if respEvt.ID == "" || respEvt.ID == queryEvt.ID {
		t.Errorf("expected unique response ID, got %s (query ID: %s)", respEvt.ID, queryEvt.ID)
	}

	data, err := respEvt.GetData()
	if err != nil {
		t.Fatalf("failed to get response data: %v", err)
	}

	rawResp, ok := data.([]byte)
	if !ok || len(rawResp) != 48 {
		t.Fatalf("expected 48 byte slice, got %v (%T)", data, data)
	}
	if rawResp[0] != 0x24 || rawResp[1] != 0x01 {
		t.Errorf("unexpected response header bytes: 0x%02x 0x%02x", rawResp[0], rawResp[1])
	}
}

func TestNTPWebSocket_ErrorWhenNotConfigured(t *testing.T) {
	nopLogger := zerolog.Nop()
	var logger zLogger.ZLogger = &nopLogger

	ctrl, err := rest.NewController("127.0.0.1:0", "http://127.0.0.1:0", nil, "test-token", false, logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	ctrl.GetBroker().Start(4)
	defer ctrl.GracefulStop()
	// Explicitly no NTP configured

	server := httptest.NewServer(ctrl.GetRouter())
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http://", "ws://", 1) + "/ws/client-ntp-err/rw?token=test-token"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("failed to connect websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	queryBytes := make([]byte, 48)
	queryEvt, err := event.NewRawEvent(event.TypeNTPQuery, "client-ntp-err", "wsbroker", queryBytes)
	if err != nil {
		t.Fatalf("failed to create ntp query event: %v", err)
	}

	if err := wsjson.Write(ctx, conn, queryEvt); err != nil {
		t.Fatalf("failed to write ntp query event: %v", err)
	}

	var respEvt event.RawEvent
	if err := wsjson.Read(ctx, conn, &respEvt); err != nil {
		t.Fatalf("failed to read event: %v", err)
	}

	if respEvt.Type != event.TypeNTPError {
		t.Fatalf("expected event type %s, got %s", event.TypeNTPError, respEvt.Type)
	}
	if respEvt.RefID != queryEvt.ID {
		t.Errorf("expected RefID %s, got %s", queryEvt.ID, respEvt.RefID)
	}
	if respEvt.ID == "" || respEvt.ID == queryEvt.ID {
		t.Errorf("expected unique response ID, got %s (query ID: %s)", respEvt.ID, queryEvt.ID)
	}
}

func TestNTPWebSocket_ClientQueryNTPMethod(t *testing.T) {
	nopLogger := zerolog.Nop()
	var logger zLogger.ZLogger = &nopLogger

	ctrl, err := rest.NewController("127.0.0.1:0", "http://127.0.0.1:0", nil, "test-token", false, logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	ctrl.GetBroker().Start(4)
	defer ctrl.GracefulStop()

	ctrl.SetNTPForwarder(func(data []byte) ([]byte, error) {
		resp := make([]byte, 48)
		copy(resp, data)
		resp[0] = 0x24
		resp[1] = 0x02 // Stratum 2
		return resp, nil
	})

	server := httptest.NewServer(ctrl.GetRouter())
	defer server.Close()

	wsClient, err := client.New(client.Config{
		Endpoint:   server.URL,
		ClientName: "test-client-helper",
		Bearer:     "test-token",
		Mode:       client.ModeRW,
	})
	if err != nil {
		t.Fatalf("failed to create wsbroker client: %v", err)
	}
	defer wsClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := wsClient.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect wsbroker client: %v", err)
	}

	query := make([]byte, 48)
	query[0] = 0x1b

	resp, err := wsClient.QueryNTP(ctx, query)
	if err != nil {
		t.Fatalf("QueryNTP failed: %v", err)
	}

	if len(resp) != 48 {
		t.Fatalf("expected 48 bytes response, got %d", len(resp))
	}
	if resp[0] != 0x24 || resp[1] != 0x02 {
		t.Errorf("unexpected NTP response bytes: 0x%02x 0x%02x", resp[0], resp[1])
	}
}

func TestNTPWebSocket_SourceMismatchForbidden(t *testing.T) {
	nopLogger := zerolog.Nop()
	var logger zLogger.ZLogger = &nopLogger

	ctrl, err := rest.NewController("127.0.0.1:0", "http://127.0.0.1:0", nil, "test-token", false, logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	ctrl.GetBroker().Start(4)
	defer ctrl.GracefulStop()

	ctrl.SetNTPForwarder(func(data []byte) ([]byte, error) {
		return data, nil
	})

	server := httptest.NewServer(ctrl.GetRouter())
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http://", "ws://", 1) + "/ws/real-client/rw?token=test-token"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("failed to connect websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	// Impersonating another client name
	queryBytes := make([]byte, 48)
	queryEvt, err := event.NewRawEvent(event.TypeNTPQuery, "impersonated-client", "wsbroker", queryBytes)
	if err != nil {
		t.Fatalf("failed to create ntp query event: %v", err)
	}

	if err := wsjson.Write(ctx, conn, queryEvt); err != nil {
		t.Fatalf("failed to write ntp query event: %v", err)
	}

	var respEvt event.RawEvent
	if err := wsjson.Read(ctx, conn, &respEvt); err != nil {
		t.Fatalf("failed to read event: %v", err)
	}

	if respEvt.Type != event.TypeNTPError {
		t.Fatalf("expected event type %s on source mismatch, got %s", event.TypeNTPError, respEvt.Type)
	}
	if respEvt.RefID != queryEvt.ID {
		t.Errorf("expected RefID %s, got %s", queryEvt.ID, respEvt.RefID)
	}
	if respEvt.ID == "" || respEvt.ID == queryEvt.ID {
		t.Errorf("expected unique response ID, got %s (query ID: %s)", respEvt.ID, queryEvt.ID)
	}
}

func TestNTPWebSocket_ForwarderError(t *testing.T) {
	nopLogger := zerolog.Nop()
	var logger zLogger.ZLogger = &nopLogger

	ctrl, err := rest.NewController("127.0.0.1:0", "http://127.0.0.1:0", nil, "test-token", false, logger)
	if err != nil {
		t.Fatalf("failed to create controller: %v", err)
	}
	ctrl.GetBroker().Start(4)
	defer ctrl.GracefulStop()

	ctrl.SetNTPForwarder(func(data []byte) ([]byte, error) {
		return nil, net.UnknownNetworkError("network failure")
	})

	server := httptest.NewServer(ctrl.GetRouter())
	defer server.Close()

	wsURL := strings.Replace(server.URL, "http://", "ws://", 1) + "/ws/client-err-test/rw?token=test-token"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("failed to connect websocket: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	queryBytes := make([]byte, 48)
	queryEvt, err := event.NewRawEvent(event.TypeNTPQuery, "client-err-test", "wsbroker", queryBytes)
	if err != nil {
		t.Fatalf("failed to create ntp query event: %v", err)
	}

	if err := wsjson.Write(ctx, conn, queryEvt); err != nil {
		t.Fatalf("failed to write ntp query event: %v", err)
	}

	var respEvt event.RawEvent
	if err := wsjson.Read(ctx, conn, &respEvt); err != nil {
		t.Fatalf("failed to read event: %v", err)
	}

	if respEvt.Type != event.TypeNTPError {
		t.Fatalf("expected event type %s on forwarder error, got %s", event.TypeNTPError, respEvt.Type)
	}
	if respEvt.RefID != queryEvt.ID {
		t.Errorf("expected RefID %s, got %s", queryEvt.ID, respEvt.RefID)
	}
	if respEvt.ID == "" || respEvt.ID == queryEvt.ID {
		t.Errorf("expected unique response ID, got %s (query ID: %s)", respEvt.ID, queryEvt.ID)
	}
}
