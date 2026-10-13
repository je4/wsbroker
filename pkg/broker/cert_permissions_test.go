package broker

import (
	"crypto/x509"
	"net/url"
	"testing"

	"github.com/je4/wsbrokerclient"
)

func TestParseCertPermissions(t *testing.T) {
	u1, _ := url.Parse("wsbroker:client:sensor-1")
	u2, _ := url.Parse("wsbroker:client:sensor-2")
	u3, _ := url.Parse("wsbroker:channel:telemetry:rw")
	u4, _ := url.Parse("wsbroker:channel:alerts:ro")
	u5, _ := url.Parse("wsbroker:channel:logs:wo")
	uOther, _ := url.Parse("https://example.com/other")

	cert := &x509.Certificate{
		URIs: []*url.URL{u1, u2, u3, u4, u5, uOther},
	}

	cp := ParseCertPermissions(cert)
	if !cp.HasRestrictions() {
		t.Fatalf("expected restrictions to be present")
	}

	// Clients
	if !cp.CanAccessClient("sensor-1") {
		t.Errorf("expected access for sensor-1")
	}
	if !cp.CanAccessClient("sensor-2") {
		t.Errorf("expected access for sensor-2")
	}
	if cp.CanAccessClient("sensor-3") {
		t.Errorf("expected access denied for sensor-3")
	}

	// Channel telemetry (rw)
	if !cp.CanAccessChannel("telemetry", wsbrokerclient.ModeRW) {
		t.Errorf("telemetry rw should allow rw")
	}
	if !cp.CanAccessChannel("telemetry", wsbrokerclient.ModeRO) {
		t.Errorf("telemetry rw should allow ro")
	}
	if !cp.CanAccessChannel("telemetry", wsbrokerclient.ModeWO) {
		t.Errorf("telemetry rw should allow wo")
	}

	// Channel alerts (ro)
	if !cp.CanAccessChannel("alerts", wsbrokerclient.ModeRO) {
		t.Errorf("alerts ro should allow ro")
	}
	if cp.CanAccessChannel("alerts", wsbrokerclient.ModeRW) {
		t.Errorf("alerts ro should not allow rw")
	}
	if cp.CanAccessChannel("alerts", wsbrokerclient.ModeWO) {
		t.Errorf("alerts ro should not allow wo")
	}

	// Channel logs (wo)
	if !cp.CanAccessChannel("logs", wsbrokerclient.ModeWO) {
		t.Errorf("logs wo should allow wo")
	}
	if cp.CanAccessChannel("logs", wsbrokerclient.ModeRW) {
		t.Errorf("logs wo should not allow rw")
	}
	if cp.CanAccessChannel("logs", wsbrokerclient.ModeRO) {
		t.Errorf("logs wo should not allow ro")
	}

	// Non-existent channel
	if cp.CanAccessChannel("secret", wsbrokerclient.ModeRO) {
		t.Errorf("unlisted channel should not be accessible")
	}
}

func TestCertPermissionsWildcards(t *testing.T) {
	uClientWildcard, _ := url.Parse("wsbroker:client:*")
	uChannelWildcard, _ := url.Parse("wsbroker:channel:*:ro")

	cert := &x509.Certificate{
		URIs: []*url.URL{uClientWildcard, uChannelWildcard},
	}

	cp := ParseCertPermissions(cert)
	if !cp.CanAccessClient("any-client") {
		t.Errorf("expected wildcard client access")
	}
	if !cp.CanAccessChannel("any-channel", wsbrokerclient.ModeRO) {
		t.Errorf("expected wildcard channel access for ro")
	}
	if cp.CanAccessChannel("any-channel", wsbrokerclient.ModeRW) {
		t.Errorf("wildcard ro should not allow rw")
	}
}

func TestCertPermissionsEmptyAndNil(t *testing.T) {
	var nilCP *CertPermissions
	if nilCP.HasRestrictions() {
		t.Errorf("nil CP should not have restrictions")
	}
	if !nilCP.CanAccessClient("client1") {
		t.Errorf("nil CP should allow client")
	}
	if !nilCP.CanAccessChannel("ch1", wsbrokerclient.ModeRW) {
		t.Errorf("nil CP should allow channel")
	}

	cpEmpty := ParseCertPermissions(&x509.Certificate{})
	if cpEmpty.HasRestrictions() {
		t.Errorf("empty cert should not have restrictions")
	}
	if !cpEmpty.CanAccessClient("client1") {
		t.Errorf("empty cert should allow client")
	}
	if !cpEmpty.CanAccessChannel("ch1", wsbrokerclient.ModeRW) {
		t.Errorf("empty cert should allow channel")
	}
}

func TestIsModeAllowed(t *testing.T) {
	tests := []struct {
		granted   wsbrokerclient.ConnectionMode
		requested wsbrokerclient.ConnectionMode
		allowed   bool
	}{
		{wsbrokerclient.ModeRW, wsbrokerclient.ModeRW, true},
		{wsbrokerclient.ModeRW, wsbrokerclient.ModeRO, true},
		{wsbrokerclient.ModeRW, wsbrokerclient.ModeWO, true},
		{wsbrokerclient.ModeRO, wsbrokerclient.ModeRO, true},
		{wsbrokerclient.ModeRO, wsbrokerclient.ModeRW, false},
		{wsbrokerclient.ModeRO, wsbrokerclient.ModeWO, false},
		{wsbrokerclient.ModeWO, wsbrokerclient.ModeWO, true},
		{wsbrokerclient.ModeWO, wsbrokerclient.ModeRW, false},
		{wsbrokerclient.ModeWO, wsbrokerclient.ModeRO, false},
	}

	for _, tt := range tests {
		got := IsModeAllowed(tt.granted, tt.requested)
		if got != tt.allowed {
			t.Errorf("IsModeAllowed(%v, %v) = %v; want %v", tt.granted, tt.requested, got, tt.allowed)
		}
	}
}
