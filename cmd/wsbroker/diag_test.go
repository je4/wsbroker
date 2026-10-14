package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

func TestCertLoader(t *testing.T) {
	_ = os.Chdir("C:/daten/go/dev/wsbroker")
	conf := &WSBrokerConfig{}
	err := LoadWSBrokerConfig("config/wsbroker_local.toml", conf)
	if err != nil {
		t.Fatalf("LoadWSBrokerConfig failed: %v", err)
	}
	t.Logf("Loaded config: %+v", conf.RESTTLS)
	zl := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
	restTLSConfig, restLoader, err := loader.CreateServerLoader(false, &conf.RESTTLS, nil, &zl)
	if err != nil {
		t.Fatalf("CreateServerLoader failed: %v", err)
	}
	defer restLoader.Close()
	t.Logf("Success! restTLSConfig: %v", restTLSConfig)
}

func TestMinivaultConnection(t *testing.T) {
	host := "localhost"
	port := "8759"
	targetAddr := net.JoinHostPort(host, port)

	caFiles := []string{
		"certs/minivault.ca.pem",
		"../../certs/minivault.ca.pem",
		"C:/daten/go/dev/wsbroker/certs/minivault.ca.pem",
	}
	var caFile string
	var caData []byte
	var err error
	for _, p := range caFiles {
		caData, err = os.ReadFile(p)
		if err == nil {
			caFile = p
			break
		}
	}

	requestURL := fmt.Sprintf("https://%s/api/v1/auth/token/get", targetAddr)
	parentToken := "pa.22f30006b50fc01b.345fd4e003624ba2"

	fmt.Println("\n================= 1. TCP & FIREWALL CHECK =================")
	conn, err := net.DialTimeout("tcp", targetAddr, 3*time.Second)
	if err != nil {
		t.Fatalf("❌ TCP-Verbindung fehlgeschlagen (Firewall blockiert oder Port geschlossen): %v", err)
	}
	fmt.Printf("✅ TCP-Verbindung erfolgreich aufgebaut zu %s\n", targetAddr)
	_ = conn.Close()

	fmt.Println("\n============= 2. TLS-HANDSHAKE (UNGEPRÜFT) ================")
	insecureTLSConfig := &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: true, // Ignoriert Zertifikatsvalidierung, prüft reinen TLS-Handshake
	}
	tlsConn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", targetAddr, insecureTLSConfig)
	if err != nil {
		t.Fatalf("❌ TLS-Handshake fehlgeschlagen (Server spricht eventuell nur HTTP, schließt TLS sofort oder Firewall/Proxy killt Handshake): %v", err)
	}
	defer tlsConn.Close()

	state := tlsConn.ConnectionState()
	fmt.Printf("✅ TLS-Handshake erfolgreich! (TLS-Version: %04x, CipherSuite: %04x)\n", state.Version, state.CipherSuite)
	if len(state.PeerCertificates) > 0 {
		cert := state.PeerCertificates[0]
		fmt.Printf("   Zertifikat Subject: %s\n", cert.Subject)
		fmt.Printf("   Zertifikat Issuer:  %s\n", cert.Issuer)
		fmt.Printf("   Gültig ab:          %s\n", cert.NotBefore)
		fmt.Printf("   Gültig bis:         %s\n", cert.NotAfter)
		fmt.Printf("   DNS Names:          %v\n", cert.DNSNames)
		fmt.Printf("   IP Addresses:       %v\n", cert.IPAddresses)
	}

	fmt.Println("\n============= 3. ZERTIFIKAT & CA-VALIDIERUNG ==============")
	if len(caData) == 0 {
		t.Fatalf("❌ CA-Datei konnte nicht gefunden werden unter Pfaden: %v", caFiles)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caData) {
		t.Fatalf("❌ CA-Zertifikat konnte nicht geparst werden (ungültiges PEM-Format)")
	}
	fmt.Printf("✅ CA-Datei '%s' erfolgreich geladen\n", caFile)

	secureTLSConfig := &tls.Config{
		ServerName: host,
		RootCAs:    rootCAs,
	}
	verifiedTLSConn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", targetAddr, secureTLSConfig)
	if err != nil {
		t.Fatalf("❌ Zertifikatsvalidierung fehlgeschlagen: %v", err)
	}
	_ = verifiedTLSConn.Close()
	fmt.Println("✅ Zertifikatskette und Hostname erfolgreich verifiziert!")

	fmt.Println("\n============= 4. RAW HTTP ÜBER VERIFIZIERTES TLS =========")
	verifiedConn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", targetAddr, secureTLSConfig)
	if err != nil {
		t.Fatalf("❌ TLS Dial fehlgeschlagen: %v", err)
	}
	defer verifiedConn.Close()

	rawReq := "GET /api/v1/auth/token/get HTTP/1.1\r\nHost: localhost:8759\r\nUser-Agent: diag-test\r\nAccept: */*\r\nConnection: close\r\n\r\n"
	_, err = verifiedConn.Write([]byte(rawReq))
	if err != nil {
		t.Fatalf("❌ Fehler beim Senden des Raw HTTP Requests: %v", err)
	}
	fmt.Println("✅ Raw HTTP Request erfolgreich gesendet. Warte auf Antwort...")

	buf := make([]byte, 4096)
	_ = verifiedConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, readErr := verifiedConn.Read(buf)
	if readErr != nil {
		fmt.Printf("❌ Fehler beim Lesen der Antwort: %v (gelesene Bytes: %d)\n", readErr, n)
	} else {
		fmt.Printf("✅ Antwort erhalten (%d Bytes):\n%s\n", n, string(buf[:n]))
	}

	fmt.Println("\n============= 5. HTTP-REQUEST TEST (OHNE AUTH) ============")
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: secureTLSConfig,
		},
		Timeout: 5 * time.Second,
	}

	req, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		t.Fatalf("Fehler beim Erstellen des Requests: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("❌ HTTP-Request fehlgeschlagen mit Fehler: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	fmt.Printf("✅ HTTP-Status: %s\n", resp.Status)
	maxLen := 200
	if len(body) < maxLen {
		maxLen = len(body)
	}
	fmt.Printf("   Response Body (erste 200 Zeichen): %s\n", string(body[:maxLen]))

	fmt.Println("\n============= 5. HTTP-REQUEST TEST (MIT BEARER TOKEN) =====")
	req2, err := http.NewRequest("GET", requestURL, nil)
	if err != nil {
		t.Fatalf("Fehler beim Erstellen des Requests: %v", err)
	}
	req2.Header.Set("Authorization", "Bearer "+parentToken)

	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("❌ HTTP-Request mit Token fehlgeschlagen mit Fehler: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()
	fmt.Printf("✅ HTTP-Status: %s\n", resp2.Status)
	maxLen2 := 200
	if len(body2) < maxLen2 {
		maxLen2 = len(body2)
	}
	fmt.Printf("   Response Body (erste 200 Zeichen): %s\n", string(body2[:maxLen2]))
}
