package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadWSBrokerConfig_AuthType(t *testing.T) {
	// 1. Default config check
	confDefault := &WSBrokerConfig{}
	if err := LoadWSBrokerConfig("", confDefault); err != nil {
		t.Fatalf("failed to load default config: %v", err)
	}
	if confDefault.AuthType != "bearer" {
		t.Errorf("expected default AuthType to be 'bearer', got %q", confDefault.AuthType)
	}

	// 2. Config with authtype = "mtls"
	tmpDir := t.TempDir()
	tomlPath1 := filepath.Join(tmpDir, "conf1.toml")
	if err := os.WriteFile(tomlPath1, []byte(`authtype = "mtls"`), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	conf1 := &WSBrokerConfig{}
	if err := LoadWSBrokerConfig(tomlPath1, conf1); err != nil {
		t.Fatalf("failed to load config 1: %v", err)
	}
	if conf1.AuthType != "mtls" {
		t.Errorf("expected AuthType to be 'mtls', got %q", conf1.AuthType)
	}

	// 3. Config with auth_type = "both"
	tomlPath2 := filepath.Join(tmpDir, "conf2.toml")
	if err := os.WriteFile(tomlPath2, []byte(`auth_type = "both"`), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	conf2 := &WSBrokerConfig{}
	if err := LoadWSBrokerConfig(tomlPath2, conf2); err != nil {
		t.Fatalf("failed to load config 2: %v", err)
	}
	if conf2.AuthType != "both" {
		t.Errorf("expected AuthType to be 'both', got %q", conf2.AuthType)
	}
}

func TestLoadWSBrokerConfig_ReadLimit(t *testing.T) {
	// 1. Default config check (16 MiB)
	confDefault := &WSBrokerConfig{}
	if err := LoadWSBrokerConfig("", confDefault); err != nil {
		t.Fatalf("failed to load default config: %v", err)
	}
	expectedDefault := int64(16 * 1024 * 1024)
	if confDefault.ReadLimit != expectedDefault {
		t.Errorf("expected default ReadLimit to be %d, got %d", expectedDefault, confDefault.ReadLimit)
	}

	// 2. Explicit custom read_limit
	tmpDir := t.TempDir()
	tomlPath1 := filepath.Join(tmpDir, "conf1.toml")
	if err := os.WriteFile(tomlPath1, []byte(`read_limit = 65536`), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	conf1 := &WSBrokerConfig{}
	if err := LoadWSBrokerConfig(tomlPath1, conf1); err != nil {
		t.Fatalf("failed to load config 1: %v", err)
	}
	if conf1.ReadLimit != 65536 {
		t.Errorf("expected ReadLimit to be 65536, got %d", conf1.ReadLimit)
	}

	// 3. Zero or negative read_limit defaults to 16 MiB
	tomlPath2 := filepath.Join(tmpDir, "conf2.toml")
	if err := os.WriteFile(tomlPath2, []byte(`read_limit = 0`), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	conf2 := &WSBrokerConfig{}
	if err := LoadWSBrokerConfig(tomlPath2, conf2); err != nil {
		t.Fatalf("failed to load config 2: %v", err)
	}
	if conf2.ReadLimit != expectedDefault {
		t.Errorf("expected ReadLimit to default to %d, got %d", expectedDefault, conf2.ReadLimit)
	}
}

func TestLoadWSBrokerConfig_DecoupledAuth(t *testing.T) {
	// 1. Independent rest_bearer and ws_bearer
	tmpDir := t.TempDir()
	tomlPath1 := filepath.Join(tmpDir, "conf1.toml")
	tomlContent1 := `
rest_bearer = "rest-secret-123"
ws_bearer = "ws-secret-456"
ws_authtype = "both"
`
	if err := os.WriteFile(tomlPath1, []byte(tomlContent1), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	conf1 := &WSBrokerConfig{}
	if err := LoadWSBrokerConfig(tomlPath1, conf1); err != nil {
		t.Fatalf("failed to load config 1: %v", err)
	}
	if conf1.RestBearer.String() != "rest-secret-123" {
		t.Errorf("expected RestBearer to be 'rest-secret-123', got %q", conf1.RestBearer.String())
	}
	if conf1.WSBearer.String() != "ws-secret-456" {
		t.Errorf("expected WSBearer to be 'ws-secret-456', got %q", conf1.WSBearer.String())
	}
	if conf1.WSAuthType != "both" {
		t.Errorf("expected WSAuthType to be 'both', got %q", conf1.WSAuthType)
	}

	// 2. Fallback when only legacy bearer and authtype are defined
	tomlPath2 := filepath.Join(tmpDir, "conf2.toml")
	tomlContent2 := `
bearer = "legacy-secret-999"
authtype = "mtls"
`
	if err := os.WriteFile(tomlPath2, []byte(tomlContent2), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	conf2 := &WSBrokerConfig{}
	if err := LoadWSBrokerConfig(tomlPath2, conf2); err != nil {
		t.Fatalf("failed to load config 2: %v", err)
	}
	if conf2.RestBearer.String() != "legacy-secret-999" {
		t.Errorf("expected RestBearer fallback to be 'legacy-secret-999', got %q", conf2.RestBearer.String())
	}
	if conf2.WSBearer.String() != "legacy-secret-999" {
		t.Errorf("expected WSBearer fallback to be 'legacy-secret-999', got %q", conf2.WSBearer.String())
	}
	if conf2.WSAuthType != "mtls" {
		t.Errorf("expected WSAuthType fallback to be 'mtls', got %q", conf2.WSAuthType)
	}

	// 3. Alternate key naming: restbearer, wsbearer, ws_auth_type, wsauthtype
	tomlPath3 := filepath.Join(tmpDir, "conf3.toml")
	tomlContent3 := `
restbearer = "rest-alt-key"
wsbearer = "ws-alt-key"
ws_auth_type = "mtls"
`
	if err := os.WriteFile(tomlPath3, []byte(tomlContent3), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	conf3 := &WSBrokerConfig{}
	if err := LoadWSBrokerConfig(tomlPath3, conf3); err != nil {
		t.Fatalf("failed to load config 3: %v", err)
	}
	if conf3.RestBearer.String() != "rest-alt-key" {
		t.Errorf("expected RestBearer to be 'rest-alt-key', got %q", conf3.RestBearer.String())
	}
	if conf3.WSBearer.String() != "ws-alt-key" {
		t.Errorf("expected WSBearer to be 'ws-alt-key', got %q", conf3.WSBearer.String())
	}
	if conf3.WSAuthType != "mtls" {
		t.Errorf("expected WSAuthType to be 'mtls', got %q", conf3.WSAuthType)
	}
}
