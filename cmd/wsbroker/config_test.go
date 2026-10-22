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
