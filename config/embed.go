// Package config provides default embedded TOML configuration templates for WSBroker.
package config

import _ "embed"

// DefaultConfig contains the embedded default wsbroker.toml configuration.
//
//go:embed wsbroker.toml
var DefaultConfig string
