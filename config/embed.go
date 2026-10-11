package config

import _ "embed"

//go:embed wsbroker.toml
var DefaultConfig string
