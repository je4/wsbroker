// Package main is the entry point and server binary for the WSBroker application.
package main

import (
	"os"
	"path/filepath"
	"strings"

	"emperror.dev/errors"
	"github.com/BurntSushi/toml"
	"github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/stashconfig"
	localconfig "github.com/je4/wsbroker/config"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

// DashboardConfig holds configuration settings for the web dashboard.
type DashboardConfig struct {
	Enabled     bool             `toml:"enabled"`
	Username    config.EnvString `toml:"username"`
	Password    config.EnvString `toml:"password"`
	AllowBearer bool             `toml:"allow_bearer"`
}

// WSBrokerConfig holds the complete configuration options for running the WSBroker service.
type WSBrokerConfig struct {
	LocalAddr    string             `toml:"localaddr"`
	ExternalAddr string             `toml:"externaladdr"`
	Bearer       config.EnvString   `toml:"bearer"`
	NumWorkers   int                `toml:"num_workers"`
	Debug        bool               `toml:"debug"`
	RESTTLS      loader.Config      `toml:"resttls"`
	Dashboard    DashboardConfig    `toml:"dashboard"`
	Log          stashconfig.Config `toml:"log"`
	AuthType     string             `toml:"authtype"`
	AuthTypeAlt  string             `toml:"auth_type"`
}

var configdirs = []string{"~/.wsbroker.toml", "/etc/wsbroker.toml", "/opt/wsbroker/wsbroker.toml"}

// expandHomedir resolves home directory prefix "~/" in file paths.
func expandHomedir(fp string) string {
	if strings.HasPrefix(fp, "~/") {
		dirname, _ := os.UserHomeDir()
		fp = filepath.Join(dirname, fp[2:])
	}
	return fp
}

// LoadWSBrokerConfig loads configuration defaults and decodes the TOML file at the specified path
// or from standard search locations.
func LoadWSBrokerConfig(fp string, conf *WSBrokerConfig) error {
	md, err := toml.Decode(localconfig.DefaultConfig, conf)
	if err != nil {
		return errors.Wrap(err, "error decoding default config")
	}
	if md.IsDefined("auth_type") && !md.IsDefined("authtype") {
		conf.AuthType = conf.AuthTypeAlt
	}
	if fp != "" {
		fp = expandHomedir(fp)
		mdFile, err := toml.DecodeFile(fp, conf)
		if err != nil {
			return errors.Wrapf(err, "error loading config file %v", fp)
		}
		if mdFile.IsDefined("auth_type") && !mdFile.IsDefined("authtype") {
			conf.AuthType = conf.AuthTypeAlt
		}
		return nil
	}
	for _, fp := range configdirs {
		fp = expandHomedir(fp)
		mdFile, err := toml.DecodeFile(fp, conf)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return errors.Wrapf(err, "error loading config file %v", fp)
		}
		if mdFile.IsDefined("auth_type") && !mdFile.IsDefined("authtype") {
			conf.AuthType = conf.AuthTypeAlt
		}

		break
	}
	return nil
}
