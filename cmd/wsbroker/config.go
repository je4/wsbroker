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
	LocalAddr      string             `toml:"localaddr"`
	ExternalAddr   string             `toml:"externaladdr"`
	Bearer         config.EnvString   `toml:"bearer"`
	RestBearer     config.EnvString   `toml:"rest_bearer"`
	RestBearerAlt  config.EnvString   `toml:"restbearer"`
	WSBearer       config.EnvString   `toml:"ws_bearer"`
	WSBearerAlt    config.EnvString   `toml:"wsbearer"`
	WSAuthType     string             `toml:"ws_authtype"`
	WSAuthTypeAlt  string             `toml:"ws_auth_type"`
	WSAuthTypeAlt2 string             `toml:"wsauthtype"`
	NumWorkers     int                `toml:"num_workers"`
	ReadLimit      int64              `toml:"read_limit"`
	Debug          bool               `toml:"debug"`
	RESTTLS        loader.Config      `toml:"resttls"`
	Dashboard      DashboardConfig    `toml:"dashboard"`
	Log            stashconfig.Config `toml:"log"`
	AuthType       string             `toml:"authtype"`
	AuthTypeAlt    string             `toml:"auth_type"`
	NTPServer      string             `toml:"ntp_server"`
	NTPTimeout     config.Duration    `toml:"ntp_timeout"`
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

func resolveConfig(md toml.MetaData, conf *WSBrokerConfig) {
	if md.IsDefined("auth_type") && !md.IsDefined("authtype") {
		conf.AuthType = conf.AuthTypeAlt
	}
	if md.IsDefined("ws_auth_type") && !md.IsDefined("ws_authtype") && !md.IsDefined("wsauthtype") {
		conf.WSAuthType = conf.WSAuthTypeAlt
	} else if md.IsDefined("wsauthtype") && !md.IsDefined("ws_authtype") {
		conf.WSAuthType = conf.WSAuthTypeAlt2
	}
	if md.IsDefined("restbearer") && !md.IsDefined("rest_bearer") {
		conf.RestBearer = conf.RestBearerAlt
	}
	if md.IsDefined("wsbearer") && !md.IsDefined("ws_bearer") {
		conf.WSBearer = conf.WSBearerAlt
	}

	hasRestBearer := md.IsDefined("rest_bearer") || md.IsDefined("restbearer")
	hasWSBearer := md.IsDefined("ws_bearer") || md.IsDefined("wsbearer")
	hasWSAuthType := md.IsDefined("ws_authtype") || md.IsDefined("ws_auth_type") || md.IsDefined("wsauthtype")
	hasLegacyBearer := md.IsDefined("bearer")
	hasLegacyAuthType := md.IsDefined("authtype") || md.IsDefined("auth_type")

	if hasLegacyBearer && !hasRestBearer {
		conf.RestBearer = conf.Bearer
	}
	if hasLegacyBearer && !hasWSBearer {
		conf.WSBearer = conf.Bearer
	}
	if hasLegacyAuthType && !hasWSAuthType {
		conf.WSAuthType = conf.AuthType
	}

	if conf.WSAuthType == "" {
		if conf.AuthType != "" {
			conf.WSAuthType = conf.AuthType
		} else {
			conf.WSAuthType = "bearer"
		}
	}
	if conf.AuthType == "" {
		conf.AuthType = conf.WSAuthType
	}
	if conf.RestBearer.String() == "" && conf.Bearer.String() != "" {
		conf.RestBearer = conf.Bearer
	}
	if conf.WSBearer.String() == "" && conf.Bearer.String() != "" {
		conf.WSBearer = conf.Bearer
	}
}

// LoadWSBrokerConfig loads configuration defaults and decodes the TOML file at the specified path
// or from standard search locations.
func LoadWSBrokerConfig(fp string, conf *WSBrokerConfig) error {
	md, err := toml.Decode(localconfig.DefaultConfig, conf)
	if err != nil {
		return errors.Wrap(err, "error decoding default config")
	}
	resolveConfig(md, conf)

	if fp != "" {
		fp = expandHomedir(fp)
		mdFile, err := toml.DecodeFile(fp, conf)
		if err != nil {
			return errors.Wrapf(err, "error loading config file %v", fp)
		}
		resolveConfig(mdFile, conf)
	} else {
		for _, fp := range configdirs {
			fp = expandHomedir(fp)
			mdFile, err := toml.DecodeFile(fp, conf)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				return errors.Wrapf(err, "error loading config file %v", fp)
			}
			resolveConfig(mdFile, conf)

			break
		}
	}
	if conf.ReadLimit <= 0 {
		conf.ReadLimit = 16 * 1024 * 1024
	}
	return nil
}
