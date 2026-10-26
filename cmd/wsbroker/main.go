// Package main is the entry point and server binary for the WSBroker application.
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/je4/wsbroker/pkg/rest"
	ublogger "gitlab.switch.ch/ub-unibas/go-ublogger/v2"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

var configfile = flag.String("config", "", "location of toml configuration file")
var dashboard = flag.Bool("dashboard", false, "start dashboard")

// main initializes configuration, logging, TLS certificates, and starts the WSBroker controller.
func main() {
	flag.Parse()

	conf := &WSBrokerConfig{}
	if err := LoadWSBrokerConfig(*configfile, conf); err != nil {
		log.Fatalf("cannot load toml from %s: %v", *configfile, err)
	}

	// create logger instance
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("cannot get hostname: %v", err)
	}

	var loggerTLSConfig *tls.Config
	var loggerLoader io.Closer
	if conf.Log.Stash.TLS != nil {
		loggerTLSConfig, loggerLoader, err = loader.CreateClientLoader(conf.Log.Stash.TLS, nil)
		if err != nil {
			log.Fatalf("cannot create client loader: %v", err)
		}
		defer func(loggerLoader io.Closer) {
			err := loggerLoader.Close()
			if err != nil {
				log.Printf("error closing logger: %v", err)
			}
		}(loggerLoader)
	}

	_logger, _logstash, _logfile, err := ublogger.CreateUbMultiLoggerTLS(conf.Log.Level, conf.Log.File,
		ublogger.SetDataset(conf.Log.Stash.Dataset),
		ublogger.SetLogStash(conf.Log.Stash.LogstashHost, conf.Log.Stash.LogstashPort, conf.Log.Stash.Namespace, conf.Log.Stash.LogstashTraceLevel),
		ublogger.SetTLS(conf.Log.Stash.TLS != nil),
		ublogger.SetTLSConfig(loggerTLSConfig),
	)
	if err != nil {
		log.Fatalf("cannot create logger: %v", err)
	}
	if _logstash != nil {
		defer func(_logstash *ublogger.NewStash) {
			err := _logstash.Close()
			if err != nil {
				_logger.Error().Err(err).Msg("error closing logstash")
			}
		}(_logstash)
	}
	if _logfile != nil {
		defer func(_logfile *os.File) {
			err := _logfile.Close()
			if err != nil {
				_logger.Error().Err(err).Msg("error closing logfile")
			}
		}(_logfile)
	}

	l2 := _logger.With().Timestamp().Str("host", hostname).Str("addr", conf.LocalAddr).Logger()
	var logger zLogger.ZLogger = &l2

	var restTLSConfig *tls.Config
	if conf.RESTTLS.Type != "" {
		var restLoader io.Closer
		restTLSConfig, restLoader, err = loader.CreateServerLoader(false, &conf.RESTTLS, nil, logger)
		if err != nil {
			logger.Fatal().Err(err).Msg("cannot create server loader")
		}
		defer func(restLoader io.Closer) {
			err := restLoader.Close()
			if err != nil {
				logger.Error().Err(err).Msg("error closing rest loader")
			}
		}(restLoader)
		if restTLSConfig != nil {
			restTLSConfig.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}

	dashboardEnabled := *dashboard || conf.Dashboard.Enabled
	if dashboardEnabled {
		if conf.Dashboard.Username.String() == "" || conf.Dashboard.Password.String() == "" {
			if !conf.Dashboard.AllowBearer || (conf.RestBearer.String() == "" && conf.Bearer.String() == "") {
				logger.Warn().Msg("dashboard is enabled without authentication credentials configured (basic auth credentials or bearer fallback required for secure operation)")
			}
		}
	}

	ctrl, err := rest.NewController(conf.LocalAddr, conf.ExternalAddr, restTLSConfig, conf.RestBearer.String(), dashboardEnabled, logger)
	if err != nil {
		logger.Fatal().Msgf("cannot create controller: %v", err)
	}
	ctrl.SetRESTBearer(conf.RestBearer.String())
	ctrl.SetWSBearer(conf.WSBearer.String())
	ctrl.SetWSAuthType(conf.WSAuthType)
	ctrl.SetDashboardAuth(conf.Dashboard.Username.String(), conf.Dashboard.Password.String(), conf.Dashboard.AllowBearer)
	ctrl.SetNumWorkers(conf.NumWorkers)
	ctrl.SetReadLimit(conf.ReadLimit)
	ctrl.SetDebug(conf.Debug)
	if conf.NTPServer != "" {
		ctrl.SetNTPServer(conf.NTPServer, time.Duration(conf.NTPTimeout))
	}
	var wg = &sync.WaitGroup{}
	ctrl.Start(wg)

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	fmt.Println("press ctrl+c to stop server")
	s := <-done
	fmt.Println("got signal:", s)

	ctrl.GracefulStop()
	wg.Wait()
}
