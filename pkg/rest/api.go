// Package rest provides HTTP REST API endpoints, WebSocket connection handling,
// and the interactive HTML dashboard for WSBroker.
package rest

import (
	"context"
	"crypto/tls"
	"encoding/json/v2"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/je4/wsbroker/pkg/broker"
	"github.com/je4/wsbroker/pkg/rest/docs"
	"go.ub.unibas.ch/cloud/swaggerui"
)

// BASEPATH defines the base URL prefix for version 1 of the WSBroker REST API.
const BASEPATH = "/api/v1"

// AuthType represents the authentication mode for WebSocket connections.
type AuthType string

const (
	AuthTypeBearer AuthType = "bearer"
	AuthTypeMTLS   AuthType = "mtls"
	AuthTypeBoth   AuthType = "both"
)

//	@title			WSBroker API
//	@version		1.0
//	@description	WSBroker API
//	@termsOfService	http://swagger.io/terms/

//	@servers.url			/api/v1
//	@servers.description	WSBroker API base path

//	@contact.name	Jürgen Enge
//	@contact.url	https://ub.unibas.ch
//	@contact.email	juergen.enge@unibas.ch

//	@license.name	Apache 2.0
//	@license.url	http://www.apache.org/licenses/LICENSE-2.0.html

//	@securityDefinitions.apikey	BearerAuth
//	@in							header
//	@name						Authorization

// NewController creates and configures a new REST and WebSocket controller instance.
func NewController(addr, extAddr string, tlsConfig *tls.Config, bearer string, dashboard bool, logger zLogger.ZLogger) (*controller, error) {
	u, err := url.Parse(extAddr)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid external address '%s'", extAddr)
	}
	subpath := "/" + strings.Trim(u.Path, "/")

	// programmatically set swagger info
	docs.SwaggerInfoWSBroker.Host = strings.TrimRight(fmt.Sprintf("%s:%s", u.Hostname(), u.Port()), " :")
	docs.SwaggerInfoWSBroker.BasePath = "/" + strings.Trim(subpath+BASEPATH, "/")
	if u.Scheme != "" {
		docs.SwaggerInfoWSBroker.Schemes = []string{u.Scheme}
	} else {
		docs.SwaggerInfoWSBroker.Schemes = []string{"http", "https"}
	}

	router := gin.Default()

	c := &controller{
		addr:       addr,
		router:     router,
		subpath:    subpath,
		logger:     logger,
		bearer:     bearer,
		restBearer: bearer,
		wsBearer:   bearer,
		authType:   AuthTypeBearer,
		wsAuthType: AuthTypeBearer,
		broker:     broker.NewConnectionManager(false, logger),
		numWorkers: runtime.NumCPU(),
		readLimit:  16 * 1024 * 1024,
		debug:      false,
	}
	if err := c.Init(tlsConfig, dashboard); err != nil {
		return nil, errors.Wrap(err, "cannot initialize rest controller")
	}
	return c, nil
}

// controller manages the HTTP router, server lifecycle, and broker integration.
type controller struct {
	server               http.Server
	router               *gin.Engine
	addr                 string
	subpath              string
	logger               zLogger.ZLogger
	restBearer           string
	wsBearer             string
	bearer               string
	authType             AuthType
	wsAuthType           AuthType
	dashboardUser        string
	dashboardPass        string
	allowDashboardBearer bool
	broker               *broker.ConnectionManager
	numWorkers           int
	readLimit            int64
	debug                bool
	ntpServer            string
	ntpTimeout           time.Duration
	ntpFunc              NTPForwarder
}

// SetNTPServer configures the remote NTP server address and timeout for raw NTP packet forwarding.
func (ctrl *controller) SetNTPServer(server string, timeout time.Duration) {
	ctrl.ntpServer = server
	ctrl.ntpTimeout = timeout
	if server != "" {
		ctrl.ntpFunc = NewNTPConnection(server, "udp", "123", "", 0, timeout)
	} else {
		ctrl.ntpFunc = nil
	}
}

// SetNTPForwarder configures a custom NTP forwarding function (useful for tests or custom forwarders).
func (ctrl *controller) SetNTPForwarder(forwarder NTPForwarder) {
	ctrl.ntpFunc = forwarder
}

// SetRESTBearer configures the Bearer token for protected REST API endpoints.
func (ctrl *controller) SetRESTBearer(bearer string) {
	ctrl.restBearer = bearer
	if ctrl.bearer == "" {
		ctrl.bearer = bearer
	}
}

// GetRESTBearer returns the configured REST Bearer token.
func (ctrl *controller) GetRESTBearer() string {
	if ctrl.restBearer != "" {
		return ctrl.restBearer
	}
	return ctrl.bearer
}

// SetWSBearer configures the Bearer token for WebSocket connections.
func (ctrl *controller) SetWSBearer(bearer string) {
	ctrl.wsBearer = bearer
	if ctrl.bearer == "" {
		ctrl.bearer = bearer
	}
}

// GetWSBearer returns the configured WebSocket Bearer token.
func (ctrl *controller) GetWSBearer() string {
	if ctrl.wsBearer != "" {
		return ctrl.wsBearer
	}
	return ctrl.bearer
}

// SetWSAuthType sets and normalizes the WebSocket authentication type ("bearer", "mtls", "both").
func (ctrl *controller) SetWSAuthType(authType string) {
	norm := strings.ToLower(strings.TrimSpace(authType))
	switch AuthType(norm) {
	case AuthTypeBearer, AuthTypeMTLS, AuthTypeBoth:
		ctrl.wsAuthType = AuthType(norm)
	default:
		ctrl.wsAuthType = AuthTypeBearer
	}
	ctrl.authType = ctrl.wsAuthType
}

// GetWSAuthType returns the configured WebSocket AuthType.
func (ctrl *controller) GetWSAuthType() AuthType {
	if ctrl.wsAuthType != "" {
		return ctrl.wsAuthType
	}
	if ctrl.authType != "" {
		return ctrl.authType
	}
	return AuthTypeBearer
}

// SetAuthType sets and normalizes the authentication type ("bearer", "mtls", "both").
func (ctrl *controller) SetAuthType(authType string) {
	ctrl.SetWSAuthType(authType)
}

// GetAuthType returns the configured AuthType.
func (ctrl *controller) GetAuthType() AuthType {
	return ctrl.GetWSAuthType()
}

// SetDashboardAuth configures the HTTP Basic Auth credentials and optional bearer token fallback for dashboard access.
func (ctrl *controller) SetDashboardAuth(username, password string, allowBearer bool) {
	ctrl.dashboardUser = username
	ctrl.dashboardPass = password
	ctrl.allowDashboardBearer = allowBearer
}

// SetNumWorkers configures the number of background worker goroutines for event dispatching.
func (ctrl *controller) SetNumWorkers(workers int) {
	if workers > 0 {
		ctrl.numWorkers = workers
	}
}

// SetReadLimit configures the maximum message read limit in bytes for incoming WebSocket connections.
func (ctrl *controller) SetReadLimit(limit int64) {
	if limit > 0 {
		ctrl.readLimit = limit
	}
}

// GetReadLimit returns the configured WebSocket read limit in bytes, defaulting to 16 MiB.
func (ctrl *controller) GetReadLimit() int64 {
	if ctrl.readLimit <= 0 {
		return 16 * 1024 * 1024
	}
	return ctrl.readLimit
}

// SetDebug toggles debug logging mode on the controller.
func (ctrl *controller) SetDebug(debug bool) {
	ctrl.debug = debug
}

// GetBroker returns the underlying broker ConnectionManager instance.
func (ctrl *controller) GetBroker() *broker.ConnectionManager {
	return ctrl.broker
}

// GetRouter returns the underlying Gin Engine instance.
func (ctrl *controller) GetRouter() *gin.Engine {
	return ctrl.router
}

// Init configures middleware, REST endpoints, WebSocket routes, Swagger UI, and the HTTP server.
func (ctrl *controller) Init(tlsConfig *tls.Config, dashboard bool) error {
	ctrl.router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"*"},
		AllowHeaders:     []string{"*"},
		AllowCredentials: true,
		AllowWebSockets:  true,
	}))

	ctrl.router.Use(func(c *gin.Context) {
		if c.Request.TLS == nil {
			c.Next()
			return
		}
		peerCertificates := c.Request.TLS.PeerCertificates
		if len(peerCertificates) == 0 {
			c.Next()
			return
		}
		var dnsNames = []string{}
		var ips = []net.IP{}
		var emails = []string{}
		var uris = []*url.URL{}
		cert := peerCertificates[0]
		dnsNames = append(dnsNames, cert.DNSNames...)
		ips = append(ips, cert.IPAddresses...)
		emails = append(emails, cert.EmailAddresses...)
		uris = append(uris, cert.URIs...)

		c.Set("names", dnsNames)
		c.Set("ips", ips)
		c.Set("emails", emails)
		c.Set("uris", uris)
		c.Next()
	})

	v1 := ctrl.router.Group(BASEPATH)

	v1.GET("/ping", ctrl.ping)

	v1.Use(ctrl.restAuthMiddleware())

	// REST Broker Endpoints
	v1.POST("/publish", ctrl.publish)
	v1.POST("/channel/:channel/publish", ctrl.publishChannel)

	if dashboard {
		v1.GET("/channels", ctrl.getChannels)
		v1.GET("/channel/:channel/clients", ctrl.getChannelClients)
		v1.GET("/clients", ctrl.getClients)
		v1.DELETE("/client/:name", ctrl.disconnectClient)
		v1.DELETE("/clients/:name", ctrl.disconnectClient)

		// Dashboard Endpoints
		dashGroup := ctrl.router.Group("/dashboard", ctrl.dashboardSecurityMiddleware(), ctrl.dashboardAuthMiddleware())
		dashGroup.GET("", ctrl.dashboardHandler)
		dashGroup.GET("/", ctrl.dashboardHandler)
		dashGroup.GET("/stats", ctrl.dashboardStatsHandler)
		dashGroup.GET("/connections", ctrl.dashboardConnectionsHandler)
		dashGroup.DELETE("/connections/:name", ctrl.dashboardDisconnectHandler)
		dashGroup.POST("/connections/:name/disconnect", ctrl.dashboardDisconnectHandler)
		dashGroup.GET("/channels", ctrl.dashboardChannelsHandler)
		dashGroup.POST("/publish", ctrl.dashboardPublishHandler)
	}

	// WebSocket Endpoints under root and /api/v1
	ctrl.router.GET("/ws/:name/:mode", ctrl.wsHandler)
	ctrl.router.GET("/ws/channel/:channel/:mode", ctrl.wsChannelHandler)
	//v1.GET("/ws/:name", ctrl.wsHandler)
	//v1.GET("/ws/channel/:channel", ctrl.wsChannelHandler)

	ctrl.router.GET("/swagger/*any", swaggerui.CustomHandler(
		swaggerui.WithTitle("WSBroker API"),
		swaggerui.WithDocFunc(func() string {
			doc := docs.SwaggerInfoWSBroker.ReadDoc()
			if docs.SwaggerInfoWSBroker.BasePath != "" {
				var spec map[string]any
				if err := json.Unmarshal([]byte(doc), &spec); err == nil {
					spec["servers"] = []map[string]any{
						{
							"url":         docs.SwaggerInfoWSBroker.BasePath,
							"description": "WSBroker API base path",
						},
					}
					if b, err := json.Marshal(spec); err == nil {
						return string(b)
					}
				}
			}
			return doc
		}),
	))

	ctrl.server = http.Server{
		Addr:      ctrl.addr,
		Handler:   ctrl.router,
		TLSConfig: tlsConfig,
	}

	return nil
}

// Start starts the worker pool and begins serving HTTP/HTTPS requests in a background goroutine.
func (ctrl *controller) Start(wg *sync.WaitGroup) {
	ctrl.broker.Start(ctrl.numWorkers)
	wg.Add(1)
	go func() {
		defer wg.Done() // let main know we are done cleaning up

		if ctrl.server.TLSConfig == nil {
			fmt.Printf("starting server at http://%s\n", ctrl.addr)
			if err := ctrl.server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				fmt.Printf("server on '%s' ended: %v\n", ctrl.addr, err)
			}
		} else {
			fmt.Printf("starting server at https://%s\n", ctrl.addr)
			if err := ctrl.server.ListenAndServeTLS("", ""); !errors.Is(err, http.ErrServerClosed) {
				fmt.Printf("server on '%s' ended: %v\n", ctrl.addr, err)
			}
		}
	}()
}

// GracefulStop shuts down the broker and HTTP server gracefully.
func (ctrl *controller) GracefulStop() {
	if ctrl.broker != nil {
		ctrl.broker.Close()
	}
	if err := ctrl.server.Shutdown(context.Background()); err != nil {
		ctrl.logger.Error().Err(err).Msg("error shutting down server")
		return
	}
}
