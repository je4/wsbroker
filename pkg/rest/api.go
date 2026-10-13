// Package rest provides HTTP REST API endpoints, WebSocket connection handling,
// and the interactive HTML dashboard for WSBroker.
package rest

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/je4/wsbroker/pkg/broker"
	"github.com/je4/wsbroker/pkg/rest/docs"
	"github.com/je4/wsbrokerclient/pkg/event"
	"go.ub.unibas.ch/cloud/swaggerui"
)

// BASEPATH defines the base URL prefix for version 1 of the WSBroker REST API.
const BASEPATH = "/api/v1"

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
		broker:     broker.NewConnectionManager(false, logger),
		numWorkers: runtime.NumCPU(),
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
	bearer               string
	dashboardUser        string
	dashboardPass        string
	allowDashboardBearer bool
	broker               *broker.ConnectionManager
	numWorkers           int
	debug                bool
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

// SetDebug toggles debug logging mode on the controller.
func (ctrl *controller) SetDebug(debug bool) {
	ctrl.debug = debug
}

// GetBroker returns the underlying broker ConnectionManager instance.
func (ctrl *controller) GetBroker() *broker.ConnectionManager {
	return ctrl.broker
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

	if len(ctrl.bearer) > 0 {
		v1.Use(func(c *gin.Context) {
			if strings.HasSuffix(c.Request.URL.Path, "/ping") {
				c.Next()
				return
			}
			authHeader := c.Request.Header.Get("Authorization")
			token := ""
			if after, ok := strings.CutPrefix(authHeader, "Bearer "); ok {
				token = after
			} else if qToken := c.Query("token"); qToken != "" {
				token = qToken
			} else if qBearer := c.Query("bearer"); qBearer != "" {
				token = qBearer
			}
			if subtle.ConstantTimeCompare([]byte(token), []byte(ctrl.bearer)) != 1 {
				c.AbortWithStatusJSON(http.StatusUnauthorized, HTTPResultMessage{
					Code:    http.StatusUnauthorized,
					Message: "invalid bearer token",
				})
				return
			}
			c.Next()
		})
	}

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
	go func() {
		wg.Add(1)
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

func (ctrl *controller) checkBearer(c *gin.Context) bool {
	if len(ctrl.bearer) == 0 {
		return true
	}
	authHeader := c.Request.Header.Get("Authorization")
	token := ""
	if after, ok := strings.CutPrefix(authHeader, "Bearer "); ok {
		token = after
	} else if qToken := c.Query("token"); qToken != "" {
		token = qToken
	} else if qBearer := c.Query("bearer"); qBearer != "" {
		token = qBearer
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(ctrl.bearer)) != 1 {
		c.AbortWithStatusJSON(http.StatusUnauthorized, HTTPResultMessage{
			Code:    http.StatusUnauthorized,
			Message: "invalid bearer token",
		})
		return false
	}
	return true
}

func (ctrl *controller) checkBearerSilent(c *gin.Context) bool {
	if len(ctrl.bearer) == 0 {
		return false
	}
	authHeader := c.Request.Header.Get("Authorization")
	token := ""
	if after, ok := strings.CutPrefix(authHeader, "Bearer "); ok {
		token = after
	} else if qToken := c.Query("token"); qToken != "" {
		token = qToken
	} else if qBearer := c.Query("bearer"); qBearer != "" {
		token = qBearer
	}
	if token == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(ctrl.bearer)) == 1
}

func (ctrl *controller) dashboardSecurityMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Set security headers
		c.Header("X-Frame-Options", "DENY")
		c.Header("X-Content-Type-Options", "nosniff")
		c.Header("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline' https://cdn.tailwindcss.com https://unpkg.com; style-src 'self' 'unsafe-inline' https://cdn.tailwindcss.com; img-src 'self' data:; connect-src 'self'")
		c.Header("Referrer-Policy", "same-origin")

		// Validate Origin / Referer for state-modifying HTTP methods (CSRF protection)
		if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead && c.Request.Method != http.MethodOptions {
			origin := c.Request.Header.Get("Origin")
			referer := c.Request.Header.Get("Referer")

			if origin != "" {
				u, err := url.Parse(origin)
				if err != nil || (u.Host != c.Request.Host && !strings.EqualFold(u.Host, c.Request.Host)) {
					c.AbortWithStatusJSON(http.StatusForbidden, HTTPResultMessage{
						Code:    http.StatusForbidden,
						Message: "forbidden: invalid origin header",
					})
					return
				}
			} else if referer != "" {
				u, err := url.Parse(referer)
				if err != nil || (u.Host != c.Request.Host && !strings.EqualFold(u.Host, c.Request.Host)) {
					c.AbortWithStatusJSON(http.StatusForbidden, HTTPResultMessage{
						Code:    http.StatusForbidden,
						Message: "forbidden: invalid referer header",
					})
					return
				}
			}
		}

		c.Next()
	}
}

func (ctrl *controller) dashboardAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// If neither basic auth credentials nor bearer auth are configured, permit access
		if ctrl.dashboardUser == "" && ctrl.dashboardPass == "" && (!ctrl.allowDashboardBearer || ctrl.bearer == "") {
			c.Next()
			return
		}

		// 1. Check HTTP Basic Auth if credentials are configured
		if ctrl.dashboardUser != "" || ctrl.dashboardPass != "" {
			user, pass, hasBasic := c.Request.BasicAuth()
			if hasBasic {
				userMatch := subtle.ConstantTimeCompare([]byte(user), []byte(ctrl.dashboardUser)) == 1
				passMatch := subtle.ConstantTimeCompare([]byte(pass), []byte(ctrl.dashboardPass)) == 1
				if userMatch && passMatch {
					c.Next()
					return
				}
			}
		}

		// 2. Check Bearer Auth Fallback if allowed
		if ctrl.allowDashboardBearer && ctrl.checkBearerSilent(c) {
			c.Next()
			return
		}

		// 3. Challenge when unauthenticated or invalid credentials
		c.Header("WWW-Authenticate", `Basic realm="WSBroker Dashboard"`)
		c.AbortWithStatusJSON(http.StatusUnauthorized, HTTPResultMessage{
			Code:    http.StatusUnauthorized,
			Message: "unauthorized dashboard access",
		})
	}
}

// ping godoc
//
//	@Summary		does pong
//	@ID				get-ping
//	@Description	for testing if server is running
//	@Tags			wsbroker
//	@Produce		plain
//	@Success		200		{string}	string
//	@Router			/ping [get]
func (ctrl *controller) ping(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}

// publish godoc
//
//	@Summary		publish message to target
//	@ID				post-publish
//	@Description	publishes a message or event to a channel or client target
//	@Tags			wsbroker
//	@Accept			json
//	@Produce		json
//	@Param			body	body		PublishRequest	true	"Publish payload"
//	@Success		200		{object}	PublishResponse
//	@Failure		400		{object}	HTTPResultMessage
//	@Failure		401		{object}	HTTPResultMessage
//	@Security		BearerAuth
//	@Router			/publish [post]
func (ctrl *controller) publish(c *gin.Context) {
	var req PublishRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		NewResultMessage(c, http.StatusBadRequest, err)
		return
	}
	if req.Target == "" {
		NewResultMessage(c, http.StatusBadRequest, errors.New("target is required"))
		return
	}
	if req.Type == "" {
		req.Type = string(event.TypeMessage)
	}

	var data json.RawMessage
	if len(req.Data) > 0 {
		data = req.Data
	} else if req.Message != "" {
		b, err := json.Marshal(req.Message)
		if err != nil {
			NewResultMessage(c, http.StatusBadRequest, errors.Wrap(err, "failed to marshal message"))
			return
		}
		data = b
	}

	evt := &event.Event{
		Type:   event.EventType(req.Type),
		Source: req.Source,
		Target: req.Target,
		Token:  req.Token,
		Data:   data,
	}

	if err := ctrl.broker.Send(evt); err != nil {
		NewResultMessage(c, http.StatusInternalServerError, errors.Wrap(err, "failed to send event"))
		return
	}

	subscribers := len(ctrl.broker.GetChannelMembers(req.Target))
	c.JSON(http.StatusOK, PublishResponse{
		Target:      req.Target,
		Type:        req.Type,
		Sent:        true,
		Subscribers: subscribers,
	})
}

// publishChannel godoc
//
//	@Summary		publish message to channel
//	@ID				post-channel-publish
//	@Description	publishes a message or event to all subscribers of a channel
//	@Tags			wsbroker
//	@Accept			json
//	@Produce		json
//	@Param			channel	path		string			true	"Channel name"
//	@Param			body	body		PublishRequest	true	"Publish payload"
//	@Success		200		{object}	PublishResponse
//	@Failure		400		{object}	HTTPResultMessage
//	@Failure		401		{object}	HTTPResultMessage
//	@Security		BearerAuth
//	@Router			/channel/{channel}/publish [post]
func (ctrl *controller) publishChannel(c *gin.Context) {
	channel := c.Param("channel")
	if channel == "" {
		NewResultMessage(c, http.StatusBadRequest, errors.New("channel name is required"))
		return
	}

	var req PublishRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		NewResultMessage(c, http.StatusBadRequest, err)
		return
	}
	req.Target = channel
	if req.Type == "" {
		req.Type = string(event.TypeMessage)
	}

	var data json.RawMessage
	if len(req.Data) > 0 {
		data = req.Data
	} else if req.Message != "" {
		b, err := json.Marshal(req.Message)
		if err != nil {
			NewResultMessage(c, http.StatusBadRequest, errors.Wrap(err, "failed to marshal message"))
			return
		}
		data = b
	}

	evt := &event.Event{
		Type:   event.EventType(req.Type),
		Source: req.Source,
		Target: req.Target,
		Token:  req.Token,
		Data:   data,
	}

	if err := ctrl.broker.Send(evt); err != nil {
		NewResultMessage(c, http.StatusInternalServerError, errors.Wrap(err, "failed to send event"))
		return
	}

	subscribers := len(ctrl.broker.GetChannelMembers(channel))
	c.JSON(http.StatusOK, PublishResponse{
		Target:      channel,
		Type:        req.Type,
		Sent:        true,
		Subscribers: subscribers,
	})
}

// getChannels godoc
//
//	@Summary		list active channels
//	@ID				get-channels
//	@Description	returns list of all active channels and their subscriber count
//	@Tags			wsbroker
//	@Produce		json
//	@Success		200		{object}	map[string]int
//	@Failure		401		{object}	HTTPResultMessage
//	@Security		BearerAuth
//	@Router			/channels [get]
func (ctrl *controller) getChannels(c *gin.Context) {
	counts := ctrl.broker.GetChannelCounts()
	c.JSON(http.StatusOK, counts)
}

// getChannelClients godoc
//
//	@Summary		list clients in channel
//	@ID				get-channel-clients
//	@Description	returns list of all clients subscribed to a specific channel
//	@Tags			wsbroker
//	@Produce		json
//	@Param			channel	path		string	true	"Channel name"
//	@Success		200		{array}		string
//	@Failure		401		{object}	HTTPResultMessage
//	@Security		BearerAuth
//	@Router			/channel/{channel}/clients [get]
func (ctrl *controller) getChannelClients(c *gin.Context) {
	channel := c.Param("channel")
	members := ctrl.broker.GetChannelMembers(channel)
	c.JSON(http.StatusOK, members)
}

// getClients godoc
//
//	@Summary		list connected clients
//	@ID				get-clients
//	@Description	returns list of all connected clients
//	@Tags			wsbroker
//	@Produce		json
//	@Success		200		{array}		string
//	@Failure		401		{object}	HTTPResultMessage
//	@Security		BearerAuth
//	@Router			/clients [get]
func (ctrl *controller) getClients(c *gin.Context) {
	clients := ctrl.broker.GetClients()
	c.JSON(http.StatusOK, clients)
}

// disconnectClient godoc
//
//	@Summary		disconnect client
//	@ID				delete-client
//	@Description	disconnects a connected WebSocket client by name
//	@Tags			wsbroker
//	@Produce		json
//	@Param			name	path		string	true	"Client name"
//	@Success		200		{object}	HTTPResultMessage
//	@Failure		404		{object}	HTTPResultMessage
//	@Failure		401		{object}	HTTPResultMessage
//	@Security		BearerAuth
//	@Router			/client/{name} [delete]
func (ctrl *controller) disconnectClient(c *gin.Context) {
	name := c.Param("name")
	if name == "" {
		NewResultMessage(c, http.StatusBadRequest, errors.New("client name is required"))
		return
	}
	if !ctrl.broker.DisconnectClient(name) {
		NewResultMessage(c, http.StatusNotFound, errors.Errorf("client %s not found", name))
		return
	}
	c.JSON(http.StatusOK, HTTPResultMessage{
		Code:    http.StatusOK,
		Message: fmt.Sprintf("client %s disconnected", name),
	})
}

func (ctrl *controller) wsHandler(c *gin.Context) {
	if !ctrl.checkBearer(c) {
		return
	}

	name := c.Param("name")
	if name == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, HTTPResultMessage{
			Code:    http.StatusBadRequest,
			Message: "client name is required",
		})
		return
	}

	modeParam := c.Param("mode")
	mode, err := broker.ParseConnectionMode(modeParam)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, HTTPResultMessage{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("invalid connection mode: %s", modeParam),
		})
		return
	}

	opts := &websocket.AcceptOptions{
		InsecureSkipVerify: true,
		OriginPatterns:     []string{"*"},
	}
	conn, err := websocket.Accept(c.Writer, c.Request, opts)
	if err != nil {
		ctrl.logger.Error().Err(err).Msgf("failed to accept websocket connection for client %s", name)
		return
	}

	ctrl.handleWSConnection(c, conn, name, mode)
}

func (ctrl *controller) wsChannelHandler(c *gin.Context) {
	if !ctrl.checkBearer(c) {
		return
	}

	channel := c.Param("channel")
	if channel == "" {
		c.AbortWithStatusJSON(http.StatusBadRequest, HTTPResultMessage{
			Code:    http.StatusBadRequest,
			Message: "channel name is required",
		})
		return
	}

	modeParam := c.Param("mode")
	mode, err := broker.ParseConnectionMode(modeParam)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, HTTPResultMessage{
			Code:    http.StatusBadRequest,
			Message: fmt.Sprintf("invalid connection mode: %s", modeParam),
		})
		return
	}

	clientName := c.Query("name")
	if clientName == "" {
		clientName = c.Query("client")
	}
	if clientName == "" {
		b := make([]byte, 8)
		_, _ = rand.Read(b)
		clientName = fmt.Sprintf("client-%s", hex.EncodeToString(b))
	}

	opts := &websocket.AcceptOptions{
		InsecureSkipVerify: true,
		OriginPatterns:     []string{"*"},
	}
	conn, err := websocket.Accept(c.Writer, c.Request, opts)
	if err != nil {
		ctrl.logger.Error().Err(err).Msgf("failed to accept websocket connection for channel %s, client %s", channel, clientName)
		return
	}

	ctrl.handleWSConnection(c, conn, clientName, mode, channel)
}

func (ctrl *controller) handleWSConnection(c *gin.Context, conn *websocket.Conn, name string, mode broker.ConnectionMode, initialChannels ...string) {
	wsConn := broker.NewConnectionWithMode(conn, name, c.Request.TLS != nil, mode)
	if err := ctrl.broker.AddWSConn(wsConn); err != nil {
		ctrl.logger.Error().Err(err).Msgf("failed to register connection %s", name)
		_ = conn.Close(websocket.StatusInternalError, "failed to register connection")
		return
	}
	defer ctrl.broker.CloseWSConn(wsConn)

	// Subscribe to initial channels
	for _, ch := range initialChannels {
		if ch != "" {
			ctrl.broker.AddToGroup(name, ch)
		}
	}
	for _, ch := range c.QueryArray("channel") {
		if ch != "" {
			ctrl.broker.AddToGroup(name, ch)
		}
	}
	if qChannels := c.Query("channels"); qChannels != "" {
		for _, ch := range strings.Split(qChannels, ",") {
			ch = strings.TrimSpace(ch)
			if ch != "" {
				ctrl.broker.AddToGroup(name, ch)
			}
		}
	}

	ctx := c.Request.Context()

	// Periodic ping
	pingCtx, pingCancel := context.WithCancel(ctx)
	defer pingCancel()
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				pCtx, pCancel := context.WithTimeout(pingCtx, 5*time.Second)
				err := conn.Ping(pCtx)
				pCancel()
				if err != nil {
					ctrl.logger.Debug().Err(err).Msgf("failed to send ping to %s", name)
					return
				}
			case <-pingCtx.Done():
				return
			}
		}
	}()

	for {
		var evt = &event.Event{}
		if err := wsjson.Read(ctx, conn, evt); err != nil {
			status := websocket.CloseStatus(err)
			if status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway || status == websocket.StatusNoStatusRcvd {
				ctrl.logger.Debug().Err(err).Msgf("websocket closed normally by client %s", name)
			} else {
				ctrl.logger.Debug().Err(err).Msgf("websocket read error for client %s: %v", name, err)
			}
			break
		}

		if evt.Source == "" {
			evt.Source = name
		}

		ctrl.logger.Debug().Msgf("client %s received event: %s", name, evt.String())

		switch evt.Type {
		case event.TypeAttach:
			ch := ""
			if d, err := evt.GetData(); err == nil && d != nil {
				if s, ok := d.(string); ok && s != "" {
					ch = s
				}
			}
			if ch == "" {
				ch = evt.GetTarget()
			}
			if ch != "" {
				ctrl.broker.AddToGroup(name, ch)
			}
		case event.TypeDetach:
			ch := ""
			if d, err := evt.GetData(); err == nil && d != nil {
				if s, ok := d.(string); ok && s != "" {
					ch = s
				}
			}
			if ch == "" {
				ch = evt.GetTarget()
			}
			if ch != "" {
				ctrl.broker.RemoveFromGroup(name, ch)
			}
		case event.TypePing:
			_ = ctrl.broker.Send(&event.Event{
				Type:   event.TypePong,
				Source: "wsbroker",
				Target: name,
				Data:   evt.Data,
			})
		default:
			if !mode.CanWrite() {
				ctrl.logger.Warn().Msgf("read-only client %s attempted to send event type %s, dropping", name, evt.Type)
				continue
			}
			if err := ctrl.broker.Send(evt); err != nil {
				ctrl.logger.Error().Err(err).Msgf("failed to forward event %s from client %s", evt.String(), name)
			}
		}
	}
}
