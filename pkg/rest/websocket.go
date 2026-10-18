// Package rest provides HTTP REST API endpoints, WebSocket connection handling,
// and the interactive HTML dashboard for WSBroker.
package rest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/gin-gonic/gin"
	"github.com/je4/wsbroker/pkg/broker"
	"github.com/je4/wsbrokerclient"
	"github.com/je4/wsbrokerclient/pkg/event"
)

func getCertPermissions(c *gin.Context) *broker.CertPermissions {
	if c.Request.TLS != nil && len(c.Request.TLS.PeerCertificates) > 0 {
		return broker.ParseCertPermissions(c.Request.TLS.PeerCertificates[0])
	}
	return nil
}

func (ctrl *controller) authenticateWS(c *gin.Context) (*broker.CertPermissions, bool) {
	switch ctrl.GetAuthType() {
	case AuthTypeMTLS:
		if c.Request.TLS == nil || len(c.Request.TLS.PeerCertificates) == 0 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, HTTPResultMessage{
				Code:    http.StatusUnauthorized,
				Message: "client certificate required",
			})
			return nil, false
		}
		return getCertPermissions(c), true

	case AuthTypeBoth:
		token, hasToken := ctrl.extractBearerToken(c)
		if hasToken {
			if !ctrl.validateBearerToken(token) {
				c.AbortWithStatusJSON(http.StatusUnauthorized, HTTPResultMessage{
					Code:    http.StatusUnauthorized,
					Message: "invalid bearer token",
				})
				return nil, false
			}
			return getCertPermissions(c), true
		}
		if c.Request.TLS == nil || len(c.Request.TLS.PeerCertificates) == 0 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, HTTPResultMessage{
				Code:    http.StatusUnauthorized,
				Message: "client certificate or bearer token required",
			})
			return nil, false
		}
		return getCertPermissions(c), true

	case AuthTypeBearer:
		fallthrough
	default:
		if len(ctrl.bearer) > 0 {
			token, hasToken := ctrl.extractBearerToken(c)
			if !hasToken || !ctrl.validateBearerToken(token) {
				c.AbortWithStatusJSON(http.StatusUnauthorized, HTTPResultMessage{
					Code:    http.StatusUnauthorized,
					Message: "invalid bearer token",
				})
				return nil, false
			}
		}
		return getCertPermissions(c), true
	}
}

func getRequestedChannels(c *gin.Context, initialChannels ...string) []string {
	var channels []string
	for _, ch := range initialChannels {
		if ch != "" {
			channels = append(channels, ch)
		}
	}
	for _, ch := range c.QueryArray("channel") {
		if ch != "" {
			channels = append(channels, ch)
		}
	}
	if qChannels := c.Query("channels"); qChannels != "" {
		for _, ch := range strings.Split(qChannels, ",") {
			ch = strings.TrimSpace(ch)
			if ch != "" {
				channels = append(channels, ch)
			}
		}
	}
	return channels
}

func (ctrl *controller) wsHandler(c *gin.Context) {
	certPerms, ok := ctrl.authenticateWS(c)
	if !ok {
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

	if certPerms != nil && certPerms.HasRestrictions() {
		if !certPerms.CanAccessClient(name) {
			c.AbortWithStatusJSON(http.StatusForbidden, HTTPResultMessage{
				Code:    http.StatusForbidden,
				Message: fmt.Sprintf("certificate does not authorize client name '%s'", name),
			})
			return
		}
		requestedChannels := getRequestedChannels(c)
		for _, ch := range requestedChannels {
			if !certPerms.CanAccessChannel(ch, mode) {
				c.AbortWithStatusJSON(http.StatusForbidden, HTTPResultMessage{
					Code:    http.StatusForbidden,
					Message: fmt.Sprintf("certificate does not authorize access to channel '%s' with mode '%s'", ch, mode),
				})
				return
			}
		}
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

	ctrl.handleWSConnection(c, conn, name, mode, certPerms)
}

func (ctrl *controller) wsChannelHandler(c *gin.Context) {
	certPerms, ok := ctrl.authenticateWS(c)
	if !ok {
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

	if certPerms != nil && certPerms.HasRestrictions() {
		if !certPerms.CanAccessChannel(channel, mode) {
			c.AbortWithStatusJSON(http.StatusForbidden, HTTPResultMessage{
				Code:    http.StatusForbidden,
				Message: fmt.Sprintf("certificate does not authorize access to channel '%s' with mode '%s'", channel, mode),
			})
			return
		}
		if !certPerms.CanAccessClient(clientName) {
			c.AbortWithStatusJSON(http.StatusForbidden, HTTPResultMessage{
				Code:    http.StatusForbidden,
				Message: fmt.Sprintf("certificate does not authorize client name '%s'", clientName),
			})
			return
		}
		requestedChannels := getRequestedChannels(c)
		for _, ch := range requestedChannels {
			if !certPerms.CanAccessChannel(ch, mode) {
				c.AbortWithStatusJSON(http.StatusForbidden, HTTPResultMessage{
					Code:    http.StatusForbidden,
					Message: fmt.Sprintf("certificate does not authorize access to channel '%s' with mode '%s'", ch, mode),
				})
				return
			}
		}
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

	ctrl.handleWSConnection(c, conn, clientName, mode, certPerms, channel)
}

func (ctrl *controller) handleWSConnection(c *gin.Context, conn *websocket.Conn, name string, mode wsbrokerclient.ConnectionMode, certPerms *broker.CertPermissions, initialChannels ...string) {
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
		} else if evt.Source != name && name != "dashboard" {
			if evt.Type == event.TypeNTPQuery {
				ctrl.logger.Error().Msgf("ntp event for %s on %s not allowed", evt.GetSource(), name)
				jsonBytes, _ := json.Marshal(fmt.Sprintf("ntp event for %s on %s not allowed", evt.GetSource(), name))
				_ = ctrl.broker.Send(&event.Event{
					Type:   event.TypeNTPError,
					Source: "wsbroker",
					Target: name,
					Data:   jsonBytes,
				})
			} else {
				ctrl.logger.Warn().Msgf("client %s attempted to send event with invalid source %s, dropping", name, evt.Source)
			}
			continue
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
				if certPerms != nil && certPerms.HasRestrictions() && !certPerms.CanAccessChannel(ch, mode) {
					ctrl.logger.Warn().Msgf("client %s certificate denied attach to channel %s with mode %s", name, ch, mode)
					continue
				}
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
		case event.TypeNTPQuery:
			ctrl.logger.Debug().Msgf("Received NTP query from %s: %s", evt.GetSource(), evt.Data)
			if name != evt.GetSource() && name != "dashboard" {
				ctrl.logger.Error().Msgf("ntp event for %s on %s not allowed", evt.GetSource(), name)
				jsonBytes, _ := json.Marshal(fmt.Sprintf("ntp event for %s on %s not allowed", evt.GetSource(), name))
				_ = ctrl.broker.Send(&event.Event{
					Type:   event.TypeNTPError,
					Source: "wsbroker",
					Target: name,
					Data:   jsonBytes,
				})
				continue
			}
			if ctrl.ntpFunc == nil {
				ctrl.logger.Error().Msg("NTP server not configured")
				jsonBytes, _ := json.Marshal("ntp service not configured")
				_ = ctrl.broker.Send(&event.Event{
					Type:   event.TypeNTPError,
					Source: "wsbroker",
					Target: name,
					Data:   jsonBytes,
				})
				continue
			}
			data, err := evt.GetData()
			if err != nil {
				ctrl.logger.Error().Err(err).Msg("Failed to get raw ntp data")
				jsonBytes, _ := json.Marshal(err.Error())
				_ = ctrl.broker.Send(&event.Event{
					Type:   event.TypeNTPError,
					Source: "wsbroker",
					Target: name,
					Data:   jsonBytes,
				})
				continue
			}
			raw, ok := data.([]byte)
			if !ok {
				ctrl.logger.Error().Msg("NTP query data is not raw bytes")
				jsonBytes, _ := json.Marshal("ntp query data must be raw bytes")
				_ = ctrl.broker.Send(&event.Event{
					Type:   event.TypeNTPError,
					Source: "wsbroker",
					Target: name,
					Data:   jsonBytes,
				})
				continue
			}
			result, err := ctrl.ntpFunc(raw)
			if err != nil {
				ctrl.logger.Error().Err(err).Msg("Failed to query ntp server")
				jsonBytes, _ := json.Marshal(err.Error())
				_ = ctrl.broker.Send(&event.Event{
					Type:   event.TypeNTPError,
					Source: "wsbroker",
					Target: name,
					Data:   jsonBytes,
				})
				continue
			}
			jsonBytes, _ := json.Marshal(result)
			ctrl.logger.Debug().Msgf("Sending ntp response to %s: %d bytes", evt.GetSource(), len(result))
			if err := ctrl.broker.Send(&event.Event{
				Type:   event.TypeNTPResponse,
				Source: "wsbroker",
				Target: name,
				Data:   jsonBytes,
			}); err != nil {
				ctrl.logger.Error().Err(err).Msg("Failed to send NTP response")
			}
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
