// Package rest provides HTTP REST API endpoints, WebSocket connection handling,
// and the interactive HTML dashboard for WSBroker.
package rest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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

func (ctrl *controller) handleWSConnection(c *gin.Context, conn *websocket.Conn, name string, mode wsbrokerclient.ConnectionMode, initialChannels ...string) {
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
