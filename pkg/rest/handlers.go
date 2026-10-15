// Package rest provides HTTP REST API endpoints, WebSocket connection handling,
// and the interactive HTML dashboard for WSBroker.
package rest

import (
	"encoding/json/v2"
	"fmt"
	"net/http"

	"emperror.dev/errors"
	"github.com/gin-gonic/gin"
	"github.com/je4/wsbrokerclient/pkg/event"
)

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

	var data []byte
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

	var data []byte
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
