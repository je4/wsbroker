// Package rest provides HTTP REST API endpoints, WebSocket connection handling,
// and the interactive HTML dashboard for WSBroker.
package rest

import (
	"encoding/json/jsontext"

	"github.com/je4/wsbrokerclient/pkg/event"
)

// PublishRequest defines the JSON payload for publishing messages via REST API endpoints.
type PublishRequest struct {
	ID      string         `json:"id,omitempty" example:"evt-a1b2c3d4"`
	RefID   string         `json:"ref_id,omitempty" example:"evt-e5f6a7b8"`
	Source  string         `json:"source" example:"publisher-1"`
	Target  string         `json:"target,omitempty" example:"alerts"`
	Type    string         `json:"type" example:"message"`
	Token   string         `json:"token,omitempty"`
	Data    jsontext.Value `json:"data,omitempty" swaggertype:"primitive,string" example:"{\"text\":\"hello world\"}"`
	Message string         `json:"message,omitempty" example:"hello world"`
}

// PublishResponse defines the JSON response returned after a message is dispatched.
type PublishResponse struct {
	ID          string `json:"id,omitempty" example:"evt-a1b2c3d4"`
	RefID       string `json:"ref_id,omitempty" example:"evt-e5f6a7b8"`
	Target      string `json:"target" example:"alerts"`
	Channel     string `json:"channel,omitempty" example:"alerts"`
	Type        string `json:"type" example:"message"`
	Sent        bool   `json:"sent" example:"true"`
	Subscribers int    `json:"subscribers" example:"3"`
}

// ChannelInfo describes a channel, its total subscriber count, and its connected client members.
type ChannelInfo = event.ChannelInfo
