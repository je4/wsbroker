// Package rest provides HTTP REST API endpoints, WebSocket connection handling,
// and the interactive HTML dashboard for WSBroker.
package rest

import "github.com/gin-gonic/gin"

// NewResultMessage sends a JSON error response with the given status code and error message.
func NewResultMessage(ctx *gin.Context, status int, err error) {
	er := HTTPResultMessage{
		Code:    status,
		Message: err.Error(),
	}
	ctx.JSON(status, er)
}

// HTTPResultMessage represents a standardized error or status message envelope returned by the REST API.
type HTTPResultMessage struct {
	Code    int    `json:"code" example:"400"`
	Message string `json:"message" example:"status bad request"`
}
