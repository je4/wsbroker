// Package rest provides HTTP REST API endpoints, WebSocket connection handling,
// and the interactive HTML dashboard for WSBroker.
package rest

import (
	"crypto/subtle"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
)

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
