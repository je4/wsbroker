// Package rest provides HTTP REST API endpoints, WebSocket connection handling,
// and the interactive HTML dashboard for WSBroker.
package rest

import (
	"bytes"
	_ "embed"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"html/template"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/je4/wsbroker/pkg/broker"
	"github.com/je4/wsbrokerclient/pkg/event"
)

//go:embed templates/dashboard.gohtml
var dashboardHTML string

//go:embed templates/stats.gohtml
var statsHTML string

//go:embed templates/connections.gohtml
var connectionsHTML string

//go:embed templates/channels.gohtml
var channelsHTML string

var dashboardFuncMap = template.FuncMap{
	"join": strings.Join,
}

var (
	dashboardTemplate          = template.Must(template.New("dashboard").Funcs(dashboardFuncMap).Parse(dashboardHTML))
	statsPartialTemplate       = template.Must(dashboardTemplate.New("statsPartial").Parse(statsHTML))
	connectionsPartialTemplate = template.Must(dashboardTemplate.New("connectionsPartial").Parse(connectionsHTML))
	channelsPartialTemplate    = template.Must(dashboardTemplate.New("channelsPartial").Parse(channelsHTML))
)

// DashboardStats holds summary metrics displayed in the dashboard header banner.
type DashboardStats struct {
	ConnectionCount int
	ChannelCount    int
	NumWorkers      int
}

// DashboardData contains aggregated data passed to render the complete dashboard page.
type DashboardData struct {
	Stats    DashboardStats
	Clients  []broker.ClientDetail
	Channels []ChannelInfo
}

// getDashboardData collects current statistics, client connections, and channel details.
func (ctrl *controller) getDashboardData() DashboardData {
	clients := ctrl.broker.GetClientDetails()
	channels := ctrl.broker.GetChannels()

	channelInfos := make([]ChannelInfo, 0, len(channels))
	for _, ch := range channels {
		members := ctrl.broker.GetChannelMembers(ch)
		channelInfos = append(channelInfos, ChannelInfo{
			Name:        ch,
			Subscribers: len(members),
			Clients:     members,
		})
	}

	return DashboardData{
		Stats: DashboardStats{
			ConnectionCount: len(clients),
			ChannelCount:    len(channels),
			NumWorkers:      ctrl.numWorkers,
		},
		Clients:  clients,
		Channels: channelInfos,
	}
}

// dashboardHandler renders the main dashboard HTML page.
func (ctrl *controller) dashboardHandler(c *gin.Context) {
	data := ctrl.getDashboardData()
	var buf bytes.Buffer
	if err := dashboardTemplate.Execute(&buf, data); err != nil {
		ctrl.logger.Error().Err(err).Msg("failed to render dashboard template")
		c.String(http.StatusInternalServerError, "Failed to render dashboard: %v", err)
		return
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", buf.Bytes())
}

// dashboardStatsHandler renders the stats partial HTML snippet for HTMX polling.
func (ctrl *controller) dashboardStatsHandler(c *gin.Context) {
	data := ctrl.getDashboardData()
	var buf bytes.Buffer
	if err := statsPartialTemplate.Execute(&buf, data.Stats); err != nil {
		c.String(http.StatusInternalServerError, "Error: %v", err)
		return
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", buf.Bytes())
}

// dashboardConnectionsHandler renders the active connections partial HTML snippet for HTMX polling.
func (ctrl *controller) dashboardConnectionsHandler(c *gin.Context) {
	data := ctrl.getDashboardData()
	var buf bytes.Buffer
	if err := connectionsPartialTemplate.Execute(&buf, data.Clients); err != nil {
		c.String(http.StatusInternalServerError, "Error: %v", err)
		return
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", buf.Bytes())
}

// dashboardChannelsHandler renders the active channels partial HTML snippet for HTMX polling.
func (ctrl *controller) dashboardChannelsHandler(c *gin.Context) {
	data := ctrl.getDashboardData()
	var buf bytes.Buffer
	if err := channelsPartialTemplate.Execute(&buf, data.Channels); err != nil {
		c.String(http.StatusInternalServerError, "Error: %v", err)
		return
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", buf.Bytes())
}

// dashboardDisconnectHandler terminates a client connection and returns updated connections HTML.
func (ctrl *controller) dashboardDisconnectHandler(c *gin.Context) {
	name := c.Param("name")
	if name != "" {
		ctrl.broker.DisconnectClient(name)
	}

	// Trigger header for HTMX to also refresh channels and stats
	c.Header("HX-Trigger", "refresh-channels, refresh-stats")

	data := ctrl.getDashboardData()
	var buf bytes.Buffer
	if err := connectionsPartialTemplate.Execute(&buf, data.Clients); err != nil {
		c.String(http.StatusInternalServerError, "Error: %v", err)
		return
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", buf.Bytes())
}

// dashboardPublishHandler handles message publishing from the dashboard form.
func (ctrl *controller) dashboardPublishHandler(c *gin.Context) {
	target := strings.TrimSpace(c.PostForm("target"))
	msgType := strings.TrimSpace(c.PostForm("type"))
	source := strings.TrimSpace(c.PostForm("source"))
	message := strings.TrimSpace(c.PostForm("message"))

	if target == "" {
		c.Data(http.StatusBadRequest, "text/html; charset=utf-8", []byte(`
			<div class="p-3 bg-rose-500/10 border border-rose-500/30 text-rose-300 rounded-lg text-xs">
				Error: Target (channel or client) must not be empty.
			</div>
		`))
		return
	}

	if msgType == "" {
		msgType = string(event.TypeMessage)
	}
	if source == "" {
		source = "dashboard"
	}

	var data []byte
	if strings.HasPrefix(message, "{") || strings.HasPrefix(message, "[") {
		if jsontext.Value(message).IsValid() {
			data = []byte(message)
		}
	}
	if len(data) == 0 {
		b, err := json.Marshal(message)
		if err != nil {
			c.Data(http.StatusBadRequest, "text/html; charset=utf-8", []byte(fmt.Sprintf(`
				<div class="p-3 bg-rose-500/10 border border-rose-500/30 text-rose-300 rounded-lg text-xs">
					Error formatting message: %v
				</div>
			`, err)))
			return
		}
		data = b
	}

	evt := &event.Event{
		Type:   event.EventType(msgType),
		Source: source,
		Target: target,
		Data:   data,
	}

	if err := ctrl.broker.Send(evt); err != nil {
		c.Data(http.StatusInternalServerError, "text/html; charset=utf-8", []byte(fmt.Sprintf(`
			<div class="p-3 bg-rose-500/10 border border-rose-500/30 text-rose-300 rounded-lg text-xs">
				Error sending message: %v
			</div>
		`, err)))
		return
	}

	// Trigger refresh for other components if needed
	c.Header("HX-Trigger", "refresh-channels, refresh-stats")

	members := ctrl.broker.GetChannelMembers(target)
	subscribersText := ""
	if len(members) > 0 {
		subscribersText = fmt.Sprintf(" (%d subscribers in channel)", len(members))
	} else if _, ok := ctrl.broker.GetWSConn(target); ok {
		subscribersText = " (Direct to client)"
	}

	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(fmt.Sprintf(`
		<div class="p-3 bg-emerald-500/10 border border-emerald-500/30 text-emerald-300 rounded-lg text-xs space-y-1">
			<div class="font-medium flex items-center gap-1.5">
				<svg class="w-4 h-4 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
					<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
				</svg>
				Message sent successfully!
			</div>
			<div class="text-[11px] text-emerald-400/80 font-mono">
				Target: <strong>%s</strong>%s
			</div>
		</div>
	`, template.HTMLEscapeString(target), subscribersText)))
}
