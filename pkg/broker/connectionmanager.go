// Package broker implements WebSocket connection lifecycle management, channel-based
// broadcasting, and asynchronous event dispatching using worker pools.
package broker

import (
	"context"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/je4/wsbroker/pkg/event"
)

// NewConnectionManager initializes and returns a new ConnectionManager.
func NewConnectionManager(debug bool, logger zLogger.ZLogger) *ConnectionManager {
	cm := &ConnectionManager{
		debug:         debug,
		wsConns:       make(map[string]*Connection),
		groups:        make(map[string][]string),
		logger:        logger,
		senderChannel: make(chan *job, 1000),
	}
	return cm
}

// job represents an asynchronous delivery unit targeting a specific client connection.
type job struct {
	evt  *event.Event
	dest string
}

// ConnectionManager manages active WebSocket connections, channel subscriptions,
// and worker goroutines for concurrent message dispatching.
type ConnectionManager struct {
	wsConns       map[string]*Connection
	wsConnsMu     sync.RWMutex
	groups        map[string][]string
	groupsMu      sync.RWMutex
	debug         bool
	logger        zLogger.ZLogger
	senderChannel chan *job
	workerWG      sync.WaitGroup
	closed        bool
	closedMu      sync.Mutex
}

// Start launches the specified number of background worker goroutines to process outgoing events.
// If numWorkers is <= 0, runtime.NumCPU() workers are started.
func (manager *ConnectionManager) Start(numWorkers int) {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	for i := 0; i < numWorkers; i++ {
		manager.logger.Debug().Msgf("Starting broker worker #%d", i)
		go manager.worker(i, manager.senderChannel)
	}
}

// Close stops all workers and terminates all active WebSocket connections.
func (manager *ConnectionManager) Close() {
	manager.closedMu.Lock()
	if manager.closed {
		manager.closedMu.Unlock()
		return
	}
	manager.closed = true
	close(manager.senderChannel)
	manager.closedMu.Unlock()

	manager.workerWG.Wait()

	manager.wsConnsMu.Lock()
	for name, conn := range manager.wsConns {
		if err := conn.Close(); err != nil {
			manager.logger.Error().Err(err).Msgf("Failed to close connection %s", name)
		}
	}
	manager.wsConns = make(map[string]*Connection)
	manager.wsConnsMu.Unlock()

	manager.groupsMu.Lock()
	manager.groups = make(map[string][]string)
	manager.groupsMu.Unlock()
}

// worker continuously receives outbound delivery jobs from the senderChannel and dispatches them.
func (manager *ConnectionManager) worker(id int, jobs <-chan *job) {
	manager.workerWG.Add(1)
	defer manager.workerWG.Done()
	for j := range jobs {
		manager.logger.Debug().Msgf("worker #%d forwarding event %s %s -> %s to %s", id, j.evt.Type, j.evt.GetSource(), j.evt.GetTarget(), j.dest)
		if err := manager.sendWS(j.dest, j.evt); err != nil {
			manager.logger.Error().Err(err).Msgf("worker #%d failed to send event to %s", id, j.dest)
			continue
		}
		manager.logger.Debug().Msgf("worker #%d event %s %s -> %s to %s forwarded", id, j.evt.Type, j.evt.GetSource(), j.evt.GetTarget(), j.dest)
	}
}

// Send enqueues an event for dispatch. If the event target matches a known channel/group,
// the event is fanned out to all subscribers; otherwise, it is directed to the named client.
func (manager *ConnectionManager) Send(evt *event.Event) error {
	if evt == nil {
		return errors.New("event is nil")
	}

	manager.closedMu.Lock()
	if manager.closed {
		manager.closedMu.Unlock()
		return errors.New("connection manager is closed")
	}
	manager.closedMu.Unlock()

	target := evt.GetTarget()
	manager.groupsMu.RLock()
	dests, ok := manager.groups[target]
	if ok {
		// Target is a group/channel
		destsCopy := make([]string, len(dests))
		copy(destsCopy, dests)
		manager.groupsMu.RUnlock()

		for _, dest := range destsCopy {
			manager.senderChannel <- &job{evt: evt, dest: dest}
		}
		return nil
	}
	manager.groupsMu.RUnlock()

	// Target is a direct client
	manager.senderChannel <- &job{evt: evt, dest: target}
	return nil
}

// sendWS sends the event directly to the named WebSocket connection with a 5-second timeout.
func (manager *ConnectionManager) sendWS(dest string, evt *event.Event) error {
	conn, ok := manager.GetWSConn(dest)
	if !ok {
		return errors.Errorf("no connection for destination %s", dest)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.WriteJSON(ctx, evt); err != nil {
		return errors.Wrapf(err, "failed to send event %s to %s->%s", evt.GetType(), evt.GetSource(), evt.GetTarget())
	}
	return nil
}

// AddWSConn registers a new WebSocket connection. If an existing connection with the same name exists,
// it is safely replaced and closed.
func (manager *ConnectionManager) AddWSConn(c *Connection) error {
	name := c.Name
	if conn, ok := manager.GetWSConn(name); ok {
		if conn.Secure && !c.Secure {
			return errors.Errorf("cannot replace secure connection %s with an insecure connection", name)
		}
		manager.CloseWSConn(conn)
		manager.logger.Warn().Msgf("replacing connection %s", name)
	}
	manager.wsConnsMu.Lock()
	defer manager.wsConnsMu.Unlock()
	manager.logger.Debug().Msgf("Adding connection %s", name)
	manager.wsConns[name] = c
	return nil
}

// GetWSConn retrieves a Connection by client name.
func (manager *ConnectionManager) GetWSConn(name string) (*Connection, bool) {
	manager.wsConnsMu.RLock()
	defer manager.wsConnsMu.RUnlock()
	conn, ok := manager.wsConns[name]
	return conn, ok
}

// RemoveWSConn unregisters a connection from the connection map without closing the socket.
func (manager *ConnectionManager) RemoveWSConn(name string) {
	manager.wsConnsMu.Lock()
	defer manager.wsConnsMu.Unlock()
	manager.logger.Debug().Msgf("Removing connection %s", name)
	delete(manager.wsConns, name)
}

// DisconnectClient forcefully closes and removes the named client connection and its channel subscriptions.
// Returns true if the client was found and disconnected.
func (manager *ConnectionManager) DisconnectClient(name string) bool {
	manager.wsConnsMu.RLock()
	conn, ok := manager.wsConns[name]
	manager.wsConnsMu.RUnlock()
	if !ok {
		return false
	}
	manager.CloseWSConn(conn)
	return true
}

// CloseWSConn closes the underlying socket of the connection and cleans up all associated group memberships.
func (manager *ConnectionManager) CloseWSConn(wsConn *Connection) {
	if wsConn == nil {
		return
	}
	manager.wsConnsMu.Lock()
	if conn, ok := manager.wsConns[wsConn.Name]; ok {
		if conn.Conn != wsConn.Conn {
			manager.wsConnsMu.Unlock()
			manager.logger.Debug().Msgf("connection %s already replaced/closed", wsConn.Name)
			return
		}
		manager.logger.Debug().Msgf("Closing connection %s", wsConn.Name)
		if err := conn.Close(); err != nil {
			manager.logger.Error().Err(err).Msg("Failed to close connection")
		}
		delete(manager.wsConns, wsConn.Name)
	}
	manager.wsConnsMu.Unlock()

	manager.RemoveFromGroups(wsConn.Name)
}

// AddToGroup adds a client to a channel / broadcast group.
func (manager *ConnectionManager) AddToGroup(name string, group string) {
	if name == "" || group == "" {
		return
	}
	manager.groupsMu.Lock()
	defer manager.groupsMu.Unlock()
	if _, ok := manager.groups[group]; !ok {
		manager.groups[group] = []string{}
	}
	if !slices.Contains(manager.groups[group], name) {
		manager.groups[group] = append(manager.groups[group], name)
		manager.logger.Debug().Msgf("Added client %s to group/channel %s (total: %d)", name, group, len(manager.groups[group]))
	}
}

// RemoveFromGroup removes a client from a specific channel / broadcast group.
func (manager *ConnectionManager) RemoveFromGroup(name string, group string) {
	if name == "" || group == "" {
		return
	}
	manager.groupsMu.Lock()
	defer manager.groupsMu.Unlock()
	if _, ok := manager.groups[group]; !ok {
		return
	}
	manager.groups[group] = slices.DeleteFunc(manager.groups[group], func(s string) bool {
		return s == name
	})
	if len(manager.groups[group]) == 0 {
		delete(manager.groups, group)
	}
	manager.logger.Debug().Msgf("Removed client %s from group/channel %s", name, group)
}

// RemoveFromGroups removes a client from all channels / broadcast groups.
func (manager *ConnectionManager) RemoveFromGroups(name string) {
	if name == "" {
		return
	}
	manager.groupsMu.Lock()
	defer manager.groupsMu.Unlock()
	for group := range manager.groups {
		manager.groups[group] = slices.DeleteFunc(manager.groups[group], func(s string) bool {
			return s == name
		})
		if len(manager.groups[group]) == 0 {
			delete(manager.groups, group)
		}
	}
	manager.logger.Debug().Msgf("Removed client %s from all groups", name)
}

// GetChannels returns a sorted slice of all active channel names.
func (manager *ConnectionManager) GetChannels() []string {
	manager.groupsMu.RLock()
	defer manager.groupsMu.RUnlock()
	channels := make([]string, 0, len(manager.groups))
	for ch := range manager.groups {
		channels = append(channels, ch)
	}
	slices.Sort(channels)
	return channels
}

// GetChannelMembers returns a sorted list of subscriber client names for a given channel.
func (manager *ConnectionManager) GetChannelMembers(group string) []string {
	manager.groupsMu.RLock()
	defer manager.groupsMu.RUnlock()
	if members, ok := manager.groups[group]; ok {
		result := make([]string, len(members))
		copy(result, members)
		slices.Sort(result)
		return result
	}
	return []string{}
}

// GetClients returns a sorted list of all registered client names.
func (manager *ConnectionManager) GetClients() []string {
	manager.wsConnsMu.RLock()
	defer manager.wsConnsMu.RUnlock()
	clients := make([]string, 0, len(manager.wsConns))
	for client := range manager.wsConns {
		clients = append(clients, client)
	}
	slices.Sort(clients)
	return clients
}

// GetChannelCounts returns a map of channel names to subscriber counts.
func (manager *ConnectionManager) GetChannelCounts() map[string]int {
	manager.groupsMu.RLock()
	defer manager.groupsMu.RUnlock()
	counts := make(map[string]int, len(manager.groups))
	for group, members := range manager.groups {
		counts[group] = len(members)
	}
	return counts
}

// ClientDetail describes a connected client, its TLS security status, and its channel subscriptions.
type ClientDetail struct {
	Name     string   `json:"name"`
	Secure   bool     `json:"secure"`
	Channels []string `json:"channels"`
}

// GetClientChannels returns all channel names that the given client is subscribed to.
func (manager *ConnectionManager) GetClientChannels(name string) []string {
	manager.groupsMu.RLock()
	defer manager.groupsMu.RUnlock()
	var channels []string
	for group, members := range manager.groups {
		if slices.Contains(members, name) {
			channels = append(channels, group)
		}
	}
	slices.Sort(channels)
	return channels
}

// GetClientDetails returns detailed information for all currently connected clients.
func (manager *ConnectionManager) GetClientDetails() []ClientDetail {
	manager.wsConnsMu.RLock()
	defer manager.wsConnsMu.RUnlock()
	manager.groupsMu.RLock()
	defer manager.groupsMu.RUnlock()

	details := make([]ClientDetail, 0, len(manager.wsConns))
	for name, conn := range manager.wsConns {
		var channels []string
		for grp, members := range manager.groups {
			if slices.Contains(members, name) {
				channels = append(channels, grp)
			}
		}
		slices.Sort(channels)
		details = append(details, ClientDetail{
			Name:     name,
			Secure:   conn.Secure,
			Channels: channels,
		})
	}
	slices.SortFunc(details, func(a, b ClientDetail) int {
		return strings.Compare(a.Name, b.Name)
	})
	return details
}
