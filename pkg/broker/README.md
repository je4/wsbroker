# Package `broker`

The `broker` package manages WebSocket connections, thread-safe group/channel memberships, and concurrent asynchronous message distribution across worker goroutines.

---

## Core Components

- **`Connection`**: Wraps a [`coder/websocket`](https://github.com/coder/websocket) socket connection. Provides mutex-guarded JSON/binary writing (`WriteJSON`, `WriteMessage`) and graceful disconnection (`Close`).
- **`ConnectionManager`**: The central broker instance managing:
  - Registered WebSocket connections (`wsConns: map[string]*Connection`).
  - Channel subscription lists (`groups: map[string][]string`).
  - Asynchronous worker dispatch queue (`senderChannel: chan *job`).

---

## Architecture Diagram

```mermaid
classDiagram
    class ConnectionManager {
        -map[string]*Connection wsConns
        -RWMutex wsConnsMu
        -map[string][]string groups
        -RWMutex groupsMu
        -chan *job senderChannel
        -WaitGroup workerWG
        +Start(numWorkers int)
        +Close()
        +Send(evt *Event) error
        +AddWSConn(conn *Connection) error
        +GetWSConn(name string) (*Connection, bool)
        +RemoveWSConn(name string)
        +DisconnectClient(name string) bool
        +AddToGroup(name string, group string)
        +RemoveFromGroup(name string, group string)
        +GetChannels() []string
        +GetChannelMembers(group string) []string
        +GetClients() []string
        +GetClientDetails() []ClientDetail
    }

    class Connection {
        +bool Secure
        +Conn *websocket.Conn
        +string Name
        -Mutex writeMu
        +WriteJSON(ctx context.Context, v any) error
        +WriteMessage(ctx context.Context, msgType, data) error
        +Close() error
    }

    class Job {
        +*Event evt
        +string dest
    }

    ConnectionManager "1" o-- "*" Connection : manages
    ConnectionManager "1" --> "*" Job : enqueues to workers
```

---

## Worker Pipeline & Dispatching

```mermaid
flowchart LR
    Publisher[Send Event] --> CM[ConnectionManager.Send]
    CM --> TargetCheck{Target is Channel or Client?}
    TargetCheck -- Channel --> FanOut[Fan-out to all channel subscribers]
    TargetCheck -- Client --> Direct[Single destination]
    FanOut --> Q[(senderChannel Queue)]
    Direct --> Q
    Q --> W1[Worker #1]
    Q --> W2[Worker #2]
    Q --> WN[Worker #N]
    W1 --> S1[Connection.WriteJSON]
    W2 --> S2[Connection.WriteJSON]
    WN --> SN[Connection.WriteJSON]
```

---

## Usage Example

```go
logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
cm := broker.NewConnectionManager(false, &logger)

// Start with 4 worker goroutines
cm.Start(4)
defer cm.Close()

// Register a connection
conn := broker.NewConnection(wsConn, "client-123", true)
cm.AddWSConn(conn)

// Subscribe client to channel
cm.AddToGroup("client-123", "notifications")

// Send a broadcast message
evt, _ := event.NewMessageEvent("server", "notifications", "Hello Subscribers!")
cm.Send(evt)
```
