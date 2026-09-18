package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// ---- Configuration ----
const listenAddr = ":8080"

var targets = []string{
	"ws://10.0.4.38:9001/ws",
	"ws://10.0.4.41:9001/ws",
	"ws://10.0.4.42:9001/ws",
	"ws://10.0.4.60:9001/ws",
	"ws://10.0.4.67:9001/ws",
	"ws://10.0.4.76:9001/ws",
}

const offlineAfter = 30 * time.Second

// ------------------------

type DeviceMetrics struct {
	DeviceID     string    `json:"device_id"`
	Hostname     string    `json:"hostname"`
	Timestamp    time.Time `json:"timestamp"`
	CPUUsage     float64   `json:"cpu_usage"`
	CPUTemp      float64   `json:"cpu_temp"`
	CoreCPUUsage []float64 `json:"core_cpu_usage"`
	TotalMemory  uint64    `json:"total_memory"`
	UsedMemory   uint64    `json:"used_memory"`
	DiskRead     uint64    `json:"disk_read"`
	DiskWrite    uint64    `json:"disk_write"`
	DiskUsagePct float64   `json:"disk_usage_pct"`
	NetRX        uint64    `json:"net_rx"`
	NetTX        uint64    `json:"net_tx"`
}

type wsMessage struct {
	Type     string          `json:"type"`
	Device   *DeviceMetrics  `json:"device,omitempty"`
	Devices  []DeviceMetrics `json:"devices,omitempty"`
	Status   *DeviceStatus   `json:"status,omitempty"`
	Statuses []DeviceStatus  `json:"statuses,omitempty"`
}

type DeviceStatus struct {
	DeviceID string    `json:"device_id"`
	Hostname string    `json:"hostname"`
	Target   string    `json:"target"`
	State    string    `json:"state"`
	Online   bool      `json:"online"`
	LastSeen time.Time `json:"last_seen,omitempty"`
	Since    time.Time `json:"since,omitempty"`
}

type client struct {
	conn *websocket.Conn
	send chan []byte
}

type hub struct {
	mu sync.RWMutex

	latest map[string]DeviceMetrics
	status map[string]DeviceStatus

	clients    map[*client]struct{}
	register   chan *client
	unregister chan *client
	broadcast  chan metricUpdate
}

func newHub() *hub {
	return &hub{
		latest:     make(map[string]DeviceMetrics),
		status:     make(map[string]DeviceStatus),
		clients:    make(map[*client]struct{}),
		register:   make(chan *client),
		unregister: make(chan *client),
		broadcast:  make(chan metricUpdate, 256),
	}
}

func newHubWithTargets(targets []string) *hub {
	h := newHub()
	for _, target := range targets {
		h.status[target] = DeviceStatus{
			DeviceID: target,
			Hostname: target,
			Target:   target,
			State:    "connecting",
			Since:    time.Now().UTC(),
		}
	}
	return h
}

func (h *hub) run(ctx context.Context) {
	statusTicker := time.NewTicker(time.Second)
	defer statusTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			h.mu.Lock()
			for c := range h.clients {
				close(c.send)
				_ = c.conn.Close()
			}
			h.clients = map[*client]struct{}{}
			h.mu.Unlock()
			return

		case c := <-h.register:
			h.mu.Lock()
			h.clients[c] = struct{}{}
			snapshot := make([]DeviceMetrics, 0, len(h.latest))
			for _, m := range h.latest {
				snapshot = append(snapshot, m)
			}
			statuses := make([]DeviceStatus, 0, len(h.status))
			for _, status := range h.status {
				statuses = append(statuses, status)
			}
			h.mu.Unlock()

			payload, err := json.Marshal(wsMessage{Type: "snapshot", Devices: snapshot, Statuses: statuses})
			if err == nil {
				select {
				case c.send <- payload:
				default:
					close(c.send)
					h.removeClient(c)
				}
			}

		case c := <-h.unregister:
			h.removeClient(c)

		case <-statusTicker.C:
			h.markOffline(time.Now().UTC())

		case update := <-h.broadcast:
			metrics := update.metrics
			h.mu.Lock()
			h.latest[metrics.DeviceID] = metrics
			previous, hadPrevious := h.status[update.target]
			status := DeviceStatus{
				DeviceID: metrics.DeviceID,
				Hostname: metrics.Hostname,
				Target:   update.target,
				State:    "online",
				Online:   true,
				LastSeen: time.Now().UTC(),
				Since:    time.Now().UTC(),
			}
			h.status[update.target] = status

			payload, err := json.Marshal(wsMessage{Type: "update", Device: &metrics})
			if err != nil {
				h.mu.Unlock()
				continue
			}
			statusPayload := []byte(nil)
			if !hadPrevious || !previous.Online || previous.DeviceID != status.DeviceID {
				statusPayload, _ = json.Marshal(wsMessage{Type: "status", Status: &status})
			}

			for c := range h.clients {
				if statusPayload != nil {
					select {
					case c.send <- statusPayload:
					default:
						close(c.send)
						delete(h.clients, c)
						_ = c.conn.Close()
						continue
					}
				}
				select {
				case c.send <- payload:
				default:
					close(c.send)
					delete(h.clients, c)
					_ = c.conn.Close()
				}
			}
			h.mu.Unlock()
		}
	}
}

func (h *hub) markOffline(now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for target, status := range h.status {
		if status.State == "offline" ||
			(status.State == "online" && now.Sub(status.LastSeen) <= offlineAfter) ||
			(status.State == "connecting" && now.Sub(status.Since) <= offlineAfter) {
			continue
		}
		status.State = "offline"
		status.Online = false
		h.status[target] = status
		payload, err := json.Marshal(wsMessage{Type: "status", Status: &status})
		if err != nil {
			continue
		}
		for c := range h.clients {
			select {
			case c.send <- payload:
			default:
				close(c.send)
				delete(h.clients, c)
				_ = c.conn.Close()
			}
		}
	}
}

func (h *hub) removeClient(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
		_ = c.conn.Close()
	}
}

type metricUpdate struct {
	metrics DeviceMetrics
	target  string
}

func (h *hub) submit(metrics DeviceMetrics, target string) {
	h.broadcast <- metricUpdate{metrics: metrics, target: target}
}

func (h *hub) snapshot() []DeviceMetrics {
	h.mu.RLock()
	defer h.mu.RUnlock()

	items := make([]DeviceMetrics, 0, len(h.latest))
	for _, m := range h.latest {
		items = append(items, m)
	}

	return items
}

type config struct {
	ListenAddr string
	Targets    []string
}

func loadConfig() (config, error) {
	if listenAddr == "" {
		return config{}, fmt.Errorf("listenAddr is not set")
	}
	if len(targets) == 0 {
		return config{}, fmt.Errorf("targets list is empty")
	}
	return config{ListenAddr: listenAddr, Targets: targets}, nil
}

func collector(ctx context.Context, target string, h *hub) {
	const (
		initialBackoff = 2 * time.Second
		maxBackoff     = 30 * time.Second
	)

	backoff := initialBackoff
	for {
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, target, nil)
		if err != nil {
			log.Printf("collector: failed to connect to %s: %v; retrying in %s", target, err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			continue
		}

		log.Printf("collector: connected to %s", target)
		backoff = initialBackoff

		readErr := func() error {
			defer conn.Close()
			for {
				_, msg, err := conn.ReadMessage()
				if err != nil {
					return err
				}
				var metrics DeviceMetrics
				if err := json.Unmarshal(msg, &metrics); err != nil {
					log.Printf("collector: %s: invalid payload: %v", target, err)
					continue
				}
				if metrics.DeviceID == "" {
					log.Printf("collector: %s: payload missing device_id, dropping", target)
					continue
				}
				if metrics.Hostname == "" {
					metrics.Hostname = "unknown-host"
				}
				if metrics.Timestamp.IsZero() {
					metrics.Timestamp = time.Now().UTC()
				}
				h.submit(metrics, target)
			}
		}()

		select {
		case <-ctx.Done():
			return
		default:
		}

		log.Printf("collector: disconnected from %s: %v; retrying in %s", target, readErr, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func main() {
	webUI := flag.Bool("w", false, "run web UI (HTTP server + browser dashboard)")
	termUI := flag.Bool("u", false, "run terminal UI")
	flag.Parse()

	if !*webUI && !*termUI {
		fmt.Fprintln(os.Stderr, "monitorhub: specify -w (web UI) or -u (terminal UI)")
		flag.Usage()
		os.Exit(1)
	}
	if *webUI && *termUI {
		fmt.Fprintln(os.Stderr, "monitorhub: -w and -u are mutually exclusive")
		os.Exit(1)
	}

	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	h := newHubWithTargets(cfg.Targets)
	go h.run(ctx)

	for _, target := range cfg.Targets {
		go collector(ctx, target, h)
	}

	if *webUI {
		runWebUI(ctx, cfg, h)
	} else {
		runTUI(ctx, h)
	}
}
