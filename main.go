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
	"ws://10.0.4.67:9001/ws",
 	"ws://10.0.4.60:9001/ws",
	"ws://10.0.4.76:9001/ws",
	"ws://10.0.4.41:9001/ws",
}
// ------------------------

type DeviceMetrics struct {
	DeviceID     string    `json:"device_id"`
	Hostname     string    `json:"hostname"`
	Timestamp    time.Time `json:"timestamp"`
	CPUUsage     float64   `json:"cpu_usage"`
	CoreCPUUsage []float64 `json:"core_cpu_usage"`
	TotalMemory  uint64    `json:"total_memory"`
	UsedMemory   uint64    `json:"used_memory"`
	DiskRead     uint64    `json:"disk_read"`
	DiskWrite    uint64    `json:"disk_write"`
	NetRX        uint64    `json:"net_rx"`
	NetTX        uint64    `json:"net_tx"`
}

type wsMessage struct {
	Type    string          `json:"type"`
	Device  *DeviceMetrics  `json:"device,omitempty"`
	Devices []DeviceMetrics `json:"devices,omitempty"`
}

type client struct {
	conn *websocket.Conn
	send chan []byte
}

type hub struct {
	mu sync.RWMutex

	latest map[string]DeviceMetrics

	clients    map[*client]struct{}
	register   chan *client
	unregister chan *client
	broadcast  chan DeviceMetrics
}

func newHub() *hub {
	return &hub{
		latest:     make(map[string]DeviceMetrics),
		clients:    make(map[*client]struct{}),
		register:   make(chan *client),
		unregister: make(chan *client),
		broadcast:  make(chan DeviceMetrics, 256),
	}
}

func (h *hub) run(ctx context.Context) {
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
			h.mu.Unlock()

			payload, err := json.Marshal(wsMessage{Type: "snapshot", Devices: snapshot})
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

		case metrics := <-h.broadcast:
			h.mu.Lock()
			h.latest[metrics.DeviceID] = metrics

			payload, err := json.Marshal(wsMessage{Type: "update", Device: &metrics})
			if err != nil {
				h.mu.Unlock()
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
			h.mu.Unlock()
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

func (h *hub) submit(metrics DeviceMetrics) {
	h.broadcast <- metrics
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
				h.submit(metrics)
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

	h := newHub()
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
