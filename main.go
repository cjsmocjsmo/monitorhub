package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
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
}
// ------------------------

type DeviceMetrics struct {
	DeviceID     string    `json:"device_id"`
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

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func writePump(c *client) {
	ticker := time.NewTicker(25 * time.Second)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func readPump(c *client, h *hub) {
	defer func() {
		h.unregister <- c
	}()

	c.conn.SetReadLimit(1024)
	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			break
		}
	}
}

func latestHandler(h *hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(h.snapshot())
	}
}

func wsHandler(h *hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		c := &client{
			conn: conn,
			send: make(chan []byte, 32),
		}

		h.register <- c

		go writePump(c)
		go readPump(c, h)
	}
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

	mux := http.NewServeMux()
	mux.Handle("/metrics/latest", latestHandler(h))
	mux.Handle("/ws", wsHandler(h))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "monhub.html")
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("graceful shutdown failed: %v", err)
		}
	}()

	log.Printf("monitorhub listening on %s", cfg.ListenAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
