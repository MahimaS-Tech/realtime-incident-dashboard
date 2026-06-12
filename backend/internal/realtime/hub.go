package realtime

import (
	"context"
	"log"
	"sync/atomic"
)

type Client struct {
	TenantID string
	Events   chan []byte
}

type Broadcast struct {
	TenantID string
	Payload  []byte
}

type Hub struct {
	register      chan *Client
	unregister    chan *Client
	broadcast     chan Broadcast
	clients       map[string]map[*Client]struct{}
	logger        *log.Logger
	clientCounter atomic.Int64
	bufferSize    int
}

func NewHub(logger *log.Logger, bufferSize int) *Hub {
	if bufferSize <= 0 {
		bufferSize = 512
	}
	return &Hub{
		register:   make(chan *Client, 4096),
		unregister: make(chan *Client, 4096),
		broadcast:  make(chan Broadcast, 65536),
		clients:    make(map[string]map[*Client]struct{}),
		logger:     logger,
		bufferSize: bufferSize,
	}
}

func (h *Hub) NewClient(tenantID string) *Client {
	return &Client{TenantID: tenantID, Events: make(chan []byte, h.bufferSize)}
}

func (h *Hub) Register(c *Client)   { h.register <- c }
func (h *Hub) Unregister(c *Client) { h.unregister <- c }

func (h *Hub) Broadcast(tenantID string, payload []byte) {
	select {
	case h.broadcast <- Broadcast{TenantID: tenantID, Payload: payload}:
	default:
		h.logger.Printf("realtime hub overloaded; dropped broadcast for tenant=%s", tenantID)
	}
}

func (h *Hub) ClientCount() int64 { return h.clientCounter.Load() }

func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			for _, set := range h.clients {
				for c := range set {
					close(c.Events)
				}
			}
			return
		case c := <-h.register:
			set := h.clients[c.TenantID]
			if set == nil {
				set = make(map[*Client]struct{})
				h.clients[c.TenantID] = set
			}
			set[c] = struct{}{}
			h.clientCounter.Add(1)
		case c := <-h.unregister:
			if set := h.clients[c.TenantID]; set != nil {
				if _, ok := set[c]; ok {
					delete(set, c)
					close(c.Events)
					h.clientCounter.Add(-1)
				}
				if len(set) == 0 {
					delete(h.clients, c.TenantID)
				}
			}
		case msg := <-h.broadcast:
			set := h.clients[msg.TenantID]
			for c := range set {
				select {
				case c.Events <- msg.Payload:
				default:
					// A slow browser can create backpressure. Drop that connection so the gateway remains low-latency.
					delete(set, c)
					close(c.Events)
					h.clientCounter.Add(-1)
				}
			}
			if len(set) == 0 {
				delete(h.clients, msg.TenantID)
			}
		}
	}
}
