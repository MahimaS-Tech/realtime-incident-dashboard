package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/example/realtime-incident-dashboard/backend/internal/config"
	"github.com/example/realtime-incident-dashboard/backend/internal/events"
	"github.com/example/realtime-incident-dashboard/backend/internal/id"
	"github.com/example/realtime-incident-dashboard/backend/internal/model"
	"github.com/example/realtime-incident-dashboard/backend/internal/realtime"
	"github.com/example/realtime-incident-dashboard/backend/internal/store"
)

type Server struct {
	cfg      config.Config
	logger   *log.Logger
	store    *store.Store
	bus      *events.Bus
	hub      *realtime.Hub
	requests atomic.Uint64
	errors   atomic.Uint64
	inflight atomic.Int64
}

func NewServer(cfg config.Config, logger *log.Logger, st *store.Store, bus *events.Bus, hub *realtime.Hub) *Server {
	return &Server{cfg: cfg, logger: logger, store: st, bus: bus, hub: hub}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/v1/incidents", s.withMiddleware(s.handleIncidents))
	mux.HandleFunc("/v1/incidents/", s.withMiddleware(s.handleIncidentByID))
	mux.HandleFunc("/v1/events", s.withMiddleware(s.handleEvents))

	srv := &http.Server{
		Addr:              s.cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      0, // SSE streams are long-lived.
		IdleTimeout:       90 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	err := srv.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func (s *Server) withMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		if s.cfg.MaxInFlight > 0 {
			current := s.inflight.Add(1)
			if current > int64(s.cfg.MaxInFlight) {
				s.inflight.Add(-1)
				s.errors.Add(1)
				writeError(w, http.StatusServiceUnavailable, "server overloaded")
				return
			}
			defer s.inflight.Add(-1)
		}
		if s.cfg.CORSOrigin != "" {
			origin := r.Header.Get("Origin")
			if s.cfg.CORSOrigin == "*" || origin == s.cfg.CORSOrigin {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				if s.cfg.CORSOrigin == "*" && origin == "" {
					w.Header().Set("Access-Control-Allow-Origin", "*")
				}
				w.Header().Set("Vary", "Origin")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Tenant-ID, Idempotency-Key")
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, OPTIONS")
			}
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		defer func() {
			if rec := recover(); rec != nil {
				s.errors.Add(1)
				s.logger.Printf("panic: %v", rec)
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next(w, r)
	}
}

func (s *Server) handleRoot(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"service": "realtime-incident-dashboard", "status": "ok"})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	if s.store != nil {
		if err := s.store.Ping(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "postgres not ready")
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	clients := int64(0)
	if s.hub != nil {
		clients = s.hub.ClientCount()
	}
	fmt.Fprintf(w, "incident_http_requests_total %d\n", s.requests.Load())
	fmt.Fprintf(w, "incident_http_errors_total %d\n", s.errors.Load())
	fmt.Fprintf(w, "incident_http_inflight %d\n", s.inflight.Load())
	fmt.Fprintf(w, "incident_realtime_clients %d\n", clients)
}

func (s *Server) handleIncidents(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.listIncidents(w, r)
	case http.MethodPost:
		s.createIncident(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleIncidentByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/incidents/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "incident not found")
		return
	}
	incidentID := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		s.getIncident(w, r, incidentID)
		return
	}
	if len(parts) == 2 && parts[1] == "status" && r.Method == http.MethodPatch {
		s.updateStatus(w, r, incidentID)
		return
	}
	writeError(w, http.StatusNotFound, "route not found")
}

func (s *Server) listIncidents(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store unavailable")
		return
	}
	tenant := tenantFromRequest(r)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	incidents, err := s.store.ListIncidents(ctx, tenant, r.URL.Query().Get("status"), r.URL.Query().Get("severity"), limit)
	if err != nil {
		s.errors.Add(1)
		writeError(w, http.StatusInternalServerError, "list incidents failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": incidents})
}

func (s *Server) getIncident(w http.ResponseWriter, r *http.Request, incidentID string) {
	if s.store == nil {
		writeError(w, http.StatusServiceUnavailable, "store unavailable")
		return
	}
	tenant := tenantFromRequest(r)
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	inc, err := s.store.GetIncident(ctx, tenant, incidentID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "incident not found")
		return
	}
	if err != nil {
		s.errors.Add(1)
		writeError(w, http.StatusInternalServerError, "get incident failed")
		return
	}
	writeJSON(w, http.StatusOK, inc)
}

func (s *Server) createIncident(w http.ResponseWriter, r *http.Request) {
	if s.bus == nil {
		writeError(w, http.StatusServiceUnavailable, "event bus unavailable")
		return
	}
	var req model.CreateIncidentRequest
	if err := decodeJSON(w, r, s.cfg.MaxBodyBytes, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	req, err := model.ValidateCreate(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	now := time.Now().UTC()
	tenant := tenantFromRequest(r)
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	incidentID := id.New("inc")
	eventID := id.New("evt")
	if idempotencyKey != "" {
		incidentID = id.Stable("inc", tenant, "create", idempotencyKey)
		eventID = id.Stable("evt", tenant, "create", idempotencyKey)
	}
	inc := model.Incident{
		ID:          incidentID,
		TenantID:    tenant,
		Title:       req.Title,
		Description: req.Description,
		Severity:    req.Severity,
		Status:      model.StatusOpen,
		Service:     req.Service,
		Assignee:    req.Assignee,
		CreatedAt:   now,
		UpdatedAt:   now,
		Version:     1,
	}
	ev := model.Event{
		EventID:        eventID,
		Type:           model.EventIncidentCreated,
		TenantID:       tenant,
		IncidentID:     inc.ID,
		IdempotencyKey: idempotencyKey,
		Incident:       &inc,
		OccurredAt:     now,
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	if err := s.bus.PublishIncidentEvent(ctx, ev); err != nil {
		s.errors.Add(1)
		writeError(w, http.StatusServiceUnavailable, "publish failed")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true, "incident": inc, "event_id": ev.EventID})
}

func (s *Server) updateStatus(w http.ResponseWriter, r *http.Request, incidentID string) {
	if s.bus == nil {
		writeError(w, http.StatusServiceUnavailable, "event bus unavailable")
		return
	}
	var req model.UpdateStatusRequest
	if err := decodeJSON(w, r, s.cfg.MaxBodyBytes, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	status, err := model.ValidateStatus(req.Status)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	now := time.Now().UTC()
	tenant := tenantFromRequest(r)
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	eventID := id.New("evt")
	if idempotencyKey != "" {
		eventID = id.Stable("evt", tenant, "status", incidentID, idempotencyKey)
	}
	ev := model.Event{
		EventID:        eventID,
		Type:           model.EventIncidentStatusChanged,
		TenantID:       tenant,
		IncidentID:     incidentID,
		IdempotencyKey: idempotencyKey,
		Status:         status,
		OccurredAt:     now,
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	if err := s.bus.PublishIncidentEvent(ctx, ev); err != nil {
		s.errors.Add(1)
		writeError(w, http.StatusServiceUnavailable, "publish failed")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true, "incident_id": incidentID, "status": status, "event_id": ev.EventID})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if s.hub == nil {
		writeError(w, http.StatusServiceUnavailable, "realtime hub unavailable")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	tenant := tenantFromRequest(r)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	client := s.hub.NewClient(tenant)
	s.hub.Register(client)
	defer s.hub.Unregister(client)

	ready, _ := json.Marshal(map[string]string{"tenant_id": tenant})
	fmt.Fprintf(w, "event: ready\ndata: %s\n\n", ready)
	flusher.Flush()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case payload, ok := <-client.Events:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: incident\ndata: %s\n\n", payload)
			flusher.Flush()
		case <-heartbeat.C:
			fmt.Fprintf(w, ": heartbeat %d\n\n", time.Now().Unix())
			flusher.Flush()
		}
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("body must contain a single JSON object")
	}
	return nil
}

func tenantFromRequest(r *http.Request) string {
	tenant := r.Header.Get("X-Tenant-ID")
	if tenant == "" {
		tenant = r.URL.Query().Get("tenant")
	}
	return model.NormalizeTenant(tenant)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, model.APIError{Error: msg})
}
