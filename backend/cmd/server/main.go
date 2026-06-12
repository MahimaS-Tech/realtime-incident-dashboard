package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/example/realtime-incident-dashboard/backend/internal/config"
	"github.com/example/realtime-incident-dashboard/backend/internal/events"
	"github.com/example/realtime-incident-dashboard/backend/internal/httpapi"
	"github.com/example/realtime-incident-dashboard/backend/internal/model"
	"github.com/example/realtime-incident-dashboard/backend/internal/realtime"
	"github.com/example/realtime-incident-dashboard/backend/internal/store"
	"github.com/example/realtime-incident-dashboard/backend/internal/worker"
)

func main() {
	cfg := config.Load()
	logger := log.New(os.Stdout, "", log.LstdFlags|log.LUTC|log.Lshortfile)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	role := strings.ToLower(strings.TrimSpace(cfg.Role))
	if role == "" {
		role = "all"
	}

	var st *store.Store
	var bus *events.Bus
	var err error

	needsStore := role == "api" || role == "worker" || role == "all"
	needsBus := role == "api" || role == "worker" || role == "realtime" || role == "all"

	if needsBus {
		bus, err = events.Connect(ctx, cfg, logger)
		must(err, logger, "connect nats")
		defer bus.Close()
		must(bus.EnsureStream(ctx), logger, "ensure nats stream")
	}

	if needsStore {
		st, err = store.Connect(ctx, cfg, logger)
		must(err, logger, "connect postgres")
		defer st.Close()
		must(st.Migrate(ctx), logger, "migrate postgres")
	}

	var hub *realtime.Hub
	if role == "realtime" || role == "all" {
		hub = realtime.NewHub(logger, cfg.SSEClientBuffer)
		_, err := bus.SubscribeLive("incidents.>", func(payload []byte) {
			var ev model.Event
			if err := json.Unmarshal(payload, &ev); err != nil {
				logger.Printf("drop malformed live event: %v", err)
				return
			}
			hub.Broadcast(ev.TenantID, payload)
		})
		must(err, logger, "subscribe live incidents")
	}

	var wg sync.WaitGroup
	runErr := make(chan error, 4)

	if hub != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hub.Run(ctx)
		}()
	}

	if role == "api" || role == "realtime" || role == "all" {
		srv := httpapi.NewServer(cfg, logger, st, bus, hub)
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger.Printf("%s server listening on %s", role, cfg.HTTPAddr)
			runErr <- srv.ListenAndServe(ctx)
		}()
	}

	if role == "worker" || role == "all" {
		wkr := worker.New(cfg, logger, st, bus)
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger.Printf("writer worker started")
			runErr <- wkr.Run(ctx)
		}()
	}

	select {
	case <-ctx.Done():
		logger.Printf("shutdown requested")
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Printf("service stopped with error: %v", err)
		}
		stop()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Printf("shutdown complete")
	case <-time.After(10 * time.Second):
		logger.Printf("shutdown timed out")
	}
}

func must(err error, logger *log.Logger, msg string) {
	if err != nil {
		logger.Fatalf("%s: %v", msg, err)
	}
}
