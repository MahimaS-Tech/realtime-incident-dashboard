package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/example/realtime-incident-dashboard/backend/internal/config"
	"github.com/example/realtime-incident-dashboard/backend/internal/events"
	"github.com/example/realtime-incident-dashboard/backend/internal/model"
	"github.com/example/realtime-incident-dashboard/backend/internal/store"
)

type Worker struct {
	cfg    config.Config
	logger *log.Logger
	store  *store.Store
	bus    *events.Bus
}

func New(cfg config.Config, logger *log.Logger, st *store.Store, bus *events.Bus) *Worker {
	return &Worker{cfg: cfg, logger: logger, store: st, bus: bus}
}

func (w *Worker) Run(ctx context.Context) error {
	sub, err := w.bus.PullSubscribe("incidents.>", "incident-writer")
	if err != nil {
		return err
	}

	batchSize := w.cfg.WorkerBatchSize
	if batchSize <= 0 {
		batchSize = 500
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgs, err := sub.Fetch(batchSize, nats.MaxWait(w.cfg.WorkerFetchWait))
		if errors.Is(err, nats.ErrTimeout) {
			continue
		}
		if err != nil {
			w.logger.Printf("fetch failed: %v", err)
			time.Sleep(250 * time.Millisecond)
			continue
		}

		evs := make([]model.Event, 0, len(msgs))
		validMsgs := make([]*nats.Msg, 0, len(msgs))
		for _, msg := range msgs {
			var ev model.Event
			if err := json.Unmarshal(msg.Data, &ev); err != nil {
				w.logger.Printf("malformed event terminated: %v", err)
				_ = msg.Term()
				continue
			}
			evs = append(evs, ev)
			validMsgs = append(validMsgs, msg)
		}
		if len(evs) == 0 {
			continue
		}

		applyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		applied, err := w.store.ApplyEvents(applyCtx, evs)
		cancel()
		if err != nil {
			w.logger.Printf("apply batch failed: %v", err)
			for _, msg := range validMsgs {
				_ = msg.Nak()
			}
			continue
		}
		for _, msg := range validMsgs {
			_ = msg.Ack()
		}
		w.logger.Printf("processed batch messages=%d applied=%d", len(validMsgs), applied)
	}
}
