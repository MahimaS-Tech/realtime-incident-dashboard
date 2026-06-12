package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/example/realtime-incident-dashboard/backend/internal/config"
	"github.com/example/realtime-incident-dashboard/backend/internal/model"
)

type Bus struct {
	cfg    config.Config
	logger *log.Logger
	nc     *nats.Conn
	js     nats.JetStreamContext
}

func Connect(ctx context.Context, cfg config.Config, logger *log.Logger) (*Bus, error) {
	nc, err := nats.Connect(
		cfg.NatsURL,
		nats.Name("incident-dashboard"),
		nats.Timeout(2*time.Second),
		nats.MaxReconnects(cfg.NatsMaxReconnects),
		nats.ReconnectWait(500*time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Printf("nats disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Printf("nats reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			logger.Printf("nats connection closed")
		}),
	)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256 * 1024))
	if err != nil {
		nc.Close()
		return nil, err
	}
	select {
	case <-ctx.Done():
		nc.Close()
		return nil, ctx.Err()
	default:
	}
	return &Bus{cfg: cfg, logger: logger, nc: nc, js: js}, nil
}

func (b *Bus) EnsureStream(ctx context.Context) error {
	if _, err := b.js.StreamInfo(b.cfg.NatsStream); err == nil {
		return nil
	} else if err != nats.ErrStreamNotFound {
		return err
	}

	_, err := b.js.AddStream(&nats.StreamConfig{
		Name:      b.cfg.NatsStream,
		Subjects:  []string{"incidents.>"},
		Retention: nats.LimitsPolicy,
		Storage:   nats.FileStorage,
		Replicas:  b.cfg.NatsReplicas,
		MaxAge:    7 * 24 * time.Hour,
		Discard:   nats.DiscardOld,
	})
	if err != nil {
		// Another replica may have created the stream between StreamInfo and AddStream.
		if _, infoErr := b.js.StreamInfo(b.cfg.NatsStream); infoErr == nil {
			return nil
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return err
	}
}

func (b *Bus) PublishIncidentEvent(ctx context.Context, ev model.Event) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	_, err = b.js.Publish(
		SubjectFor(ev),
		payload,
		nats.MsgId(ev.EventID),
		nats.Context(ctx),
	)
	return err
}

func (b *Bus) PullSubscribe(filterSubject, durable string) (*nats.Subscription, error) {
	return b.js.PullSubscribe(
		filterSubject,
		durable,
		nats.BindStream(b.cfg.NatsStream),
		nats.ManualAck(),
	)
}

func (b *Bus) SubscribeLive(subject string, handler func(payload []byte)) (*nats.Subscription, error) {
	sub, err := b.nc.Subscribe(subject, func(msg *nats.Msg) {
		handler(msg.Data)
	})
	if err != nil {
		return nil, err
	}
	return sub, b.nc.Flush()
}

func (b *Bus) Close() {
	if b.nc != nil {
		b.nc.Drain()
		b.nc.Close()
	}
}

func SubjectFor(ev model.Event) string {
	tenant := cleanSubjectToken(ev.TenantID)
	suffix := "event"
	switch ev.Type {
	case model.EventIncidentCreated:
		suffix = "created"
	case model.EventIncidentStatusChanged:
		suffix = "status_changed"
	}
	return fmt.Sprintf("incidents.%s.%s", tenant, suffix)
}

func cleanSubjectToken(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "demo"
	}
	replacer := strings.NewReplacer(".", "_", "*", "_", ">", "_", " ", "_")
	return replacer.Replace(s)
}
