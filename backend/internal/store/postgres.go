package store

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/realtime-incident-dashboard/backend/internal/config"
	"github.com/example/realtime-incident-dashboard/backend/internal/model"
)

type Store struct {
	pool   *pgxpool.Pool
	logger *log.Logger
}

func Connect(ctx context.Context, cfg config.Config, logger *log.Logger) (*Store, error) {
	pgCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	pgCfg.MaxConns = cfg.PGMaxConns
	pgCfg.MinConns = cfg.PGMinConns
	pgCfg.MaxConnLifetime = 30 * time.Minute
	pgCfg.MaxConnIdleTime = 5 * time.Minute
	pgCfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, pgCfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool, logger: logger}, nil
}

func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

func (s *Store) ListIncidents(ctx context.Context, tenantID, status, severity string, limit int) ([]model.Incident, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}

	args := []any{tenantID}
	where := []string{"tenant_id = $1"}
	if status != "" {
		args = append(args, model.NormalizeStatus(status))
		where = append(where, fmt.Sprintf("status = $%d", len(args)))
	}
	if severity != "" {
		args = append(args, model.NormalizeSeverity(severity))
		where = append(where, fmt.Sprintf("severity = $%d", len(args)))
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT id, tenant_id, title, description, severity, status, service, assignee, created_at, updated_at, version
		FROM incidents
		WHERE %s
		ORDER BY updated_at DESC
		LIMIT $%d`, strings.Join(where, " AND "), len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	incidents := make([]model.Incident, 0, limit)
	for rows.Next() {
		var inc model.Incident
		if err := rows.Scan(&inc.ID, &inc.TenantID, &inc.Title, &inc.Description, &inc.Severity, &inc.Status, &inc.Service, &inc.Assignee, &inc.CreatedAt, &inc.UpdatedAt, &inc.Version); err != nil {
			return nil, err
		}
		incidents = append(incidents, inc)
	}
	return incidents, rows.Err()
}

func (s *Store) GetIncident(ctx context.Context, tenantID, incidentID string) (model.Incident, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, tenant_id, title, description, severity, status, service, assignee, created_at, updated_at, version
		FROM incidents
		WHERE tenant_id = $1 AND id = $2`, tenantID, incidentID)

	var inc model.Incident
	if err := row.Scan(&inc.ID, &inc.TenantID, &inc.Title, &inc.Description, &inc.Severity, &inc.Status, &inc.Service, &inc.Assignee, &inc.CreatedAt, &inc.UpdatedAt, &inc.Version); err != nil {
		return model.Incident{}, err
	}
	return inc, nil
}

func (s *Store) ApplyEvents(ctx context.Context, events []model.Event) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	applied := 0
	for _, ev := range events {
		fresh, err := insertProcessedEvent(ctx, tx, ev)
		if err != nil {
			return applied, err
		}
		if !fresh {
			continue
		}
		switch ev.Type {
		case model.EventIncidentCreated:
			if ev.Incident == nil {
				return applied, errors.New("created event missing incident")
			}
			if err := upsertIncident(ctx, tx, *ev.Incident); err != nil {
				return applied, err
			}
		case model.EventIncidentStatusChanged:
			if err := updateIncidentStatus(ctx, tx, ev.TenantID, ev.IncidentID, ev.Status, ev.OccurredAt); err != nil {
				return applied, err
			}
		default:
			return applied, fmt.Errorf("unknown event type: %s", ev.Type)
		}
		applied++
	}
	if err := tx.Commit(ctx); err != nil {
		return applied, err
	}
	return applied, nil
}

func insertProcessedEvent(ctx context.Context, tx pgx.Tx, ev model.Event) (bool, error) {
	var eventID string
	err := tx.QueryRow(ctx, `
		INSERT INTO processed_events(event_id, tenant_id, incident_id, idempotency_key, event_type, created_at)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6)
		ON CONFLICT DO NOTHING
		RETURNING event_id`, ev.EventID, ev.TenantID, ev.IncidentID, ev.IdempotencyKey, ev.Type, ev.OccurredAt).Scan(&eventID)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return false, err
}

func upsertIncident(ctx context.Context, tx pgx.Tx, inc model.Incident) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO incidents(id, tenant_id, title, description, severity, status, service, assignee, created_at, updated_at, version)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (tenant_id, id) DO UPDATE SET
			title = EXCLUDED.title,
			description = EXCLUDED.description,
			severity = EXCLUDED.severity,
			status = EXCLUDED.status,
			service = EXCLUDED.service,
			assignee = EXCLUDED.assignee,
			updated_at = EXCLUDED.updated_at,
			version = incidents.version + 1`,
		inc.ID, inc.TenantID, inc.Title, inc.Description, inc.Severity, inc.Status, inc.Service, inc.Assignee, inc.CreatedAt, inc.UpdatedAt, inc.Version)
	return err
}

func updateIncidentStatus(ctx context.Context, tx pgx.Tx, tenantID, incidentID, status string, updatedAt time.Time) error {
	cmd, err := tx.Exec(ctx, `
		UPDATE incidents
		SET status = $1, updated_at = $2, version = version + 1
		WHERE tenant_id = $3 AND id = $4`, status, updatedAt, tenantID, incidentID)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		// Keep the event for audit, but do not fail the whole batch.
		return nil
	}
	return nil
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS incidents (
    id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    severity TEXT NOT NULL CHECK (severity IN ('SEV1','SEV2','SEV3','SEV4')),
    status TEXT NOT NULL CHECK (status IN ('open','acked','resolved')),
    service TEXT NOT NULL DEFAULT '',
    assignee TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    PRIMARY KEY (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS incidents_tenant_status_updated_idx
ON incidents(tenant_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS incidents_tenant_severity_updated_idx
ON incidents(tenant_id, severity, updated_at DESC);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    incident_id TEXT NOT NULL,
    idempotency_key TEXT,
    event_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS processed_events_tenant_idempotency_unique
ON processed_events(tenant_id, idempotency_key)
WHERE idempotency_key IS NOT NULL AND idempotency_key <> '';
`
