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
