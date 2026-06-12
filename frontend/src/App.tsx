import { FormEvent, useEffect, useMemo, useState } from 'react';
import { createIncident, listIncidents, openIncidentStream, updateIncidentStatus } from './api';
import type { Incident, IncidentEvent, IncidentStatus, Severity } from './types';

const severities: Severity[] = ['SEV1', 'SEV2', 'SEV3', 'SEV4'];
const statuses: IncidentStatus[] = ['open', 'acked', 'resolved'];

export default function App() {
  const [tenant, setTenant] = useState(localStorage.getItem('tenant') || 'demo');
  const [connected, setConnected] = useState(false);
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [error, setError] = useState('');
  const [form, setForm] = useState({
    title: 'Checkout latency above SLO',
    description: 'p95 latency crossed 1.2s for 5 minutes',
    severity: 'SEV2' as Severity,
    service: 'checkout-api',
    assignee: 'oncall'
  });

  useEffect(() => {
    localStorage.setItem('tenant', tenant);
    listIncidents(tenant).then(setIncidents).catch((err) => setError(err.message));
  }, [tenant]);

  useEffect(() => {
    setConnected(false);
    const es = openIncidentStream(tenant);
    es.addEventListener('ready', () => setConnected(true));
    es.addEventListener('incident', (msg) => {
      const ev: IncidentEvent = JSON.parse((msg as MessageEvent).data);
      setIncidents((current) => applyEvent(current, ev));
    });
    es.onerror = () => setConnected(false);
    return () => es.close();
  }, [tenant]);

  const counts = useMemo(() => {
    return statuses.reduce((acc, status) => {
      acc[status] = incidents.filter((i) => i.status === status).length;
      return acc;
    }, {} as Record<IncidentStatus, number>);
  }, [incidents]);

  async function onCreate(e: FormEvent) {
    e.preventDefault();
    setError('');
    try {
      const optimistic = await createIncident(tenant, form);
      setIncidents((current) => upsertIncident(current, optimistic));
    } catch (err) {
      setError(err instanceof Error ? err.message : 'create failed');
    }
  }

  async function onStatus(id: string, status: IncidentStatus) {
    setError('');
    setIncidents((current) => current.map((i) => i.id === id ? { ...i, status, updated_at: new Date().toISOString() } : i));
    try {
      await updateIncidentStatus(tenant, id, status);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'update failed');
      listIncidents(tenant).then(setIncidents).catch(() => undefined);
    }
  }

  return (
    <main>
      <header className="hero">
        <div>
          <p className="eyebrow">Real-time SRE dashboard</p>
          <h1>Incident Management</h1>
          <p className="subtitle">Async ingestion, live incident stream, idempotent writes, and a scalable event-driven backend.</p>
        </div>
        <div className="tenant-card">
          <label>Tenant</label>
          <input value={tenant} onChange={(e) => setTenant(e.target.value || 'demo')} />
          <span className={connected ? 'pill ok' : 'pill bad'}>{connected ? 'live connected' : 'reconnecting'}</span>
        </div>
      </header>

      {error && <div className="alert">{error}</div>}

      <section className="stats">
        <div><strong>{incidents.length}</strong><span>Total</span></div>
        <div><strong>{counts.open || 0}</strong><span>Open</span></div>
        <div><strong>{counts.acked || 0}</strong><span>Acked</span></div>
        <div><strong>{counts.resolved || 0}</strong><span>Resolved</span></div>
      </section>

      <section className="layout">
        <form className="panel" onSubmit={onCreate}>
          <h2>Create incident</h2>
          <label>Title</label>
          <input value={form.title} onChange={(e) => setForm({ ...form, title: e.target.value })} />
          <label>Description</label>
          <textarea value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} />
          <div className="grid2">
            <div>
              <label>Severity</label>
              <select value={form.severity} onChange={(e) => setForm({ ...form, severity: e.target.value as Severity })}>
                {severities.map((s) => <option key={s}>{s}</option>)}
              </select>
            </div>
            <div>
              <label>Service</label>
              <input value={form.service} onChange={(e) => setForm({ ...form, service: e.target.value })} />
            </div>
          </div>
          <label>Assignee</label>
          <input value={form.assignee} onChange={(e) => setForm({ ...form, assignee: e.target.value })} />
          <button>Create</button>
        </form>

        <section className="panel list">
          <div className="list-head">
            <h2>Live incidents</h2>
            <button className="secondary" onClick={() => listIncidents(tenant).then(setIncidents).catch((err) => setError(err.message))}>Refresh</button>
          </div>
          {incidents.length === 0 && <p className="empty">No incidents yet. Create one or open another tab to watch live updates.</p>}
          {incidents.map((inc) => (
            <article className="incident" key={inc.id}>
              <div className="incident-top">
                <span className={`sev ${inc.severity.toLowerCase()}`}>{inc.severity}</span>
                <span className={`status ${inc.status}`}>{inc.status}</span>
              </div>
              <h3>{inc.title}</h3>
              <p>{inc.description}</p>
              <dl>
                <div><dt>Service</dt><dd>{inc.service || '-'}</dd></div>
                <div><dt>Assignee</dt><dd>{inc.assignee || '-'}</dd></div>
                <div><dt>Updated</dt><dd>{new Date(inc.updated_at).toLocaleString()}</dd></div>
              </dl>
              <div className="actions">
                {statuses.map((s) => <button key={s} className="secondary" disabled={inc.status === s} onClick={() => onStatus(inc.id, s)}>{s}</button>)}
              </div>
            </article>
          ))}
        </section>
      </section>
    </main>
  );
}

function applyEvent(current: Incident[], ev: IncidentEvent): Incident[] {
  if (ev.type === 'incident.created' && ev.incident) {
    return upsertIncident(current, ev.incident);
  }
  if (ev.type === 'incident.status_changed' && ev.status) {
    return current.map((i) => i.id === ev.incident_id ? { ...i, status: ev.status!, updated_at: ev.occurred_at, version: i.version + 1 } : i);
  }
  return current;
}

function upsertIncident(current: Incident[], incident: Incident): Incident[] {
  const exists = current.some((i) => i.id === incident.id);
  const next = exists ? current.map((i) => i.id === incident.id ? incident : i) : [incident, ...current];
  return next.sort((a, b) => Date.parse(b.updated_at) - Date.parse(a.updated_at));
}
