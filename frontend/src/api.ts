import type { Incident, IncidentStatus, Severity } from './types';

const API_BASE = import.meta.env.VITE_API_BASE || '/api';
const EVENTS_BASE = import.meta.env.VITE_EVENTS_BASE || '/events';

export async function listIncidents(tenant: string): Promise<Incident[]> {
  const res = await fetch(`${API_BASE}/incidents?limit=100`, {
    headers: { 'X-Tenant-ID': tenant }
  });
  if (!res.ok) throw new Error(await errorText(res));
  const body = await res.json();
  return body.items ?? [];
}

export async function createIncident(tenant: string, input: {
  title: string;
  description: string;
  severity: Severity;
  service: string;
  assignee: string;
}): Promise<Incident> {
  const res = await fetch(`${API_BASE}/incidents`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Tenant-ID': tenant,
      'Idempotency-Key': crypto.randomUUID()
    },
    body: JSON.stringify(input)
  });
  if (!res.ok) throw new Error(await errorText(res));
  const body = await res.json();
  return body.incident;
}

export async function updateIncidentStatus(tenant: string, incidentId: string, status: IncidentStatus): Promise<void> {
  const res = await fetch(`${API_BASE}/incidents/${incidentId}/status`, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
      'X-Tenant-ID': tenant,
      'Idempotency-Key': crypto.randomUUID()
    },
    body: JSON.stringify({ status })
  });
  if (!res.ok) throw new Error(await errorText(res));
}

export function openIncidentStream(tenant: string): EventSource {
  const url = new URL(EVENTS_BASE, window.location.origin);
  url.searchParams.set('tenant', tenant);
  return new EventSource(url.toString());
}

async function errorText(res: Response): Promise<string> {
  try {
    const body = await res.json();
    return body.error || res.statusText;
  } catch {
    return res.statusText;
  }
}
