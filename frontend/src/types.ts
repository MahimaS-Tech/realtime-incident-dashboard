export type Severity = 'SEV1' | 'SEV2' | 'SEV3' | 'SEV4';
export type IncidentStatus = 'open' | 'acked' | 'resolved';

export interface Incident {
  id: string;
  tenant_id: string;
  title: string;
  description: string;
  severity: Severity;
  status: IncidentStatus;
  service: string;
  assignee: string;
  created_at: string;
  updated_at: string;
  version: number;
}

export interface IncidentEvent {
  event_id: string;
  type: 'incident.created' | 'incident.status_changed';
  tenant_id: string;
  incident_id: string;
  incident?: Incident;
  status?: IncidentStatus;
  occurred_at: string;
}
