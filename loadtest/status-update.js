import http from 'k6/http';
import { check } from 'k6';

export const options = {
  vus: 20,
  duration: '30s',
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<250']
  }
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const TENANT_ID = __ENV.TENANT_ID || 'demo';
const INCIDENT_ID = __ENV.INCIDENT_ID;

export default function () {
  if (!INCIDENT_ID) throw new Error('Set INCIDENT_ID');
  const status = ['open', 'acked', 'resolved'][Math.floor(Math.random() * 3)];
  const res = http.patch(`${BASE_URL}/v1/incidents/${INCIDENT_ID}/status`, JSON.stringify({ status }), {
    headers: {
      'Content-Type': 'application/json',
      'X-Tenant-ID': TENANT_ID,
      'Idempotency-Key': `${__VU}-${__ITER}-${Date.now()}`
    }
  });
  check(res, { accepted: (r) => r.status === 202 });
}
