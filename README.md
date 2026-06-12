# Real-Time Incident Management Dashboard

A complete end-to-end real-time incident management project designed as a scalable portfolio-ready system:

- Go backend API for incident ingestion and status updates
- NATS JetStream event log for durable async ingestion
- PostgreSQL projection store for querying incidents
- SSE realtime gateway for live dashboard updates
- React + Vite frontend
- Docker Compose for local development
- Kubernetes manifests and HPA examples
- k6 load-test scripts

> Important: "1M requests/sec" is not a property of source code alone. This repo is designed for horizontal scaling and low-latency async ingestion, but you must prove any target with distributed load testing, production-grade NATS/Postgres topology, kernel/network tuning, observability, and capacity planning.

## Architecture

```text
Browser Dashboard
  |  GET /api/incidents                         GET /events SSE stream
  v                                            ^
Frontend Nginx ---------------------> API pods | Realtime gateway pods
                                      |        |
                                      |        | NATS live subscription
                                      v        |
                               NATS JetStream  <---- durable event stream
                                      |
                                      v
                              Worker pods, batched consumers
                                      |
                                      v
                              PostgreSQL projection store
```

### Why async ingestion?

The write path returns after the event is durably accepted by NATS JetStream, not after a synchronous database transaction. Workers persist events to PostgreSQL in batches. This keeps the API low-latency and lets you scale API pods, worker pods, and realtime gateway pods independently.

## Local run

```bash
cp k8s/secret.example.yaml /tmp/ignore-this.yaml # only for reference
make up
```

Open:

```text
http://localhost:3000
```

API examples:

```bash
curl -X POST http://localhost:8080/v1/incidents \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: demo' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"title":"Payments down","description":"5xx spike","severity":"SEV1","service":"payments","assignee":"oncall"}'

curl -H 'X-Tenant-ID: demo' http://localhost:8080/v1/incidents
```

SSE realtime stream:

```bash
curl -N 'http://localhost:8081/v1/events?tenant=demo'
```

## Load testing

Install k6, then:

```bash
BASE_URL=http://localhost:8080 TENANT_ID=loadtest k6 run loadtest/ingest.js
```

To approach very high request rates, do not run k6 from one laptop. Use distributed load generators in the same region as the cluster, gradually ramp traffic, and watch p95/p99 latency, NATS publish ack latency, NATS stream disk I/O, worker lag, Postgres write throughput, CPU throttling, dropped SSE clients, and error rate.

## Production scaling checklist

For a serious 1M req/sec target, use this implementation pattern but upgrade the deployment:

1. **API layer**
   - Keep API stateless.
   - Horizontally scale API pods behind a layer-7 load balancer.
   - Avoid synchronous DB writes on the hot path.
   - Apply request size limits, idempotency keys, timeouts, and admission control.

2. **Event bus**
   - Run NATS as a multi-node JetStream cluster.
   - Use file storage on fast disks.
   - Partition subjects by tenant or region when needed.
   - Monitor publish ack latency, stream storage, consumer lag, and redeliveries.

3. **Workers and database**
   - Batch database writes.
   - Partition PostgreSQL tables by tenant/time for large installations.
   - Add read replicas for dashboard-heavy workloads.
   - Keep processed_events retention bounded with TTL/partition cleanup.

4. **Realtime gateway**
   - Keep realtime pods stateless.
   - Use SSE here because incident dashboards are mostly server-push; status updates still go through HTTP.
   - Drop or disconnect slow clients rather than letting one browser create backpressure.
   - Use sticky sessions only if your gateway keeps in-memory subscription state that must persist per client.

5. **Reliability**
   - JetStream publish acknowledgement protects the ingest path.
   - Worker acknowledgements happen only after successful DB persistence.
   - Idempotency keys and processed_events prevent duplicate side effects.
   - Use retries with bounded timeouts.

6. **Observability**
   - Expose `/metrics` and scrape it with Prometheus.
   - Add structured logs and traces in production.
   - Track RED metrics: rate, errors, duration.
   - Track queue depth and consumer lag.

7. **Security**
   - Replace demo tenant header with real auth/JWT.
   - Enforce tenant isolation server-side.
   - Use TLS everywhere.
   - Store secrets in a cloud secret manager.
   - Add rate limiting at gateway and API layers.

## Repository layout

```text
backend/          Go API, worker, realtime gateway
frontend/         React dashboard
k8s/              Kubernetes manifests and HPA examples
loadtest/         k6 scripts
.github/          CI workflow
```

## Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/healthz` | Liveness |
| GET | `/readyz` | Readiness |
| GET | `/metrics` | Minimal Prometheus-style metrics |
| POST | `/v1/incidents` | Async incident creation |
| GET | `/v1/incidents` | List latest incidents |
| GET | `/v1/incidents/{id}` | Get incident |
| PATCH | `/v1/incidents/{id}/status` | Async status update |
| GET | `/v1/events?tenant=demo` | Server-Sent Events stream |
```
