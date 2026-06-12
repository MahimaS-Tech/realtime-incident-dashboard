.PHONY: up down logs smoke load fmt

up:
	docker compose up --build

down:
	docker compose down -v

logs:
	docker compose logs -f api realtime worker

smoke:
	curl -sS -X POST http://localhost:8080/v1/incidents \
		-H 'Content-Type: application/json' \
		-H 'X-Tenant-ID: demo' \
		-H "Idempotency-Key: smoke-$$(date +%s)" \
		-d '{"title":"Smoke incident","description":"created by make smoke","severity":"SEV3","service":"demo-api","assignee":"oncall"}' | jq .
	curl -sS -H 'X-Tenant-ID: demo' http://localhost:8080/v1/incidents | jq .

load:
	k6 run loadtest/ingest.js

fmt:
	cd backend && gofmt -w ./cmd ./internal
