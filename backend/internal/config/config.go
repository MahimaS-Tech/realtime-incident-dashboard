package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Role              string
	HTTPAddr          string
	DatabaseURL       string
	NatsURL           string
	NatsStream        string
	NatsReplicas      int
	CORSOrigin        string
	MaxBodyBytes      int64
	RequestTimeout    time.Duration
	MaxInFlight       int
	WorkerBatchSize   int
	WorkerFetchWait   time.Duration
	SSEClientBuffer   int
	PGMaxConns        int32
	PGMinConns        int32
	NatsMaxReconnects int
}

func Load() Config {
	return Config{
		Role:              getenv("ROLE", "all"),
		HTTPAddr:          getenv("HTTP_ADDR", ":8080"),
		DatabaseURL:       getenv("DATABASE_URL", "postgres://incident:incident@localhost:5432/incidentdb?sslmode=disable"),
		NatsURL:           getenv("NATS_URL", "nats://localhost:4222"),
		NatsStream:        getenv("NATS_STREAM", "INCIDENTS"),
		NatsReplicas:      getenvInt("NATS_REPLICAS", 1),
		CORSOrigin:        getenv("CORS_ORIGIN", "*"),
		MaxBodyBytes:      int64(getenvInt("MAX_BODY_BYTES", 1<<20)),
		RequestTimeout:    getenvDuration("REQUEST_TIMEOUT", 3*time.Second),
		MaxInFlight:       getenvInt("MAX_IN_FLIGHT", 10000),
		WorkerBatchSize:   getenvInt("WORKER_BATCH_SIZE", 500),
		WorkerFetchWait:   getenvDuration("WORKER_FETCH_WAIT", 750*time.Millisecond),
		SSEClientBuffer:   getenvInt("SSE_CLIENT_BUFFER", 512),
		PGMaxConns:        int32(getenvInt("PG_MAX_CONNS", 32)),
		PGMinConns:        int32(getenvInt("PG_MIN_CONNS", 2)),
		NatsMaxReconnects: getenvInt("NATS_MAX_RECONNECTS", -1),
	}
}

func getenv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	v := getenv(key, "")
	if v == "" {
		return fallback
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return i
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	v := getenv(key, "")
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
