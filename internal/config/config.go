// Package config holds environment-driven configuration shared by services.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Env reads a string env var with a default.
func Env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// EnvInt reads an int env var with a default.
func EnvInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// EnvDuration reads a duration env var with a default.
func EnvDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// EnvBool reads a boolean env var with a default.
func EnvBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

// KafkaBrokers returns the configured Kafka broker list.
func KafkaBrokers() []string {
	return strings.Split(Env("KAFKA_BROKERS", "localhost:9092"), ",")
}

// Topic names.
const (
	TopicSpans     = "spans"
	TopicSpansKept = "spans.kept"
	TopicSpansDLQ  = "spans.dlq"
)
