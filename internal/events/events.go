// Package events is the event plane's transport seam (DESIGN.md §4.4).
// Production uses Kafka via franz-go (kafka.go); tests use the in-memory
// Bus. Everything on this plane is lossy by contract: producers never block
// the hot path, and losing an event costs at most a missed optimization.
package events

import "context"

// Topic names, versioned per DESIGN.md §4.4.
const (
	TopicAccess    = "prefix.access.v1"
	TopicWarm      = "cache.warm.v1"
	TopicTelemetry = "cache.telemetry.v1"
)

// Producer publishes fire-and-forget. Implementations must be safe for
// concurrent use and must never block the caller on broker I/O.
type Producer interface {
	Produce(topic string, key, value []byte)
	Close()
}

// Handler processes one event. Errors are the handler's problem — the plane
// redelivers at-least-once at best, so handlers are idempotent by design.
type Handler func(key, value []byte)

// Consumer runs handlers against subscribed topics until ctx ends.
type Consumer interface {
	Run(ctx context.Context)
}
