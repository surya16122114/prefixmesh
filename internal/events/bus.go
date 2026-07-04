package events

import (
	"context"
	"sync"
)

// Bus is the in-memory event plane used by tests: same at-least-once,
// unordered-across-keys semantics as the Kafka path, no broker.
type Bus struct {
	mu   sync.Mutex
	subs map[string][]chan msg
}

type msg struct{ key, value []byte }

func NewBus() *Bus {
	return &Bus{subs: map[string][]chan msg{}}
}

func (b *Bus) Produce(topic string, key, value []byte) {
	b.mu.Lock()
	chans := append([]chan msg(nil), b.subs[topic]...)
	b.mu.Unlock()
	for _, ch := range chans {
		select {
		case ch <- msg{key, value}:
		default: // lossy by contract, like acks=0
		}
	}
}

func (b *Bus) Close() {}

// Subscribe returns a Consumer delivering topic events to handler.
func (b *Bus) Subscribe(topic string, handler Handler) Consumer {
	ch := make(chan msg, 1024)
	b.mu.Lock()
	b.subs[topic] = append(b.subs[topic], ch)
	b.mu.Unlock()
	return &busConsumer{ch: ch, handler: handler}
}

type busConsumer struct {
	ch      chan msg
	handler Handler
}

func (c *busConsumer) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-c.ch:
			c.handler(m.key, m.value)
		}
	}
}
