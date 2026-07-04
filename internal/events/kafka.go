package events

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaProducer publishes with acks=0 semantics: the hot path hands the
// record to franz-go's buffer and moves on. Broker loss drops telemetry,
// never requests (DESIGN.md §4.1).
type KafkaProducer struct {
	cl *kgo.Client
}

func NewKafkaProducer(brokers []string) (*KafkaProducer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.NoAck()),
		kgo.DisableIdempotentWrite(), // required for acks=0
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{cl: cl}, nil
}

func (p *KafkaProducer) Produce(topic string, key, value []byte) {
	p.cl.Produce(context.Background(), &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}, func(_ *kgo.Record, err error) {
		if err != nil {
			// Expected during broker outages; the plane is lossy by contract.
			slog.Debug("events: produce dropped", "topic", topic, "err", err)
		}
	})
}

func (p *KafkaProducer) Close() { p.cl.Close() }

// KafkaConsumer runs a handler over one topic within a consumer group.
type KafkaConsumer struct {
	cl      *kgo.Client
	handler Handler
}

func NewKafkaConsumer(brokers []string, group, topic string, handler Handler) (*KafkaConsumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaConsumer{cl: cl, handler: handler}, nil
}

func (c *KafkaConsumer) Run(ctx context.Context) {
	defer c.cl.Close()
	for ctx.Err() == nil {
		fetches := c.cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, p int32, err error) {
			slog.Warn("events: fetch error", "topic", topic, "partition", p, "err", err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			c.handler(r.Key, r.Value)
		})
	}
}
