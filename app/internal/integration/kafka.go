package integration

import (
	"context"
	"fmt"
	"strings"

	"github.com/fidaroffxx/webhook-relay/internal/config"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Kafka struct {
	kafkaWriters map[string]*kafka.Writer
}

func NewKafka(config *config.Kafka) *Kafka {
	addr := fmt.Sprintf("%s:%s", config.Host, config.Port)

	conn, err := kafka.Dial(config.ConnectionType, fmt.Sprintf("%s:%s", config.Host, config.Port))
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Kafka: %v", err))
	}
	defer conn.Close()

	logrus.Infof(
		"Connected to Kafka. Broker %s:%d",
		conn.Broker().Host,
		conn.Broker().Port,
	)

	topics := strings.Split(config.Topics, ",")
	initializedWriters := make(map[string]*kafka.Writer)

	for _, topic := range topics {
		topic = strings.TrimSpace(topic)
		if topic == "" {
			continue
		}

		writer := &kafka.Writer{
			Addr:     kafka.TCP(addr),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}

		initializedWriters[topic] = writer
	}

	return &Kafka{
		kafkaWriters: initializedWriters,
	}
}

func (k *Kafka) Close() {
	for _, writer := range k.kafkaWriters {
		if err := writer.Close(); err != nil {
			logrus.Errorf("Failed to close writer: %v", err)
		}
	}
}

func (k *Kafka) Publish(ctx context.Context, topic string, key, value []byte) error {
	v, ok := k.kafkaWriters[topic]
	if !ok {
		return fmt.Errorf("topic %s does not exist", topic)
	}

	return v.WriteMessages(ctx, kafka.Message{
		Value: value,
	})
}
