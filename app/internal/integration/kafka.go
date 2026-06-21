package integration

import (
	"context"
	"fmt"
	"strings"

	"github.com/fidaroffxx/webhook-relay/internal/config"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type kafkaIntegration struct {
	kafkaWriters map[string]*kafka.Writer
	brokers      []string
}

type KafkaIntegration interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
	NewReader(topic string) *kafka.Reader
	Close()
}

func NewKafka(config *config.Kafka) KafkaIntegration {
	addr := fmt.Sprintf("%s:%s", config.Host, config.Port)
	topics := strings.Split(config.Topics, ",")

	conn, err := kafka.Dial(config.ConnectionType, addr)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Kafka: %v", err))
	}
	defer conn.Close()

	logrus.Infof(
		"Connected to Kafka. Broker %s:%d",
		conn.Broker().Host,
		conn.Broker().Port,
	)

	brokers := []string{
		addr,
	}

	return &kafkaIntegration{
		kafkaWriters: createWriters(addr, topics),
		brokers:      brokers,
	}
}

func createWriters(addr string, topics []string) map[string]*kafka.Writer {
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

	return initializedWriters
}

func (k *kafkaIntegration) NewReader(topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: k.brokers,
		Topic:   topic,
		GroupID: fmt.Sprintf("%s-group", topic),
	})
}

func (k *kafkaIntegration) Close() {
	for _, writer := range k.kafkaWriters {
		if err := writer.Close(); err != nil {
			logrus.Errorf("Failed to close writer: %v", err)
		}
	}
}

func (k *kafkaIntegration) Publish(ctx context.Context, topic string, key, value []byte) error {
	v, ok := k.kafkaWriters[topic]
	if !ok {
		return fmt.Errorf("topic %s does not exist", topic)
	}

	return v.WriteMessages(ctx, kafka.Message{
		Key:   key,
		Value: value,
	})
}
