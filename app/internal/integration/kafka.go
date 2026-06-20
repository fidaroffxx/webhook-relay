package integration

import (
	"fmt"

	"github.com/fidaroffxx/webhook-relay/internal/config"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type Kafka struct {
	kafkaConn *kafka.Conn
}

func NewKafka(config *config.Kafka) *Kafka {
	conn, err := kafka.Dial(config.ConnectionType, fmt.Sprintf("%s:%s", config.Host, config.Port))
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to Kafka: %v", err))
	}

	logrus.Infof(
		"Connected to Kafka. Broker %s:%d",
		conn.Broker().Host,
		conn.Broker().Port,
	)

	return &Kafka{
		kafkaConn: conn,
	}
}

func (k *Kafka) Close() error {
	return k.kafkaConn.Close()
}
