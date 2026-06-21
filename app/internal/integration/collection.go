package integration

import "github.com/fidaroffxx/webhook-relay/internal/config"

type Collection struct {
	kafka KafkaIntegration
}

func NewCollection(config *config.Config) *Collection {
	return &Collection{
		kafka: NewKafka(config.Kafka),
	}
}

func (c *Collection) GetKafka() KafkaIntegration {
	return c.kafka
}
