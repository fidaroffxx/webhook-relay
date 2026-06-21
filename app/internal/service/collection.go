package service

import (
	"github.com/fidaroffxx/webhook-relay/internal/integration"
	"github.com/fidaroffxx/webhook-relay/internal/repository"
)

type Collection struct {
	eventsService       EventsService
	subscriptionService SubscriptionService
	outboxService       OutboxService
	deliveryService     DeliveryService
}

func NewServiceCollection(
	repositories *repository.Collection,
	integration *integration.Collection,
) *Collection {
	return &Collection{
		eventsService: NewEventsService(
			repositories.GetEventsRepository(),
			repositories.GetSubscriptionRepository(),
		),

		subscriptionService: NewSubscriptionService(
			repositories.GetSubscriptionRepository(),
		),

		outboxService: NewOutboxService(
			repositories.GetOutboxRepository(),
			integration.GetKafka(),
		),

		deliveryService: NewDeliveryService(
			repositories.GetProcessedEventsRepository(),
			repositories.GetEventsRepository(),
			repositories.GetSubscriptionRepository(),
			integration.GetKafka(),
		),
	}
}

func (c *Collection) GetEventsService() EventsService {
	return c.eventsService
}

func (c *Collection) GetSubscriptionService() SubscriptionService {
	return c.subscriptionService
}

func (c *Collection) GetOutboxService() OutboxService {
	return c.outboxService
}

func (c *Collection) GetDeliveryService() DeliveryService {
	return c.deliveryService
}
