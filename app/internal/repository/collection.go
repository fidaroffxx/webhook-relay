package repository

import (
	"github.com/fidaroffxx/webhook-relay/internal/db"
)

type Collection struct {
	subscriptionRepository    SubscriptionRepository
	eventsRepository          EventsRepository
	outboxRepository          OutboxRepository
	processedEventsRepository ProcessedEventsRepository
}

func NewRepositoryCollection(db *db.DB) *Collection {
	return &Collection{
		subscriptionRepository:    NewSubscriptionRepository(db),
		eventsRepository:          NewEventsRepository(db),
		outboxRepository:          NewOutboxRepository(db),
		processedEventsRepository: NewProcessedEventsRepository(db),
	}
}

func (c *Collection) GetSubscriptionRepository() SubscriptionRepository {
	return c.subscriptionRepository
}

func (c *Collection) GetEventsRepository() EventsRepository {
	return c.eventsRepository
}

func (c *Collection) GetOutboxRepository() OutboxRepository {
	return c.outboxRepository
}

func (c *Collection) GetProcessedEventsRepository() ProcessedEventsRepository {
	return c.processedEventsRepository
}
