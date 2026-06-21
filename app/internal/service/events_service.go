package service

import (
	"context"

	"github.com/fidaroffxx/webhook-relay/internal/handlers/events/dto"
	"github.com/fidaroffxx/webhook-relay/internal/model"
	"github.com/fidaroffxx/webhook-relay/internal/repository"
	"github.com/pkg/errors"
)

type EventsService interface {
	Create(ctx context.Context, event *dto.CreateEventRequestDto) (string, error)
}

type eventsService struct {
	eventRepository        repository.EventsRepository
	subscriptionRepository repository.SubscriptionRepository
}

func NewEventsService(
	eventRepository repository.EventsRepository,
	subscriptionRepository repository.SubscriptionRepository,
) EventsService {
	return &eventsService{
		eventRepository:        eventRepository,
		subscriptionRepository: subscriptionRepository,
	}
}

func (s *eventsService) Create(ctx context.Context, event *dto.CreateEventRequestDto) (string, error) {
	if err := s.validateSubscription(ctx, event.SubscriptionID); err != nil {
		return "0", err
	}

	eventId, err := s.eventRepository.Create(ctx, &model.Event{
		SubscriptionID: event.SubscriptionID,
		EventType:      event.EventType,
		Payload:        event.Payload,
	})
	if err != nil {
		return "", err
	}

	return eventId, nil
}

func (s *eventsService) validateSubscription(ctx context.Context, subscriptionID int64) error {
	subscription, err := s.subscriptionRepository.Get(ctx, subscriptionID)
	if err != nil {
		return err
	}

	if !subscription.Active {
		return errors.New("subscription is not active")
	}

	return nil
}
