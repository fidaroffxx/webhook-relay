package service

import (
	"context"

	"github.com/fidaroffxx/webhook-relay/internal/handlers/subscriptions/dto"
	"github.com/fidaroffxx/webhook-relay/internal/model"
	"github.com/fidaroffxx/webhook-relay/internal/repository"
)

type SubscriptionService interface {
	Create(ctx context.Context, sub *dto.CreateSubscriptionRequest) (int64, error)
	List(ctx context.Context) ([]*dto.CreateSubscriptionResponse, error)
}

type subscriptionService struct {
	subscriptionRepository repository.SubscriptionRepository
}

func NewSubscriptionService(
	subscriptionRepository repository.SubscriptionRepository,
) SubscriptionService {
	return &subscriptionService{
		subscriptionRepository: subscriptionRepository,
	}
}

func (s *subscriptionService) Create(ctx context.Context, sub *dto.CreateSubscriptionRequest) (int64, error) {
	return s.subscriptionRepository.Create(ctx, &model.Subscription{
		Name:      sub.Name,
		TargetUrl: sub.TargetUrl,
		Active:    true,
	})
}

func (s *subscriptionService) List(ctx context.Context) ([]*dto.CreateSubscriptionResponse, error) {
	list, err := s.subscriptionRepository.List(ctx)
	if err != nil {
		return nil, err
	}

	var result []*dto.CreateSubscriptionResponse

	for i := range list {
		result = append(result, &dto.CreateSubscriptionResponse{
			ID:        list[i].ID,
			Name:      &list[i].Name,
			TargetUrl: &list[i].TargetUrl,
			CreatedAt: &list[i].CreatedAt,
		})
	}
	return result, nil
}
