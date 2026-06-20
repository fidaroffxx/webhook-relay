package service

import (
	"context"
	"github.com/fidaroffxx/webhook-relay/internal/model"
	"github.com/fidaroffxx/webhook-relay/internal/repository"
)

type StatusService interface {
	GetStatuses(ctx context.Context) ([]model.Status, error)
}

type statusService struct {
	statusRepository repository.StatusRepository
}

func (s *statusService) GetStatuses(ctx context.Context) ([]model.Status, error) {
	statuses, err := s.statusRepository.GetStatuses(ctx)
	if err != nil {
		return nil, err
	}

	return statuses, nil
}

func NewStatusService(statusRepository repository.StatusRepository) StatusService {
	return &statusService{
		statusRepository: statusRepository,
	}
}
