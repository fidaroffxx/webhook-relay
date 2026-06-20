package service

import (
	"github.com/fidaroffxx/webhook-relay/internal/repository"
)

type Collection struct {
	statusService StatusService
}

func NewServiceCollection(repositories *repository.Collection) *Collection {
	return &Collection{
		statusService: NewStatusService(repositories.GetStatusRepository()),
	}
}

func (c *Collection) GetStatusService() StatusService {
	return c.statusService
}
