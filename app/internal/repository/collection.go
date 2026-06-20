package repository

import (
	"github.com/fidaroffxx/webhook-relay/internal/db"
)

type Collection struct {
	statusRepository StatusRepository
}

func NewRepositoryCollection(db *db.DB) *Collection {
	return &Collection{
		statusRepository: NewStatusRepository(db),
	}
}

func (c *Collection) GetStatusRepository() StatusRepository {
	return c.statusRepository
}
