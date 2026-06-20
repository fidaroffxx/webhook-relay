package repository

import (
	"context"
	"github.com/fidaroffxx/webhook-relay/internal/db"
	"github.com/fidaroffxx/webhook-relay/internal/model"
)

type statusRepository struct {
	db *db.DB
}

type StatusRepository interface {
	GetStatuses(ctx context.Context) ([]model.Status, error)
}

func NewStatusRepository(db *db.DB) StatusRepository {
	return &statusRepository{
		db: db,
	}
}

func (r *statusRepository) GetStatuses(ctx context.Context) ([]model.Status, error) {
	rows, err := r.db.DB.QueryContext(ctx, "select * from statuses")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]model.Status, 0)
	for rows.Next() {
		var s model.Status
		if err = rows.Scan(&s.Id, &s.Name); err != nil {
			return nil, err
		}
		result = append(result, s)
	}

	return result, nil
}
