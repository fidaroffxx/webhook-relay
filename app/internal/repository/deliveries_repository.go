package repository

import (
	"context"

	"github.com/fidaroffxx/webhook-relay/internal/db"
	"github.com/fidaroffxx/webhook-relay/internal/model"
)

type deliveriesRepository struct {
	*db.DB
}

type DeliveriesRepository interface {
	Create(ctx context.Context, delivery *model.Deliveries) (deliveriesId string, err error)
	Update(ctx context.Context, delivery *model.Deliveries) error
	Get(ctx context.Context, eventId string) (*model.Deliveries, error)
}

func NewDeliveriesRepository(db *db.DB) DeliveriesRepository {
	return &deliveriesRepository{
		db,
	}
}

func (r *deliveriesRepository) Update(ctx context.Context, deliveries *model.Deliveries) error {
	_, err := r.DB.ExecContext(
		ctx,
		"UPDATE deliveries SET attempts = $1, status = $2, err = $3, log_path = $4, duration_ms = $5 WHERE id = $6",
		deliveries.Attempts,
		deliveries.Status,
		deliveries.Err,
		deliveries.LogPath,
		deliveries.DurationMs,
		deliveries.Id,
	)
	if err != nil {
		return err
	}

	return nil
}

func (r *deliveriesRepository) Get(ctx context.Context, eventId string) (*model.Deliveries, error) {
	result := model.Deliveries{}

	err := r.DB.QueryRowContext(
		ctx,
		"SELECT id, event_id, attempts, status, err, log_path, duration_ms, created_at FROM deliveries WHERE event_id = $1",
		eventId,
	).Scan(
		&result.Id,
		&result.EventId,
		&result.Attempts,
		&result.Status,
		&result.Err,
		&result.LogPath,
		&result.DurationMs,
		&result.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func (r *deliveriesRepository) Create(
	ctx context.Context,
	delivery *model.Deliveries,
) (deliveriesId string, err error) {
	err = r.DB.QueryRowContext(
		ctx,
		"INSERT INTO deliveries (event_id, attempts, status, err, log_path, duration_ms) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
		delivery.EventId,
		delivery.Attempts,
		delivery.Status,
		delivery.Err,
		delivery.LogPath,
		delivery.DurationMs,
	).Scan(&delivery.Id)
	if err != nil {
		return "", err
	}

	return delivery.Id, nil
}
