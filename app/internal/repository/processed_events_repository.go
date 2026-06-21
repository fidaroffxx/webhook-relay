package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/fidaroffxx/webhook-relay/internal/db"
)

type processedEventsRepository struct {
	*db.DB
}

type ProcessedEventsRepository interface {
	IsDone(ctx context.Context, topic, eventId string) (bool, error)
	MarkDone(ctx context.Context, topic, eventId string) error
	Create(ctx context.Context, eventId, topic string) (string, error)
}

func NewProcessedEventsRepository(db *db.DB) ProcessedEventsRepository {
	return &processedEventsRepository{
		db,
	}
}

func (r *processedEventsRepository) Create(ctx context.Context, eventId, topic string) (string, error) {
	var processingEventId string

	err := r.DB.QueryRowContext(
		ctx,
		"INSERT INTO processed_events(event_id, topic) VALUES ($1, $2) ON CONFLICT (topic, event_id) DO NOTHING RETURNING event_id",
		eventId,
		topic,
	).Scan(&processingEventId)
	if errors.Is(err, sql.ErrNoRows) {
		return "", err
	}

	return processingEventId, nil
}

func (r *processedEventsRepository) IsDone(ctx context.Context, topic, eventId string) (bool, error) {
	var processedAt sql.NullTime

	err := r.DB.QueryRowContext(
		ctx,
		"SELECT processed_at from processed_events where event_id = $1 and topic = $2",
		eventId,
		topic,
	).Scan(&processedAt)
	if err != nil && errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (r *processedEventsRepository) MarkDone(ctx context.Context, topic, eventId string) error {
	_, err := r.DB.ExecContext(
		ctx,
		"UPDATE processed_events SET processed_at = now() where event_id = $1 and topic = $2",
		eventId,
		topic,
	)
	if err != nil {
		return err
	}

	return nil
}
