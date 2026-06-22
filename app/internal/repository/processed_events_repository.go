package repository

import (
	"context"
	"database/sql"
	"errors"

	"github.com/fidaroffxx/webhook-relay/internal/db"
	"github.com/fidaroffxx/webhook-relay/internal/model"
)

type processedEventsRepository struct {
	*db.DB
}

type ProcessedEventsRepository interface {
	IsProcessed(ctx context.Context, topic, eventId string) (bool, error)
	MarkDone(ctx context.Context, topic, eventId string) error
	Republish(ctx context.Context, topic, eventId string) error
	GetOrCreate(ctx context.Context, eventId, topic string) (*model.ProcessedEvents, error)
}

func NewProcessedEventsRepository(db *db.DB) ProcessedEventsRepository {
	return &processedEventsRepository{
		db,
	}
}

func (r *processedEventsRepository) GetOrCreate(ctx context.Context, eventId, topic string) (*model.ProcessedEvents, error) {
	data := model.ProcessedEvents{}

	const queryGetOrCreate = `
		INSERT INTO processed_events (event_id, topic, locked_processed_at)
		VALUES ($1, $2, now() + interval '1 minute')
		ON CONFLICT (topic, event_id) DO UPDATE
		SET locked_processed_at = EXCLUDED.locked_processed_at
		WHERE processed_events.processed_at IS NULL
		  AND (
				processed_events.locked_processed_at IS NULL
			 OR processed_events.locked_processed_at < now()
			  )
		RETURNING event_id, processed_at`

	err := r.DB.QueryRowContext(
		ctx,
		queryGetOrCreate,
		eventId,
		topic,
	).Scan(
		&data.EventID,
		&data.ProcessedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	return &data, nil
}

func (r *processedEventsRepository) IsProcessed(ctx context.Context, topic, eventId string) (bool, error) {
	var processedAt sql.NullTime
	err := r.DB.QueryRowContext(
		ctx,
		`SELECT processed_at FROM processed_events
		 WHERE event_id = $1 AND topic = $2 AND processed_at IS NOT NULL`,
		eventId,
		topic,
	).Scan(&processedAt)
	if errors.Is(err, sql.ErrNoRows) {
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

func (r *processedEventsRepository) Republish(ctx context.Context, topic, eventId string) error {
	_, err := r.DB.ExecContext(
		ctx,
		"UPDATE processed_events SET locked_processed_at = null where event_id = $1 and topic = $2",
		eventId,
		topic,
	)
	if err != nil {
		return err
	}

	return nil
}
