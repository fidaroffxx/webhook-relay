package repository

import (
	"context"

	"github.com/fidaroffxx/webhook-relay/internal/db"
	"github.com/fidaroffxx/webhook-relay/internal/model"
)

const (
	newStatus = "new"
)

type eventsRepository struct {
	*db.DB
}

type EventsRepository interface {
	Create(ctx context.Context, event *model.Event) (eventId string, err error)
	MarkDone(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id string) error
	Get(ctx context.Context, eventId string) (event *model.Event, err error)
}

func NewEventsRepository(db *db.DB) EventsRepository {
	return &eventsRepository{
		db,
	}
}

func (r *eventsRepository) MarkDone(ctx context.Context, id string) error {
	_, err := r.DB.ExecContext(
		ctx,
		"UPDATE events SET status = 'done', delivery_at = now() WHERE id = $1",
		id,
	)
	if err != nil {
		return err
	}

	return nil
}

func (r *eventsRepository) MarkFailed(ctx context.Context, id string) error {
	_, err := r.DB.ExecContext(
		ctx,
		"UPDATE events SET status = 'failed' WHERE id = $1",
		id,
	)
	if err != nil {
		return err
	}

	return nil
}

func (r *eventsRepository) Get(ctx context.Context, eventId string) (*model.Event, error) {
	result := model.Event{}

	err := r.DB.QueryRowContext(
		ctx,
		"SELECT id, subscriptions_id, event_type, payload, status, created_at, delivery_at "+
			"FROM events where id = $1 AND status IN ('new')",
		eventId,
	).Scan(
		&result.ID,
		&result.SubscriptionID,
		&result.EventType,
		&result.Payload,
		&result.Status,
		&result.CreatedAt,
		&result.DeliveredAt,
	)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func (r *eventsRepository) Create(
	ctx context.Context,
	event *model.Event,
) (eventId string, err error) {
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}

		if err != nil {
			_ = tx.Rollback()

			return
		}
		err = tx.Commit()
	}()

	err = tx.QueryRowContext(
		ctx,
		"INSERT INTO events (subscriptions_id, event_type, payload, status) values ($1, $2, $3, $4) returning id;",
		event.SubscriptionID,
		event.EventType,
		event.Payload,
		newStatus,
	).Scan(&event.ID)
	if err != nil {
		return "", err
	}

	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO outbox(event_id) VALUES ( $1 )",
		event.ID,
	)
	if err != nil {
		return "", err
	}

	return event.ID, nil
}
