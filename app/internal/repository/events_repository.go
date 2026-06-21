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
	Create(ctx context.Context, event *model.Event) error
}

func NewEventsRepository(db *db.DB) EventsRepository {
	return &eventsRepository{
		db,
	}
}

func (r *eventsRepository) Create(
	ctx context.Context,
	event *model.Event,
) (err error) {
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
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
		return err
	}

	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO outbox(event_id) VALUES ( $1 )",
		event.ID,
	)
	if err != nil {
		return err
	}

	return nil
}
