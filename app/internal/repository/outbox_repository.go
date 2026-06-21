package repository

import (
	"context"
	"database/sql"

	"github.com/fidaroffxx/webhook-relay/internal/db"
	"github.com/fidaroffxx/webhook-relay/internal/model"
)

type outboxRepository struct {
	*db.DB
}

type OutboxRepository interface {
	GetNew(ctx context.Context) ([]model.Outbox, error)
	MarkDone(ctx context.Context, id string) error
	MarkError(ctx context.Context, id string) error
}

func NewOutboxRepository(db *db.DB) OutboxRepository {
	return &outboxRepository{
		db,
	}
}

func (r *outboxRepository) GetNew(
	ctx context.Context,
) (results []model.Outbox, err error) {
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()

			return
		}

		err = tx.Commit()
	}()

	rows, err := tx.QueryContext(
		ctx,
		"SELECT id, event_id, locked_at, locked_until, status, created_at, publish_at FROM outbox where status IN ('new', 'in_progress') AND "+
			"(locked_until IS NULL OR locked_until < now()) FOR UPDATE SKIP LOCKED LIMIT 3",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results = make([]model.Outbox, 0)

	for rows.Next() {
		var result model.Outbox

		if err = rows.Scan(
			&result.ID,
			&result.EventID,
			&result.LockedAt,
			&result.LockedUntil,
			&result.Status,
			&result.CreatedAt,
			&result.PublishAt,
		); err != nil {
			return nil, err
		}

		results = append(results, result)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	rows.Close()

	for i := range results {
		if err = r.setInProgress(ctx, tx, &results[i]); err != nil {
			return nil, err
		}
	}

	return results, nil
}

func (r *outboxRepository) setInProgress(ctx context.Context, tx *sql.Tx, jobRow *model.Outbox) error {
	err := tx.QueryRowContext(
		ctx,
		"UPDATE outbox SET status = 'in_progress', locked_at = now(), locked_until = now() + interval '1 minute' WHERE id = $1 "+
			"RETURNING status, locked_at, locked_until",
		jobRow.ID,
	).Scan(
		&jobRow.Status,
		&jobRow.LockedAt,
		&jobRow.LockedUntil,
	)
	if err != nil {
		return err
	}

	return nil
}

func (r *outboxRepository) MarkDone(ctx context.Context, id string) error {
	_, err := r.DB.ExecContext(ctx, "UPDATE outbox SET status = 'done', locked_at = null, publish_at = now() where id = $1", id)
	if err != nil {
		return err
	}

	return nil
}

func (r *outboxRepository) MarkError(ctx context.Context, id string) error {
	_, err := r.DB.ExecContext(ctx, "UPDATE outbox SET status = 'new', locked_at = null ,locked_until = null where id = $1", id)
	if err != nil {
		return err
	}

	return nil
}
