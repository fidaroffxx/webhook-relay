package repository

import (
	"context"
	"database/sql"
	"errors"

	pkgerrors "github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/fidaroffxx/webhook-relay/internal/db"
	"github.com/fidaroffxx/webhook-relay/internal/model"
)

type subscriptionRepository struct {
	*db.DB
}

type SubscriptionRepository interface {
	Create(ctx context.Context, sub *model.Subscription) (int64, error)
	List(ctx context.Context) ([]*model.Subscription, error)
	Get(ctx context.Context, id int64) (*model.Subscription, error)
}

func NewSubscriptionRepository(db *db.DB) SubscriptionRepository {
	return &subscriptionRepository{
		db,
	}
}

func (r *subscriptionRepository) Create(
	ctx context.Context,
	sub *model.Subscription,
) (int64, error) {
	err := r.DB.QueryRowContext(
		ctx,
		"INSERT INTO subscriptions(name, target_url, active ) VALUES ( $1, $2, $3 ) returning id",
		sub.Name, sub.TargetUrl, sub.Active,
	).Scan(&sub.ID)
	if err != nil {
		return 0, pkgerrors.Errorf("failed to insert subscription: %v", err)
	}

	return sub.ID, nil
}

func (r *subscriptionRepository) List(ctx context.Context) ([]*model.Subscription, error) {
	rows, err := r.DB.QueryContext(
		ctx,
		"SELECT id, name, target_url, created_at FROM subscriptions where active = $1",
		true,
	)
	if err != nil {
		return nil, pkgerrors.Errorf("failed to list subscriptions: %v", err)
	}
	defer rows.Close()

	resultList := make([]*model.Subscription, 0)

	for rows.Next() {
		row := &model.Subscription{}

		if err = rows.Scan(&row.ID, &row.Name, &row.TargetUrl, &row.CreatedAt); err != nil {
			logrus.Errorf("failed to list subscriptions: %v", err)

			continue
		}

		resultList = append(resultList, row)
	}

	return resultList, nil
}

func (r *subscriptionRepository) Get(ctx context.Context, id int64) (*model.Subscription, error) {
	result := model.Subscription{}

	err := r.DB.QueryRowContext(
		ctx,
		"SELECT id, name, target_url, active FROM subscriptions WHERE id = $1 and active = true",
		id,
	).
		Scan(
			&result.ID,
			&result.Name,
			&result.TargetUrl,
			&result.Active,
		)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, pkgerrors.Errorf("invalid subscription was sent %d", id)
	}

	if err != nil {
		return nil, pkgerrors.Errorf("failed to get subscription with id %d: %v", id, err)
	}

	return &result, nil
}
