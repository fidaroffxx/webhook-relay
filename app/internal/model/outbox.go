package model

import (
	"database/sql"
	"time"
)

type Outbox struct {
	ID      string `json:"id"`
	EventID string `json:"event_id"`
	Status  string `json:"status"`

	CreatedAt   time.Time    `json:"created_at"`
	LockedAt    sql.NullTime `json:"locked_at"`
	LockedUntil sql.NullTime `json:"locked_until"`
	PublishAt   sql.NullTime `json:"publish_at"`
}
