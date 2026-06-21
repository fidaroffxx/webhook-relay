package model

import (
	"encoding/json"
	"time"
)

type Event struct {
	ID             string          `json:"id"`
	SubscriptionID int64           `json:"subscription_id"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`

	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	DeliveredAt time.Time `json:"delivery_at"`
}
