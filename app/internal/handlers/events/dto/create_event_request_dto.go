package dto

import (
	"encoding/json"
	"errors"
)

type CreateEventRequestDto struct {
	SubscriptionID int64           `json:"subscription_id"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
}

func NewCreateEventRequestDto() *CreateEventRequestDto {
	return &CreateEventRequestDto{}
}

func (d *CreateEventRequestDto) Validate() error {
	if d.SubscriptionID == 0 {
		return errors.New("subscription_id is required")
	}

	if d.EventType == "" {
		return errors.New("event_type is required")
	}

	if d.Payload == nil {
		return errors.New("payload is required")
	}

	return nil
}
