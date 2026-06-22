package model

import "database/sql"

type ProcessedEvents struct {
	EventID           string       `json:"event_id"`
	Topic             string       `json:"topic"`
	ProcessedAt       sql.NullTime `json:"processed_at"`
	LockedProcessedAt sql.NullTime `json:"locked_processed_at"`
}
