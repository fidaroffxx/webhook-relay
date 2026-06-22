package model

import "time"

type Deliveries struct {
	Id         string
	EventId    string
	Attempts   int8
	Status     string
	Err        string
	LogPath    string
	DurationMs int64
	CreatedAt  time.Time
}
