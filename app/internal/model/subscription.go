package model

import "time"

type Subscription struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	TargetUrl string    `json:"target_url"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
}
