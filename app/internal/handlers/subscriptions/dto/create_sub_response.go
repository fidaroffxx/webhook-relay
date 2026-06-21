package dto

import "time"

type CreateSubscriptionResponse struct {
	ID        int64      `json:"id"`
	Name      *string    `json:"name,omitempty"`
	TargetUrl *string    `json:"targetUrl,omitempty"`
	CreatedAt *time.Time `json:"created_at,omitempty"`
}
