package handlers

import (
	"github.com/fidaroffxx/webhook-relay/internal/handlers/base"
	"github.com/fidaroffxx/webhook-relay/internal/handlers/events"
	"github.com/fidaroffxx/webhook-relay/internal/handlers/subscriptions"
	projectMiddleware "github.com/fidaroffxx/webhook-relay/internal/middleware"
	"github.com/fidaroffxx/webhook-relay/internal/service"

	"github.com/go-chi/chi/v5"
)

type Collection struct {
	EventsController       *events.Controller
	SubscriptionController *subscriptions.Controller
}

func NewCollection(collection *service.Collection) *Collection {
	baseController := base.NewBaseController()

	return &Collection{
		EventsController: events.NewController(
			baseController,
			collection.GetEventsService(),
		),
		SubscriptionController: subscriptions.NewController(
			baseController,
			collection.GetSubscriptionService(),
		),
	}
}

type RegisterController interface {
	Register(r chi.Router, m *projectMiddleware.Collection)
}
