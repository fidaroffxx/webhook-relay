package handlers

import (
	projectMiddleware "github.com/fidaroffxx/webhook-relay/internal/middleware"
	"github.com/fidaroffxx/webhook-relay/internal/service"

	"github.com/go-chi/chi/v5"
)

type Collection struct {
	StatusController *Controller
}

func NewCollection(collection *service.Collection) *Collection {
	return &Collection{
		StatusController: NewController(collection.GetStatusService()),
	}
}

type RegisterController interface {
	Register(r chi.Router, m *projectMiddleware.Collection)
}
