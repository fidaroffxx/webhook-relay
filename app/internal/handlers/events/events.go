package events

import (
	"encoding/json"
	"net/http"

	"github.com/fidaroffxx/webhook-relay/internal/handlers/base"
	"github.com/fidaroffxx/webhook-relay/internal/handlers/events/dto"
	"github.com/fidaroffxx/webhook-relay/internal/middleware"
	"github.com/fidaroffxx/webhook-relay/internal/service"
	"github.com/go-chi/chi/v5"
)

type Controller struct {
	*base.Controller
	service service.EventsService
}

func NewController(
	controller *base.Controller,
	service service.EventsService,
) *Controller {
	return &Controller{
		controller,
		service,
	}
}

func (c *Controller) Register(r chi.Router, m *middleware.Collection) {
	r.Post("/events", c.createEvent)
}

func (c *Controller) createEvent(w http.ResponseWriter, r *http.Request) {
	eventDto := dto.NewCreateEventRequestDto()

	err := json.NewDecoder(r.Body).Decode(&eventDto)
	if err != nil {
		c.ERROR(w, r, err)

		return
	}

	if err = eventDto.Validate(); err != nil {
		c.ERROR(w, r, err)

		return
	}

	if err = c.service.Create(r.Context(), eventDto); err != nil {
		c.ERROR(w, r, err)

		return
	}
}
