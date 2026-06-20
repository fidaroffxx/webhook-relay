package handlers

import (
	"encoding/json"
	"github.com/fidaroffxx/webhook-relay/internal/middleware"
	"github.com/fidaroffxx/webhook-relay/internal/service"
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Controller struct {
	statusService service.StatusService
}

func NewController(statusService service.StatusService) *Controller {
	return &Controller{
		statusService: statusService,
	}
}

func (c *Controller) Register(r chi.Router, m *middleware.Collection) {
	r.With(m.GetCanViewStatus().Can()).
		Get("/status", c.GetStatus)
}

func (c *Controller) GetStatus(w http.ResponseWriter, r *http.Request) {
	statuses, err := c.statusService.GetStatuses(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	if err = json.NewEncoder(w).Encode(statuses); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
