package subscriptions

import (
	"encoding/json"
	"net/http"

	"github.com/fidaroffxx/webhook-relay/internal/handlers/base"
	"github.com/fidaroffxx/webhook-relay/internal/handlers/subscriptions/dto"
	"github.com/fidaroffxx/webhook-relay/internal/middleware"
	"github.com/fidaroffxx/webhook-relay/internal/service"
	"github.com/go-chi/chi/v5"
)

type Controller struct {
	*base.Controller
	subscriptionService service.SubscriptionService
}

func NewController(
	baseController *base.Controller,
	subscriptionService service.SubscriptionService,
) *Controller {
	return &Controller{
		Controller:          baseController,
		subscriptionService: subscriptionService,
	}
}

func (c *Controller) Register(r chi.Router, m *middleware.Collection) {
	r.Post("/subscriptions", c.createSubscription)
	r.Get("/subscriptions", c.listSubscription)
}

func (c *Controller) createSubscription(w http.ResponseWriter, r *http.Request) {
	subDto := dto.NewCreateSubscriptionRequest()

	err := json.NewDecoder(r.Body).Decode(&subDto)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	err = subDto.Validate()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	id, err := c.subscriptionService.Create(r.Context(), subDto)
	if err != nil {
		c.ERROR(w, r, err)

		return
	}

	c.JSON(w, &dto.CreateSubscriptionResponse{
		ID: id,
	}, http.StatusOK)
}

func (c *Controller) listSubscription(w http.ResponseWriter, r *http.Request) {
	list, err := c.subscriptionService.List(r.Context())
	if err != nil {
		c.ERROR(w, r, err)

		return
	}

	c.JSON(w, list, http.StatusOK)
}
