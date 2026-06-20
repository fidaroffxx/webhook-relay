package server

import (
	"fmt"
	"github.com/fidaroffxx/webhook-relay/internal/config"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func NewServer(config *config.HTTPConfig, r chi.Router) *http.Server {
	return &http.Server{
		Addr:    fmt.Sprintf(":%s", config.Port),
		Handler: r,
	}
}
