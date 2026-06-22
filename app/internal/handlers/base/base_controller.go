package base

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/sirupsen/logrus"
)

type Controller struct {
}

func NewBaseController() *Controller {
	return &Controller{}
}

func (b *Controller) JSON(w http.ResponseWriter, data any, statusCode int) {
	w.Header().Add("Content-Type", "application/json")
	w.Header().Add("User-Agent", "webhook-relay/1.0")
	w.WriteHeader(statusCode)

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		logrus.Printf("Error marshalling data: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	_, err = w.Write(jsonBytes)
	if err != nil {
		logrus.Printf("Error writing response: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}
}

func (b *Controller) ERROR(w http.ResponseWriter, r *http.Request, err error) {
	logrus.WithFields(logrus.Fields{
		"trace_id": middleware.GetReqID(r.Context()),
		"method":   r.Method,
		"path":     r.URL.Path,
		"layer":    "handler",
	}).Errorf("%+v", err)

	statusCode := b.parseError(err)

	w.Header().Add("Content-Type", "application/json")
	w.Header().Add("X-Trace-ID", middleware.GetReqID(r.Context()))
	w.Header().Add("User-Agent", "webhook-relay/1.0")

	w.WriteHeader(statusCode)

	body, err := json.Marshal(Error{
		Code:    statusCode,
		Message: err.Error(),
		TraceID: middleware.GetReqID(r.Context()),
	})
	if err != nil {
		logrus.Printf("Error marshalling data: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	_, err = w.Write(body)
	if err != nil {
		logrus.Printf("Error writing response: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}
}

func (b *Controller) parseError(err error) int {
	if err == nil {
		return 0
	}

	if errors.Is(err, sql.ErrNoRows) {
		return http.StatusNotFound
	}

	return http.StatusBadRequest
}

func (b *Controller) STATUS(w http.ResponseWriter, statusCode int) {
	w.Header().Add("Content-Type", "application/json")
	w.Header().Add("User-Agent", "webhook-relay/1.0")
	w.WriteHeader(statusCode)
}
