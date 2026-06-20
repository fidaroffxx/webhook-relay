package server

import (
	"fmt"
	"github.com/fidaroffxx/webhook-relay/internal/handlers"
	"reflect"

	projectMiddleware "github.com/fidaroffxx/webhook-relay/internal/middleware"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func NewRouter(h *handlers.Collection, m *projectMiddleware.Collection) *chi.Mux {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	v := reflect.ValueOf(h)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}

	t := v.Type()

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)

		if !fieldType.IsExported() {
			continue
		}

		if field.Kind() == reflect.Pointer && field.IsNil() {
			continue
		}

		ctrl, ok := field.Interface().(handlers.RegisterController)
		if !ok {
			panic(fmt.Sprintf("field %s does not implement handlers.Controller", fieldType.Name))
		}

		ctrl.Register(r, m)
	}

	return r
}
