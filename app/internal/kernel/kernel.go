package kernel

import (
	"context"
	"errors"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/fidaroffxx/webhook-relay/internal/config"
	"github.com/fidaroffxx/webhook-relay/internal/db"
	"github.com/fidaroffxx/webhook-relay/internal/handlers"
	"github.com/fidaroffxx/webhook-relay/internal/integration"
	"github.com/fidaroffxx/webhook-relay/internal/middleware"
	"github.com/fidaroffxx/webhook-relay/internal/repository"
	"github.com/fidaroffxx/webhook-relay/internal/server"
	"github.com/fidaroffxx/webhook-relay/internal/service"
	"github.com/sirupsen/logrus"

	"github.com/go-chi/chi/v5"
)

type Kernel struct {
	configs     *config.Config
	db          *db.DB
	repos       *repository.Collection
	logger      *logrus.Logger
	services    *service.Collection
	handlers    *handlers.Collection
	middlewares *middleware.Collection
	integration *integration.Collection
	router      *chi.Mux
	serve       *http.Server
}

func NewKernel() *Kernel {
	return &Kernel{}
}

func (k *Kernel) GetDB() *db.DB {
	return k.db
}

func (k *Kernel) Load() error {
	k.configs = config.NewConfig()

	if err := k.configs.Load(); err != nil {
		panic(err)
	}

	k.db = db.NewDB(k.configs.DB)
	k.integration = integration.NewCollection(k.configs)

	k.repos = repository.NewRepositoryCollection(k.db)
	k.services = service.NewServiceCollection(k.repos, k.integration)
	k.handlers = handlers.NewCollection(k.services)
	k.middlewares = middleware.NewCollection()
	k.router = server.NewRouter(k.handlers, k.middlewares)
	k.serve = server.NewServer(k.configs.HTTP, k.router)

	return nil
}
func (k *Kernel) Serve() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logrus.Println("starting server")
		if err := k.serve.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logrus.Fatal(err)
		}
	}()

	go func() {
		logrus.Println("out box workers started")
		if err := k.services.GetOutboxService().Run(ctx); err != nil {
			logrus.Fatal(err)
		}
	}()

	go func() {
		logrus.Println("delivery workers started")
		if err := k.services.GetDeliveryService().Run(ctx); err != nil {
			logrus.Fatal(err)
		}
	}()

	<-ctx.Done()

	logrus.Println("Received termination signal, shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	k.serve.RegisterOnShutdown(func() {
		err := k.db.DB.Close()
		if err != nil {
			logrus.Printf("Error closing database connection: %v", err)
		}

		k.integration.GetKafka().Close()
	})

	err := k.serve.Shutdown(shutdownCtx)
	if err != nil {
		logrus.Printf("Error shutting down server: %v", err)
	}

}
