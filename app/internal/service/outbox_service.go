package service

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/fidaroffxx/webhook-relay/internal/integration"
	"github.com/fidaroffxx/webhook-relay/internal/repository"
	"github.com/sirupsen/logrus"
)

const (
	numWorkers   = 2
	kafkaTopic   = "webhook-relay"
	pollInterval = 500 * time.Millisecond
)

type outboxService struct {
	outboxRepository repository.OutboxRepository
	kafkaIntegration *integration.Kafka
}

type OutboxService interface {
	Run(ctx context.Context) error
}

func NewOutboxService(
	outboxRepository repository.OutboxRepository,
	kafka *integration.Kafka,
) OutboxService {
	return &outboxService{
		outboxRepository: outboxRepository,
		kafkaIntegration: kafka,
	}
}

func (o *outboxService) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	for i := range numWorkers {
		wg.Add(1)

		go func(workerIndex int) {
			for {
				logrus.Printf("Running worker %d", workerIndex)

				if ctx.Err() != nil {
					return
				}

				messages, err := o.outboxRepository.GetNew(ctx)
				if err != nil {
					log.Printf("Error getting new messages: %v. Worker %d", err, workerIndex)
					sleepOrExit(ctx, pollInterval)

					continue
				}

				if len(messages) == 0 {
					sleepOrExit(ctx, pollInterval)

					continue
				}

				for _, message := range messages {
					if ctx.Err() != nil {
						return
					}

					if err = o.kafkaIntegration.Publish(
						ctx,
						kafkaTopic,
						[]byte(message.ID),
						[]byte(message.EventID),
					); err != nil {
						log.Printf("worker %d: publish error: %v", workerIndex, err)

						if markError := o.outboxRepository.MarkError(ctx, message.ID); markError != nil {
							log.Printf("worker %d: MarkError error: %v", workerIndex, markError)

							continue
						}

						continue
					}

					if err = o.outboxRepository.MarkDone(ctx, message.ID); err != nil {
						log.Printf("worker %d: MarkDone error: %v", workerIndex, err)

						continue
					}

					log.Printf("Mark done: %v", message.ID)
				}
			}
		}(i)
	}

	<-ctx.Done()
	wg.Wait()

	return nil
}

func sleepOrExit(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
