package service

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/fidaroffxx/webhook-relay/internal/integration"
	"github.com/fidaroffxx/webhook-relay/internal/model"
	"github.com/fidaroffxx/webhook-relay/internal/repository"
	"github.com/sirupsen/logrus"
)

const (
	topicName  = "webhook-relay"
	doneStatus = "done"
)

type deliveryService struct {
	processedEventsRepository repository.ProcessedEventsRepository
	eventsRepository          repository.EventsRepository
	subscriptionRepository    repository.SubscriptionRepository

	kafkaIntegration integration.KafkaIntegration
}

type DeliveryService interface {
	Run(ctx context.Context) error
}

func NewDeliveryService(
	ProcessedEventsRepository repository.ProcessedEventsRepository,
	EventsRepository repository.EventsRepository,
	SubscriptionRepository repository.SubscriptionRepository,

	kafkaIntegration integration.KafkaIntegration,
) DeliveryService {
	return &deliveryService{
		processedEventsRepository: ProcessedEventsRepository,
		eventsRepository:          EventsRepository,
		subscriptionRepository:    SubscriptionRepository,

		kafkaIntegration: kafkaIntegration,
	}
}

func (d *deliveryService) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	for i := range 3 {
		wg.Add(1)

		go func(readerIndex int) {
			r := d.kafkaIntegration.NewReader(topicName)

			defer func() {
				wg.Done()
				r.Close()
			}()

			for {
				if ctx.Err() != nil {
					return
				}

				logrus.Printf("Running reader worker %d", readerIndex)

				kafkaMessage, err := r.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					logrus.Errorf("Failed to fetch message from kafka: %v", err)

					continue
				}

				eventId := string(kafkaMessage.Value)

				eventId, err = d.processedEventsRepository.Create(ctx, eventId, topicName)
				if errors.Is(err, sql.ErrNoRows) || eventId == "" {
					_ = r.CommitMessages(ctx, kafkaMessage)

					continue
				}

				if err != nil {
					continue
				}

				event, err := d.eventsRepository.Get(ctx, eventId)
				if errors.Is(err, sql.ErrNoRows) {
					_ = r.CommitMessages(ctx, kafkaMessage)

					continue
				}

				if err != nil {
					logrus.Errorf("Failed to fetch event from kafka: %v", err)

					continue
				}

				if event.Status == doneStatus {
					if err = r.CommitMessages(ctx, kafkaMessage); err != nil {
						continue
					}

					continue
				}

				if err = d.DoRequest(ctx, event); err != nil {
					logrus.Errorf("Failed to do request: %v", err)

					continue
				}

				if err = d.markDone(ctx, event.ID); err != nil {
					logrus.Errorf("Failed to mark done for event: %v", err)

					continue
				}

				if err = r.CommitMessages(ctx, kafkaMessage); err != nil {
					logrus.Errorf("Failed to fetch event from kafka: %v", err)

					continue
				}

				logrus.Infof("Received event from kafka: %v", event.ID)

			}
		}(i)
	}

	<-ctx.Done()
	wg.Wait()

	return nil
}

func (d *deliveryService) markDone(ctx context.Context, eventId string) error {
	if err := d.eventsRepository.MarkDone(ctx, eventId); err != nil {
		return err
	}

	if err := d.processedEventsRepository.MarkDone(ctx, topicName, eventId); err != nil {
		return err
	}

	return nil
}

func (d *deliveryService) DoRequest(ctx context.Context, event *model.Event) error {
	requestCtx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	subscriberUrl, err := d.getSubscribe(ctx, event.SubscriptionID)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		subscriberUrl,
		bytes.NewBuffer(event.Payload),
	)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	result, err := http.DefaultClient.Do(req)
	duration := time.Since(start)

	if err != nil {
		return err
	}
	defer result.Body.Close()

	if result.StatusCode < 200 || result.StatusCode >= 300 {
		return fmt.Errorf("invalid status code: %d", result.StatusCode)
	}

	logrus.Infof("status: %s. duration %v", result.Status, duration)

	return nil
}

func (d *deliveryService) getSubscribe(ctx context.Context, subscribeId int64) (string, error) {
	result, err := d.subscriptionRepository.Get(ctx, subscribeId)
	if err != nil {
		return "", err
	}

	return result.TargetUrl, nil
}
