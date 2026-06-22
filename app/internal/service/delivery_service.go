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
	topicName              = "webhook-relay"
	doneStatus             = "done"
	retryStatus            = "retry"
	errorStatus            = "error"
	maxDeliveryAttempts    = 5
	deliveryRequestTimeout = 5 * time.Second
)

type RequestResult struct {
	response *http.Response
	duration time.Duration
	err      error
}

type deliveryService struct {
	processedEventsRepository repository.ProcessedEventsRepository
	eventsRepository          repository.EventsRepository
	subscriptionRepository    repository.SubscriptionRepository
	deliveriesRepository      repository.DeliveriesRepository

	kafkaIntegration integration.KafkaIntegration
}

type DeliveryService interface {
	Run(ctx context.Context) error
}

func NewDeliveryService(
	processedEventsRepository repository.ProcessedEventsRepository,
	eventsRepository repository.EventsRepository,
	subscriptionRepository repository.SubscriptionRepository,
	deliveriesRepository repository.DeliveriesRepository,
	kafkaIntegration integration.KafkaIntegration,
) DeliveryService {
	return &deliveryService{
		processedEventsRepository: processedEventsRepository,
		eventsRepository:          eventsRepository,
		subscriptionRepository:    subscriptionRepository,
		deliveriesRepository:      deliveriesRepository,
		kafkaIntegration:          kafkaIntegration,
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

				processed, err := d.processedEventsRepository.GetOrCreate(ctx, eventId, topicName)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					continue
				}
				if processed == nil {
					var done bool
					done, err = d.processedEventsRepository.IsProcessed(ctx, topicName, eventId)
					if err != nil {
						continue
					}
					if done {
						_ = r.CommitMessages(ctx, kafkaMessage)
					}

					continue
				}

				event, err := d.eventsRepository.Get(ctx, eventId)
				if errors.Is(err, sql.ErrNoRows) {
					_ = d.processedEventsRepository.MarkDone(ctx, topicName, eventId)
					_ = r.CommitMessages(ctx, kafkaMessage)

					continue
				}
				if err != nil {
					logrus.Errorf("Failed to fetch event: %v", err)

					continue
				}

				requestResult := d.DoRequest(ctx, event)
				httpErr := d.requestError(requestResult)

				status, writeErr := d.writeRequestResult(
					ctx,
					requestResult.response,
					requestResult.duration,
					eventId,
					httpErr,
				)
				if writeErr != nil {
					logrus.Errorf("Failed to write delivery result for event %s: %v", eventId, writeErr)

					continue
				}

				switch status {
				case doneStatus:
					if err = d.markDone(ctx, event.ID); err != nil {
						logrus.Errorf("Failed to mark event done: %v", err)
						continue
					}

					if err = r.CommitMessages(ctx, kafkaMessage); err != nil {
						logrus.Errorf("Failed to commit kafka message: %v", err)
						continue
					}

					logrus.Infof("Delivered event %s", event.ID)
				case retryStatus:
					if err = d.processedEventsRepository.Republish(ctx, topicName, eventId); err != nil {
						logrus.Errorf("Failed to republish event: %v", err)
					}

					if err = d.kafkaIntegration.Publish(
						ctx,
						topicName,
						[]byte(event.ID),
						[]byte(event.ID),
					); err != nil {
						logrus.Errorf("Failed to publish delivery result for event %s: %v", eventId, err)
					}

					if err = r.CommitMessages(ctx, kafkaMessage); err != nil {
						logrus.Errorf("Failed to commit kafka message: %v", err)
						continue
					}

				case errorStatus:
					if err = d.eventsRepository.MarkFailed(ctx, event.ID); err != nil {
						logrus.Errorf("Failed to mark event failed: %v", err)

						continue
					}

					if err = d.processedEventsRepository.MarkDone(ctx, topicName, eventId); err != nil {
						logrus.Errorf("Failed to mark processed event done: %v", err)

						continue
					}

					if err = r.CommitMessages(ctx, kafkaMessage); err != nil {
						logrus.Errorf("Failed to commit kafka message: %v", err)

						continue
					}

					logrus.Warnf("Delivery failed permanently for event %s", event.ID)
				}
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

func (d *deliveryService) DoRequest(ctx context.Context, event *model.Event) RequestResult {
	client := &http.Client{Timeout: deliveryRequestTimeout}

	subscriberURL, err := d.getSubscribe(ctx, event.SubscriptionID)
	if err != nil {
		return RequestResult{err: err}
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		subscriberURL,
		bytes.NewBuffer(event.Payload),
	)
	if err != nil {
		return RequestResult{err: err}
	}

	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := client.Do(req)
	duration := time.Since(start)

	if err != nil {
		return RequestResult{err: err}
	}

	return RequestResult{
		response: resp,
		duration: duration,
	}
}

func (d *deliveryService) requestError(result RequestResult) error {
	if result.err != nil {
		return result.err
	}

	if result.response == nil {
		return nil
	}

	if result.response.StatusCode >= 200 && result.response.StatusCode <= 299 {
		return nil
	}

	return fmt.Errorf("unexpected status code: %d", result.response.StatusCode)
}

func (d *deliveryService) writeRequestResult(
	ctx context.Context,
	resp *http.Response,
	duration time.Duration,
	eventId string,
	httpErr error,
) (string, error) {
	defer d.closeResponse(resp)

	delivery, err := d.deliveriesRepository.Get(ctx, eventId)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}

	if errors.Is(err, sql.ErrNoRows) {
		delivery, err = d.createDelivery(ctx, resp, eventId, duration, httpErr)
		if err != nil {
			return "", err
		}

		return delivery.Status, nil
	}

	if delivery.Status == errorStatus {
		return delivery.Status, nil
	}

	delivery.Attempts++
	delivery.Status = d.getStatus(resp, delivery.Attempts)
	delivery.DurationMs = duration.Milliseconds()
	delivery.Err = d.checkErr(httpErr)

	if err = d.deliveriesRepository.Update(ctx, delivery); err != nil {
		return "", err
	}

	return delivery.Status, nil
}

func (d *deliveryService) createDelivery(
	ctx context.Context,
	resp *http.Response,
	eventId string,
	duration time.Duration,
	httpErr error,
) (*model.Deliveries, error) {
	delivery := &model.Deliveries{
		DurationMs: duration.Milliseconds(),
		Attempts:   1,
		EventId:    eventId,
		Status:     d.getStatus(resp, 1),
		LogPath:    "",
		Err:        d.checkErr(httpErr),
	}

	if _, err := d.deliveriesRepository.Create(ctx, delivery); err != nil {
		return nil, err
	}

	return delivery, nil
}

func (d *deliveryService) closeResponse(resp *http.Response) {
	if resp == nil {
		return
	}

	_ = resp.Body.Close()
}

func (d *deliveryService) checkErr(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

func (d *deliveryService) getStatus(resp *http.Response, attempts int8) string {
	if resp == nil && attempts >= maxDeliveryAttempts {
		return errorStatus
	}

	if resp == nil {
		return retryStatus
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		if attempts >= maxDeliveryAttempts {
			return errorStatus
		}

		return retryStatus
	}

	return doneStatus
}

func (d *deliveryService) getSubscribe(ctx context.Context, subscribeId int64) (string, error) {
	result, err := d.subscriptionRepository.Get(ctx, subscribeId)
	if err != nil {
		return "", err
	}

	return result.TargetUrl, nil
}
