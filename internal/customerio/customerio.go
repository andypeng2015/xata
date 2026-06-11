// Package customerio provides a client for sending transactional emails via the Customer.io API.
package customerio

//go:generate go run github.com/vektra/mockery/v3 --output mocks --outpkg mocks --with-expecter --name APIClientInterface

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/customerio/go-customerio/v3"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
)

type APIClientInterface interface {
	SendEmail(ctx context.Context, req *customerio.SendEmailRequest) (*customerio.SendEmailResponse, error)
}

type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

type Client struct {
	api           APIClientInterface
	httpClient    httpDoer
	appAPILimiter *rate.Limiter
	apiKey        string
	appAPIURL     string
	isProduction  bool
}

func NewClient(cfg Config) (*Client, error) {
	if cfg.CustomerIoAPIKey == "" {
		return nil, fmt.Errorf("CUSTOMER_IO_API_KEY is required")
	}

	apiClient := customerio.NewAPIClient(cfg.CustomerIoAPIKey, customerio.WithRegion(customerio.RegionEU))
	return &Client{
		api:           apiClient,
		httpClient:    http.DefaultClient,
		appAPILimiter: newAppAPILimiter(),
		apiKey:        cfg.CustomerIoAPIKey,
		appAPIURL:     "https://api-eu.customer.io/v1",
		isProduction:  cfg.CustomerIoIsProduction,
	}, nil
}

const (
	safeEmailRecipient = "testemails@xata.io"
	// Customer.io App API is limited to 10 req/s: https://docs.customer.io/integrations/api/app/
	appAPIMaxRetries       = uint64(3)
	appAPIRetryBaseDelay   = 250 * time.Millisecond
	appAPIRetryMaxDelay    = 3 * time.Second
	appAPIRateLimitSpacing = 250 * time.Millisecond
)

func (c *Client) safeEmail(toEmail string) string {
	if c.isProduction {
		return toEmail
	}

	if strings.HasSuffix(toEmail, "@xata.io") {
		return toEmail
	}

	return safeEmailRecipient
}

func (c *Client) HasRecentTransactionalMessage(ctx context.Context, email, triggerName string, since time.Time) (bool, error) {
	messageID, err := c.transactionalMessageID(ctx, triggerName)
	if err != nil {
		return false, err
	}

	messages, err := c.customerMessages(ctx, email, since, 100)
	if err != nil {
		return false, err
	}

	for _, message := range messages.Messages {
		if message.TransactionalMessageID == messageID {
			return true, nil
		}
	}
	return false, nil
}

func SendTransactionalEmail[T EmailMessageData](c *Client, ctx context.Context, to string, messageData T) error {
	messageDataMap, err := structToMap(messageData)
	if err != nil {
		return fmt.Errorf("failed to convert message data: %w", err)
	}

	safeToEmail := c.safeEmail(to)

	// customer.io documentation: https://docs.customer.io/journeys/transactional-email/#examples-and-api-parameters (click 'with template' and 'trigger name')
	request := customerio.SendEmailRequest{
		To:                     safeToEmail,
		TransactionalMessageID: messageData.TriggerName(),
		MessageData:            messageDataMap,
		// We always use email as the identifier at Xata
		Identifiers: map[string]string{
			"email": safeToEmail,
		},
	}

	log.Info().
		Str("original to", to).
		Bool("is_production", c.isProduction).
		Str("to (safe)", safeToEmail).
		Str("transactional_message_id", messageData.TriggerName()).
		Interface("message_data", messageDataMap).
		Msg("Sending Customer.io transactional email")

	_, err = c.api.SendEmail(ctx, &request)
	if err != nil {
		log.Error().
			Err(err).
			Str("transactional_message_id", messageData.TriggerName()).
			Msg("Failed to send Customer.io transactional email")
		return fmt.Errorf("failed to send Customer.io email: %w", err)
	}

	return nil
}

type transactionalMessagesResponse struct {
	Messages []transactionalMessage `json:"messages"`
}

type transactionalMessage struct {
	ID          int    `json:"id"`
	TriggerName string `json:"trigger_name"`
}

type customerMessagesResponse struct {
	Messages []customerMessage `json:"messages"`
}

type customerMessage struct {
	ID                     string `json:"id"`
	TransactionalMessageID int    `json:"transactional_message_id"`
}

func (c *Client) transactionalMessageID(ctx context.Context, triggerName string) (int, error) {
	var response transactionalMessagesResponse
	if err := c.getJSON(ctx, c.appAPIURL+"/transactional", &response); err != nil {
		return 0, fmt.Errorf("list transactional messages: %w", err)
	}

	for _, message := range response.Messages {
		if message.TriggerName == triggerName {
			return message.ID, nil
		}
	}

	return 0, fmt.Errorf("transactional message %q not found", triggerName)
}

func (c *Client) customerMessages(ctx context.Context, email string, since time.Time, limit int) (*customerMessagesResponse, error) {
	query := url.Values{}
	query.Set("id_type", "email")
	query.Set("limit", strconv.Itoa(limit))
	query.Set("start_ts", strconv.FormatInt(since.Unix(), 10))

	var response customerMessagesResponse
	requestURL := c.appAPIURL + "/customers/" + url.PathEscape(email) + "/messages?" + query.Encode()
	if err := c.getJSON(ctx, requestURL, &response); err != nil {
		// In case we have not yet created the customer.io "person" in customer.io yet
		if isAppAPIStatus(err, http.StatusNotFound) {
			return &customerMessagesResponse{}, nil
		}
		return nil, fmt.Errorf("list customer messages: %w", err)
	}
	return &response, nil
}

func (c *Client) getJSON(ctx context.Context, requestURL string, target any) error {
	bo := newAppAPIBackOff()
	op := func() error {
		err := c.getJSONOnce(ctx, requestURL, target)
		if err == nil {
			return nil
		}
		if !shouldRetryAppAPIRequest(err) {
			return backoff.Permanent(err)
		}
		bo.useRetryAfter(err, time.Now())
		return err
	}

	return backoff.RetryNotify(op, backoff.WithContext(backoff.WithMaxRetries(bo, appAPIMaxRetries), ctx), func(err error, d time.Duration) {
		log.Ctx(ctx).Warn().Err(err).Dur("retry_in", d).Msg("customer.io app api request failed; retrying")
	})
}

func (c *Client) getJSONOnce(ctx context.Context, requestURL string, target any) error {
	if err := c.waitAppAPILimiter(ctx); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	res, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return appAPIError{statusCode: res.StatusCode, body: string(body), retryAfter: res.Header.Get("Retry-After")}
	}
	if err := json.Unmarshal(body, target); err != nil {
		return err
	}

	return nil
}

type appAPIError struct {
	statusCode int
	body       string
	retryAfter string
}

func (e appAPIError) Error() string {
	return fmt.Sprintf("customer.io app api status %d: %s", e.statusCode, e.body)
}

func newAppAPILimiter() *rate.Limiter {
	return rate.NewLimiter(rate.Every(appAPIRateLimitSpacing), 1)
}

func (c *Client) waitAppAPILimiter(ctx context.Context) error {
	if c.appAPILimiter == nil {
		return nil
	}
	return c.appAPILimiter.Wait(ctx)
}

func shouldRetryAppAPIRequest(err error) bool {
	return isAppAPIStatus(err, http.StatusTooManyRequests) || isAppAPIServerError(err)
}

func isAppAPIStatus(err error, statusCode int) bool {
	var appErr appAPIError
	return errors.As(err, &appErr) && appErr.statusCode == statusCode
}

func isAppAPIServerError(err error) bool {
	var appErr appAPIError
	return errors.As(err, &appErr) && appErr.statusCode >= http.StatusInternalServerError
}

type appAPIBackOff struct {
	delegate   backoff.BackOff
	retryAfter *time.Duration
}

func newAppAPIBackOff() *appAPIBackOff {
	return &appAPIBackOff{
		delegate: backoff.NewExponentialBackOff(
			backoff.WithInitialInterval(appAPIRetryBaseDelay),
			backoff.WithMaxInterval(appAPIRetryMaxDelay),
		),
	}
}

func (b *appAPIBackOff) Reset() {
	b.retryAfter = nil
	b.delegate.Reset()
}

func (b *appAPIBackOff) NextBackOff() time.Duration {
	if b.retryAfter == nil {
		return b.delegate.NextBackOff()
	}
	delay := *b.retryAfter
	b.retryAfter = nil
	b.delegate.NextBackOff()
	return delay
}

func (b *appAPIBackOff) useRetryAfter(err error, now time.Time) {
	var appErr appAPIError
	if !errors.As(err, &appErr) {
		return
	}
	delay, ok := parseRetryAfter(appErr.retryAfter, now)
	if !ok {
		return
	}
	b.retryAfter = &delay
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	if value == "" {
		return 0, false
	}
	seconds, err := strconv.Atoi(value)
	if err == nil {
		return max(time.Duration(seconds)*time.Second, 0), true
	}
	retryAt, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	return max(retryAt.Sub(now).Round(time.Millisecond), 0), true
}

func structToMap(data any) (map[string]any, error) {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	var result map[string]any
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		return nil, err
	}

	return result, nil
}
