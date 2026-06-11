package customerio

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"xata/internal/customerio/mocks"

	"github.com/customerio/go-customerio/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSafeEmail(t *testing.T) {
	tests := []struct {
		name         string
		isProduction bool
		input        string
		want         string
	}{
		{
			name:         "external email in production",
			isProduction: true,
			input:        "user@example.com",
			want:         "user@example.com",
		},
		{
			name:         "xata.io email in production",
			isProduction: true,
			input:        "engineer@xata.io",
			want:         "engineer@xata.io",
		},
		{
			name:         "external email redirected to test email",
			isProduction: false,
			input:        "user@example.com",
			want:         "testemails@xata.io",
		},
		{
			name:         "xata.io email allowed in non-production",
			isProduction: false,
			input:        "engineer@xata.io",
			want:         "engineer@xata.io",
		},
		{
			name:         "another external email redirected",
			isProduction: false,
			input:        "customer@company.com",
			want:         "testemails@xata.io",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				isProduction: tt.isProduction,
			}
			result := client.safeEmail(tt.input)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestNewClient(t *testing.T) {
	tests := []struct {
		name             string
		cfg              Config
		wantErr          bool
		wantErrContains  string
		wantIsProduction bool
	}{
		{
			name: "requires API key",
			cfg: Config{
				CustomerIoAPIKey:       "",
				CustomerIoIsProduction: false,
			},
			wantErr:         true,
			wantErrContains: "CUSTOMER_IO_API_KEY is required",
		},
		{
			name: "success with non-production",
			cfg: Config{
				CustomerIoAPIKey:       "test-api-key",
				CustomerIoIsProduction: false,
			},
			wantErr:          false,
			wantIsProduction: false,
		},
		{
			name: "success with production",
			cfg: Config{
				CustomerIoAPIKey:       "test-api-key",
				CustomerIoIsProduction: true,
			},
			wantErr:          false,
			wantIsProduction: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(tt.cfg)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, client)
				if tt.wantErrContains != "" {
					assert.Contains(t, err.Error(), tt.wantErrContains)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)
				assert.Equal(t, tt.wantIsProduction, client.isProduction)
			}
		})
	}
}

func TestHasRecentTransactionalMessage(t *testing.T) {
	tests := map[string]struct {
		responses []*http.Response
		want      bool
		wantErr   string
	}{
		"GET /customers/{email}/messages 404 means no recent messages": {
			responses: []*http.Response{
				newAppAPIResponse(http.StatusOK, `{"messages":[{"id":42,"trigger_name":"billing_trial_expires_soon_v3"}]}`, ""),
				newAppAPIResponse(http.StatusNotFound, `{"errors":[{"detail":"not found"}]}`, ""),
			},
		},
		"matching transactional message is recent": {
			responses: []*http.Response{
				newAppAPIResponse(http.StatusOK, `{"messages":[{"id":42,"trigger_name":"billing_trial_expires_soon_v3"}]}`, ""),
				newAppAPIResponse(http.StatusOK, `{"messages":[{"id":"msg-1","transactional_message_id":42}]}`, ""),
			},
			want: true,
		},
		"GET /transactional 404 is fatal": {
			responses: []*http.Response{
				newAppAPIResponse(http.StatusNotFound, `{"errors":[{"detail":"not found"}]}`, ""),
			},
			wantErr: "list transactional messages: customer.io app api status 404",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			doer := &responseDoer{responses: tt.responses}
			client := &Client{httpClient: doer, apiKey: "test-api-key", appAPIURL: "https://example.com"}

			got, err := client.HasRecentTransactionalMessage(context.Background(), "customer@example.com", "billing_trial_expires_soon_v3", time.Unix(123, 0))

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
			require.Equal(t, len(tt.responses), doer.calls)
		})
	}
}

func TestCustomerMessages(t *testing.T) {
	doer := &responseDoer{responses: []*http.Response{
		newAppAPIResponse(http.StatusNotFound, `{"errors":[{"detail":"not found"}]}`, ""),
	}}
	client := &Client{httpClient: doer, apiKey: "test-api-key", appAPIURL: "https://example.com"}

	got, err := client.customerMessages(context.Background(), "customer@example.com", time.Unix(123, 0), 100)

	require.NoError(t, err)
	require.Empty(t, got.Messages)
	require.Equal(t, 1, doer.calls)
}

func TestGetJSON(t *testing.T) {
	tests := map[string]struct {
		responses []*http.Response
		wantErr   string
		wantName  string
		wantCalls int
	}{
		"retries rate limited response": {
			responses: []*http.Response{
				newAppAPIResponse(http.StatusTooManyRequests, `{"errors":[{"detail":"rate limited"}]}`, "0"),
				newAppAPIResponse(http.StatusOK, `{"name":"ok"}`, ""),
			},
			wantName:  "ok",
			wantCalls: 2,
		},
		"retries server errors up to limit": {
			responses: []*http.Response{
				newAppAPIResponse(http.StatusInternalServerError, `server error`, "0"),
				newAppAPIResponse(http.StatusInternalServerError, `server error`, "0"),
				newAppAPIResponse(http.StatusInternalServerError, `server error`, "0"),
				newAppAPIResponse(http.StatusInternalServerError, `server error`, "0"),
			},
			wantErr:   "customer.io app api status 500",
			wantCalls: int(appAPIMaxRetries) + 1,
		},
		"does not retry client errors": {
			responses: []*http.Response{
				newAppAPIResponse(http.StatusBadRequest, `bad request`, ""),
			},
			wantErr:   "customer.io app api status 400",
			wantCalls: 1,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			doer := &responseDoer{responses: tt.responses}
			client := &Client{httpClient: doer, apiKey: "test-api-key"}

			var got struct {
				Name string `json:"name"`
			}
			err := client.getJSON(context.Background(), "https://example.com", &got)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantName, got.Name)
			require.Equal(t, tt.wantCalls, doer.calls)
		})
	}
}

type responseDoer struct {
	responses []*http.Response
	calls     int
}

func (d *responseDoer) Do(_ *http.Request) (*http.Response, error) {
	if d.calls >= len(d.responses) {
		return newAppAPIResponse(http.StatusInternalServerError, "unexpected request", "0"), nil
	}
	response := d.responses[d.calls]
	d.calls++
	return response, nil
}

func newAppAPIResponse(statusCode int, body string, retryAfter string) *http.Response {
	header := http.Header{}
	if retryAfter != "" {
		header.Set("Retry-After", retryAfter)
	}
	return &http.Response{StatusCode: statusCode, Header: header, Body: io.NopCloser(strings.NewReader(body))}
}

func TestStructToMap(t *testing.T) {
	data := DummyTestEmailV1{
		UserName:         "John Doe",
		OrganizationName: "Acme Corp",
	}

	result, err := structToMap(data)
	assert.NoError(t, err)
	assert.Equal(t, "John Doe", result["user_name"])
	assert.Equal(t, "Acme Corp", result["organization_name"])
}

func TestMessageDataInterface(t *testing.T) {
	testEmail := DummyTestEmailV1{
		UserName:         "John Doe",
		OrganizationName: "Acme Corp",
	}
	assert.Equal(t, "dummy_test_email_v1", testEmail.TriggerName())
}

func TestSendTransactionalEmail(t *testing.T) {
	mockAPI := mocks.NewAPIClientInterface(t)

	mockAPI.EXPECT().SendEmail(
		mock.Anything,
		mock.MatchedBy(func(req *customerio.SendEmailRequest) bool {
			return req.To == "monica@xata.io" &&
				req.TransactionalMessageID == "dummy_test_email_v1" &&
				req.MessageData["user_name"] == "Monica Sarbu" &&
				req.MessageData["organization_name"] == "Xata" &&
				req.Identifiers["email"] == "monica@xata.io"
		}),
	).Return(&customerio.SendEmailResponse{}, nil)

	client := &Client{
		api:          mockAPI,
		isProduction: false,
	}

	messageData := DummyTestEmailV1{
		UserName:         "Monica Sarbu",
		OrganizationName: "Xata",
	}

	err := SendTransactionalEmail(client, context.Background(), "monica@xata.io", messageData)

	require.NoError(t, err)
	mockAPI.AssertExpectations(t)
}
