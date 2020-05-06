package zendesk

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/goflow/utils/jsonx"
)

// Client is a basic zendesk client which uses OAuth
type Client struct {
	httpClient  *http.Client
	httpRetries *httpx.RetryConfig
	subdomain   string
	token       string
}

// NewClient creates a new zendesk client
func NewClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, subdomain, token string) *Client {
	return &Client{
		httpClient:  httpClient,
		httpRetries: httpRetries,
		subdomain:   subdomain,
		token:       token,
	}
}

type errorResponse struct {
	Error       string `json:"error"`
	Description string `json:"description"`
}

type User struct {
	ID         int64  `json:"id,omitempty"`
	URL        string `json:"url,omitempty"`
	Name       string `json:"name"`
	Role       string `json:"role"`
	ExternalID string `json:"external_id"`
}

// CreateOrUpdateUser creates or updates a user matching on external ID
// see https://developer.zendesk.com/rest_api/docs/support/users#create-or-update-user
func (c *Client) CreateOrUpdateUser(name, role, externalID string) (*User, *httpx.Trace, error) {
	user := &struct {
		User User `json:"user"`
	}{
		User: User{
			Name:       name,
			Role:       role,
			ExternalID: externalID,
		},
	}

	trace, err := c.post("users/create_or_update", user, user)
	if err != nil {
		return nil, trace, err
	}

	return &user.User, trace, nil
}

type ticketComment struct {
	Body string `json:"body"`
}

type newTicket struct {
	RequesterID int64         `json:"requester_id"`
	Subject     string        `json:"subject"`
	Comment     ticketComment `json:"comment"`
	ExternalID  string        `json:"external_id"`
}

// Ticket is a ticket in Zendesk
type Ticket struct {
	ID          int64     `json:"id"`
	URL         string    `json:"url"`
	RequesterID int64     `json:"requester_id"`
	Subject     string    `json:"subject"`
	ExternalID  string    `json:"external_id"`
	CreatedAt   time.Time `json:"created_at"`
}

// CreateTicket creates a new ticket
// see https://developer.zendesk.com/rest_api/docs/support/tickets#create-ticket
func (c *Client) CreateTicket(requesterID int64, subject, body string) (*Ticket, *httpx.Trace, error) {
	newTicket := &struct {
		Ticket newTicket `json:"ticket"`
	}{
		Ticket: newTicket{
			RequesterID: requesterID,
			Subject:     subject,
			Comment:     ticketComment{Body: body},
		},
	}
	ticket := &struct {
		Ticket Ticket `json:"ticket"`
	}{}

	trace, err := c.post("tickets", newTicket, ticket)
	if err != nil {
		return nil, trace, err
	}

	return &ticket.Ticket, trace, nil
}

func (c *Client) post(endpoint string, payload interface{}, response interface{}) (*httpx.Trace, error) {
	data, err := jsonx.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("https://%s.zendesk.com/api/v2/%s.json", c.subdomain, endpoint), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))

	trace, err := httpx.DoTrace(c.httpClient, req, c.httpRetries, nil, -1)
	if err != nil {
		return trace, err
	}

	if trace.Response.StatusCode >= 400 {
		response := &errorResponse{}
		jsonx.Unmarshal(trace.ResponseBody, response)
		return trace, errors.New(response.Description)
	}

	return trace, jsonx.Unmarshal(trace.ResponseBody, response)
}
