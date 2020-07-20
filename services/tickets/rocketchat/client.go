package rocketchat

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/goflow/utils/jsonx"
	"io"
	"net/http"
)

// Client is a basic RocketChat app client
type Client struct {
	httpClient  *http.Client
	httpRetries *httpx.RetryConfig
	domain      string
	appID       string
	secret      string
}

// NewClient creates a new RocketChat app client
func NewClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, domain, appID, secret string) *Client {
	return &Client{
		httpClient:  httpClient,
		httpRetries: httpRetries,
		domain:      domain,
		appID:       appID,
		secret:      secret,
	}
}

type errorResponse struct {
	Title       string `json:"error"`
	Description string `json:"details"`
}

func (c *Client) request(method, endpoint string, payload interface{}, response interface{}) (*httpx.Trace, error) {
	url := fmt.Sprintf("https://%s/api/apps/public/%s/%s", c.domain, c.appID, endpoint)
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Token %s", c.secret),
		"Content-Type":  "application/json",
	}
	var body io.Reader

	if payload != nil {
		data, err := jsonx.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(data)
	}

	req, err := httpx.NewRequest(method, url, body, headers)
	if err != nil {
		return nil, err
	}

	trace, err := httpx.DoTrace(c.httpClient, req, c.httpRetries, nil, -1)
	if err != nil {
		return trace, err
	}

	if trace.Response.StatusCode >= 400 {
		response := &errorResponse{}
		err := jsonx.Unmarshal(trace.ResponseBody, response)
		if err != nil {
			return trace, err
		}
		return trace, errors.New(response.Description)
	}

	if response != nil {
		return trace, jsonx.Unmarshal(trace.ResponseBody, response)
	}
	return trace, nil
}

func (c *Client) get(endpoint string, payload interface{}, response interface{}) (*httpx.Trace, error) {
	return c.request("GET", endpoint, payload, response)
}

func (c *Client) post(endpoint string, payload interface{}, response interface{}) (*httpx.Trace, error) {
	return c.request("POST", endpoint, payload, response)
}

type Visitor struct {
	Token        string            `json:"token"       validate:"required"`
	ContactUUID  string            `json:"contactUuid"`
	Department   string            `json:"department"`
	Name         string            `json:"name"`
	Email        string            `json:"email"`
	Phone        string            `json:"phone"`
	CustomFields map[string]string `json:"customFields"`
}

type Room struct {
	Visitor      Visitor `json:"visitor"     validate:"required"`
	TicketID     string  `json:"ticketId"    validate:"required"`
	Priority     string  `json:"priority"`
	SessionStart string  `json:"sessionStart"`
}

// CreateRoom creates a new room and returns the ID
func (c *Client) CreateRoom(room *Room) (string, *httpx.Trace, error) {
	response := &struct {
		ID string `json:"id"`
	}{}

	trace, err := c.get("room", room, response)
	if err != nil {
		return "", trace, err
	}

	return response.ID, trace, nil
}

func (c *Client) CloseRoom(visitor *Visitor) (*httpx.Trace, error) {
	payload := struct {
		Visitor *Visitor `json:"visitor"`
	}{Visitor: visitor}

	trace, err := c.get("room.close", payload, nil)
	if err != nil {
		return trace, err
	}

	return trace, nil
}

type VisitorMsg struct {
	Visitor     Visitor  `json:"visitor"    validate:"required"`
	Text        string   `json:"text"`
	Attachments []string `json:"attachments"`
}

func (c *Client) SendMessage(msg *VisitorMsg) (string, *httpx.Trace, error) {
	response := &struct {
		ID string `json:"id"`
	}{}

	trace, err := c.post("visitor-message", msg, response)
	if err != nil {
		return "", trace, err
	}

	return response.ID, trace, nil
}
