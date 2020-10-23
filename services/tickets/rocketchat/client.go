package rocketchat

import (
	"bytes"
	"fmt"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/pkg/errors"
	"io"
	"net/http"
)

// Client is a basic RocketChat app client
type Client struct {
	httpClient  *http.Client
	httpRetries *httpx.RetryConfig
	baseURL     string
	secret      string
}

// NewClient creates a new RocketChat app client
func NewClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, baseURL, secret string) *Client {
	return &Client{
		httpClient:  httpClient,
		httpRetries: httpRetries,
		baseURL:     baseURL,
		secret:      secret,
	}
}

type errorResponse struct {
	Error string `json:"error"`
}

func (c *Client) request(method, endpoint string, payload interface{}, response interface{}) (*httpx.Trace, error) {
	url := fmt.Sprintf("%s/%s", c.baseURL, endpoint)
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
		return trace, errors.New(response.Error)
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
	Token        string            `json:"token"`
	ContactUUID  string            `json:"contactUUID,omitempty"`
	Name         string            `json:"name,omitempty"`
	Email        string            `json:"email,omitempty"`
	Phone        string            `json:"phone,omitempty"`
}

type Room struct {
	Visitor  Visitor `json:"visitor"`
	TicketID string  `json:"ticketID"`
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

type Attachment struct {
	Type string `json:"type"`
	URL  string `json:"url"`
}

type VisitorMsg struct {
	Visitor     Visitor      `json:"visitor"`
	Text        string       `json:"text,omitempty"`
	Attachments []Attachment `json:"attachments,omitempty"`
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
