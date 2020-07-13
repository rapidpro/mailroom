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

const RCAppID = "29542a4b-5a89-4f27-872b-5f8091899f7b"

// Client is a basic RocketChat app client
type Client struct {
	httpClient  *http.Client
	httpRetries *httpx.RetryConfig
	domain      string
	secret      string
}

// NewClient creates a new RocketChat app client
func NewClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, domain, secret string) *Client {
	return &Client{
		httpClient:  httpClient,
		httpRetries: httpRetries,
		domain:      domain,
		secret:      secret,
	}
}

type errorResponse struct {
	Title       string `json:"error"`
	Description string `json:"details"`
}

func (c *Client) request(method, endpoint string, payload interface{}, response interface{}) (*httpx.Trace, error) {
	url := fmt.Sprintf("https://%s/api/apps/public/%s/%s", c.domain, RCAppID, endpoint)
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

type VisitorToken string

type Visitor struct {
	Token        VisitorToken      `json:"token"`
	Name         string            `json:"name"`
	Email        string            `json:"email"`
	Phone        string            `json:"phone"`
	CustomFields map[string]string `json:"customFields"`
}

type Room struct {
	ID string `json:"id"`
}

func (c *Client) CreateRoom(visitor *Visitor) (*Room, *httpx.Trace, error) {
	payload := struct {
		Visitor *Visitor `json:"visitor"`
	}{Visitor: visitor}

	response := &Room{}

	trace, err := c.get("room", payload, response)
	if err != nil {
		return nil, trace, err
	}

	return response, trace, nil
}

func (c *Client) CloseRoom(token VisitorToken, comment string) (*httpx.Trace, error) {
	payload := struct {
		Visitor struct {
			Token VisitorToken `json:"token"`
		} `json:"visitor"`
		Comment string `json:"comment"`
	}{Comment: comment}

	payload.Visitor.Token = token
	response := &Room{}

	trace, err := c.get("room.close", payload, response)
	if err != nil {
		return trace, err
	}

	return trace, nil
}

type VisitorMsg struct {
	Visitor struct {
		Token VisitorToken `json:"token"`
	} `json:"visitor"`
	Text        string   `json:"text"`
	Attachments []string `json:"attachments"`
}

type VisitorMsgResult struct {
	ID string `json:"id"`
}

func (c *Client) SendVisitorMessage(msg *VisitorMsg) (*VisitorMsgResult, *httpx.Trace, error) {
	response := &VisitorMsgResult{}

	trace, err := c.post("visitor-message", msg, response)
	if err != nil {
		return nil, trace, err
	}

	return response, trace, nil
}
