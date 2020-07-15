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

type VisitorToken string

type Visitor struct {
	Token        VisitorToken      `json:"token"`
	Department   string            `json:"department"`
	Name         string            `json:"name"`
	Email        string            `json:"email"`
	Phone        string            `json:"phone"`
	CustomFields map[string]string `json:"customFields"`
}

type Room struct {
	ID string `json:"id"`
}

func (c *Client) CreateRoom(visitor *Visitor, extraFields string) (*Room, *httpx.Trace, error) {
	payload := struct {
		Visitor      *Visitor `json:"visitor"`
		SessionStart string   `json:"sessionStart"`
		Priority     string   `json:"priority"`
	}{Visitor: visitor}

	extra := &struct {
		SessionStart string            `json:"sessionStart"`
		Priority     string            `json:"priority"`
		Department   string            `json:"department"`
		CustomFields map[string]string `json:"customFields"`
	}{}
	if err := jsonx.Unmarshal([]byte(extraFields), extra); err == nil {
		payload.Visitor.Department = extra.Department
		payload.Visitor.CustomFields = extra.CustomFields
		payload.Priority = extra.Priority
		payload.SessionStart = extra.SessionStart
	}

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

func (c *Client) SendMessage(msg *VisitorMsg) (*VisitorMsgResult, *httpx.Trace, error) {
	response := &VisitorMsgResult{}

	trace, err := c.post("visitor-message", msg, response)
	if err != nil {
		return nil, trace, err
	}

	return response, trace, nil
}
