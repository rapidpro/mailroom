package zendesk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/goflow/utils/jsonx"
)

// Client is a basic Zendesk client
type Client struct {
	httpClient  *http.Client
	httpRetries *httpx.RetryConfig
	subdomain   string
	token       string
}

// NewClient creates a new Zendesk client
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

// Author see https://developer.zendesk.com/rest_api/docs/support/channel_framework#author-object
type Author struct {
	ExternalID string          `json:"external_id"`
	Name       string          `json:"name,omitempty"`
	ImageURL   string          `json:"image_url,omitempty"`
	Locale     string          `json:"locale,omitempty"`
	Fields     json.RawMessage `json:"fields,omitempty"`
}

// DisplayInfo see https://developer.zendesk.com/rest_api/docs/support/channel_framework#display_info-object
type DisplayInfo struct {
	Type string            `json:"type"`
	Data map[string]string `json:"data"`
}

// ExternalResource see https://developer.zendesk.com/rest_api/docs/support/channel_framework#external_resource-object
type ExternalResource struct {
	ExternalID       string        `json:"external_id"`
	Message          string        `json:"message"`
	HTMLMessage      string        `json:"html_message,omitempty"`
	ParentID         string        `json:"parent_id,omitempty"`
	ThreadID         string        `json:"thread_id,omitempty"`
	CreatedAt        time.Time     `json:"created_at"`
	Author           Author        `json:"author"`
	DisplayInfo      []DisplayInfo `json:"display_info,omitempty"`
	AllowChannelback bool          `json:"allow_channelback"`
}

// Status see https://developer.zendesk.com/rest_api/docs/support/channel_framework#status-object
type Status struct {
	Code        string `json:"code"`
	Description string `json:"description"`
}

// Result see https://developer.zendesk.com/rest_api/docs/support/channel_framework#result-object
type Result struct {
	ExternalResourceID string `json:"external_resource_id"`
	Status             Status `json:"status"`
}

// Push pushes the given external resources
func (c *Client) Push(instanceID string, externalResources []*ExternalResource) ([]*Result, *httpx.Trace, error) {
	push := struct {
		InstancePushID    string              `json:"instance_push_id"`
		ExternalResources []*ExternalResource `json:"external_resources"`
	}{
		InstancePushID:    instanceID,
		ExternalResources: externalResources,
	}

	response := &struct {
		Results []*Result `json:"results"`
	}{}

	trace, err := c.post("any_channel/push", push, response)
	if err != nil {
		return nil, trace, err
	}

	return response.Results, trace, nil
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
