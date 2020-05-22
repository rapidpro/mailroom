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

type baseClient struct {
	httpClient  *http.Client
	httpRetries *httpx.RetryConfig
	subdomain   string
	token       string
}

func newBaseClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, subdomain, token string) baseClient {
	return baseClient{
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

func (c *baseClient) post(endpoint string, payload interface{}, response interface{}) (*httpx.Trace, error) {
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

// RESTClient is a client for the Zendesk REST API
type RESTClient struct {
	baseClient
}

// NewRESTClient creates a new REST client
func NewRESTClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, subdomain, token string) *RESTClient {
	return &RESTClient{baseClient: newBaseClient(httpClient, httpRetries, subdomain, token)}
}

// Target see https://developer.zendesk.com/rest_api/docs/support/targets
type Target struct {
	ID          int64  `json:"id"`
	Title       string `json:"title"`
	Type        string `json:"type"`
	TargetURL   string `json:"target_url"`
	Method      string `json:"method"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	ContentType string `json:"content_type"`
}

// CreateTarget see https://developer.zendesk.com/rest_api/docs/support/targets#create-target
func (c *RESTClient) CreateTarget(target *Target) (*Target, *httpx.Trace, error) {
	payload := struct {
		Target *Target `json:"target"`
	}{Target: target}

	response := &struct {
		Target *Target `json:"target"`
	}{}

	trace, err := c.post("targets", payload, response)
	if err != nil {
		return nil, trace, err
	}

	return response.Target, trace, nil
}

// Condition see https://developer.zendesk.com/rest_api/docs/support/triggers#conditions
type Condition struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

// Conditions see https://developer.zendesk.com/rest_api/docs/support/triggers#conditions
type Conditions struct {
	All []Condition `json:"all"`
	Any []Condition `json:"any"`
}

// Action see https://developer.zendesk.com/rest_api/docs/support/triggers#actions
type Action struct {
	Field string   `json:"field"`
	Value []string `json:"value"`
}

// Trigger see https://developer.zendesk.com/rest_api/docs/support/triggers
type Trigger struct {
	ID         int64      `json:"id"`
	Title      string     `json:"title"`
	Conditions Conditions `json:"conditions"`
	Actions    []Action   `json:"actions"`
}

// CreateTrigger see https://developer.zendesk.com/rest_api/docs/support/triggers#create-trigger
func (c *RESTClient) CreateTrigger(trigger *Trigger) (*Trigger, *httpx.Trace, error) {
	payload := struct {
		Trigger *Trigger `json:"trigger"`
	}{Trigger: trigger}

	response := &struct {
		Trigger *Trigger `json:"trigger"`
	}{}

	trace, err := c.post("triggers", payload, response)
	if err != nil {
		return nil, trace, err
	}

	return response.Trigger, trace, nil
}

// PushClient is a client for the Zendesk channel push API and requires a special push token
type PushClient struct {
	baseClient
}

// NewPushClient creates a new push client
func NewPushClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, subdomain, token string) *PushClient {
	return &PushClient{baseClient: newBaseClient(httpClient, httpRetries, subdomain, token)}
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
func (c *PushClient) Push(instanceID string, externalResources []*ExternalResource) ([]*Result, *httpx.Trace, error) {
	payload := struct {
		InstancePushID    string              `json:"instance_push_id"`
		ExternalResources []*ExternalResource `json:"external_resources"`
	}{InstancePushID: instanceID, ExternalResources: externalResources}

	response := &struct {
		Results []*Result `json:"results"`
	}{}

	trace, err := c.post("any_channel/push", payload, response)
	if err != nil {
		return nil, trace, err
	}

	return response.Results, trace, nil
}
