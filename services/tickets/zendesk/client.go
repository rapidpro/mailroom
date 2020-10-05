package zendesk

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/gocommon/jsonx"
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
	return c.request("POST", endpoint, payload, response)
}

func (c *baseClient) put(endpoint string, payload interface{}, response interface{}) (*httpx.Trace, error) {
	return c.request("PUT", endpoint, payload, response)
}

func (c *baseClient) delete(endpoint string) (*httpx.Trace, error) {
	return c.request("DELETE", endpoint, nil, nil)
}

func (c *baseClient) request(method, endpoint string, payload interface{}, response interface{}) (*httpx.Trace, error) {
	url := fmt.Sprintf("https://%s.zendesk.com/api/v2/%s", c.subdomain, endpoint)
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", c.token),
	}
	var body io.Reader

	if payload != nil {
		data, err := jsonx.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(data)
		headers["Content-Type"] = "application/json"
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
		jsonx.Unmarshal(trace.ResponseBody, response)
		return trace, errors.New(response.Description)
	}

	if response != nil {
		return trace, jsonx.Unmarshal(trace.ResponseBody, response)
	}
	return trace, nil
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

	trace, err := c.post("targets.json", payload, response)
	if err != nil {
		return nil, trace, err
	}

	return response.Target, trace, nil
}

// DeleteTarget see https://developer.zendesk.com/rest_api/docs/support/targets#delete-target
func (c *RESTClient) DeleteTarget(id int64) (*httpx.Trace, error) {
	return c.delete(fmt.Sprintf("targets/%d.json", id))
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

	trace, err := c.post("triggers.json", payload, response)
	if err != nil {
		return nil, trace, err
	}

	return response.Trigger, trace, nil
}

// DeleteTrigger see https://developer.zendesk.com/rest_api/docs/support/triggers#delete-trigger
func (c *RESTClient) DeleteTrigger(id int64) (*httpx.Trace, error) {
	return c.delete(fmt.Sprintf("triggers/%d.json", id))
}

// Ticket see https://developer.zendesk.com/rest_api/docs/support/tickets#json-format
type Ticket struct {
	ID         int64  `json:"id,omitempty"`
	ExternalID string `json:"external_id,omitempty"`
	Status     string `json:"status,omitempty"`
}

// JobStatus see https://developer.zendesk.com/rest_api/docs/support/job_statuses#job-statuses
type JobStatus struct {
	ID     string `json:"id"`
	URL    string `json:"url"`
	Status string `json:"status"`
}

// UpdateManyTickets see https://developer.zendesk.com/rest_api/docs/support/tickets#update-many-tickets
func (c *RESTClient) UpdateManyTickets(ids []int64, status string) (*JobStatus, *httpx.Trace, error) {
	payload := struct {
		Ticket *Ticket `json:"ticket"`
	}{
		Ticket: &Ticket{Status: status},
	}

	response := &struct {
		JobStatus *JobStatus `json:"job_status"`
	}{}

	trace, err := c.put("tickets/update_many.json?ids="+encodeIds(ids), payload, response)
	if err != nil {
		return nil, trace, err
	}

	return response.JobStatus, trace, nil
}

// PushClient is a client for the Zendesk channel push API and requires a special push token
type PushClient struct {
	baseClient
}

// NewPushClient creates a new push client
func NewPushClient(httpClient *http.Client, httpRetries *httpx.RetryConfig, subdomain, token string) *PushClient {
	return &PushClient{baseClient: newBaseClient(httpClient, httpRetries, subdomain, token)}
}

// FieldValue is a value for the named field
type FieldValue struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

// Author see https://developer.zendesk.com/rest_api/docs/support/channel_framework#author-object
type Author struct {
	ExternalID string       `json:"external_id"`
	Name       string       `json:"name,omitempty"`
	ImageURL   string       `json:"image_url,omitempty"`
	Locale     string       `json:"locale,omitempty"`
	Fields     []FieldValue `json:"fields,omitempty"`
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
	Fields           []FieldValue  `json:"fields,omitempty"`
	FileURLs         []string      `json:"file_urls,omitempty"`
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
func (c *PushClient) Push(instanceID, requestID string, externalResources []*ExternalResource) ([]*Result, *httpx.Trace, error) {
	payload := struct {
		InstancePushID    string              `json:"instance_push_id"`
		RequestID         string              `json:"request_id,omitempty"`
		ExternalResources []*ExternalResource `json:"external_resources"`
	}{InstancePushID: instanceID, RequestID: requestID, ExternalResources: externalResources}

	response := &struct {
		Results []*Result `json:"results"`
	}{}

	trace, err := c.post("any_channel/push.json", payload, response)
	if err != nil {
		return nil, trace, err
	}

	return response.Results, trace, nil
}

func encodeIds(ids []int64) string {
	idStrs := make([]string, len(ids))
	for i := range ids {
		idStrs[i] = fmt.Sprintf("%d", ids[i])
	}
	return strings.Join(idStrs, ",")
}
