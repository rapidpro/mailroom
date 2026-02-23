package runtime

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/go-playground/validator/v10"
	"github.com/nyaruka/ezconf"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/utils"
)

func init() {
	utils.RegisterValidatorAlias("session_storage", "eq=db|eq=s3", func(e validator.FieldError) string { return "is not a valid session storage mode" })
}

// Config is our top level configuration object
type Config struct {
	DB         string `validate:"url,startswith=postgres:"           help:"URL for your Postgres database"`
	ReadonlyDB string `validate:"omitempty,url,startswith=postgres:" help:"URL of optional connection to readonly database instance"`
	DBPoolSize int    `                                              help:"the size of our db pool"`
	Valkey     string `validate:"url,startswith=valkey:"             help:"URL for your Valkey instance"`
	SentryDSN  string `                                              help:"the DSN used for logging errors to Sentry"`

	Address          string `help:"the address to bind our web server to"`
	Port             int    `help:"the port to bind our web server to"`
	AuthToken        string `help:"the token clients will need to authenticate web requests"`
	Domain           string `help:"the domain that mailroom is listening on"`
	AttachmentDomain string `help:"the domain that will be used for relative attachment"`
	SpoolDir         string `help:"the directory to use for spool files"`

	WorkersRealtime  int     `help:"the number of workers for the realtime task queue"`
	WorkersBatch     int     `help:"the number of workers for the batch task queue"`
	WorkersThrottled int     `help:"the number of workers for the throttled task queue"`
	WorkerOwnerLimit float64 `help:"the maximum number of workers, across nodes, available to a single owner, as a fraction of the per node worker counts"`

	WebhooksTimeout              int     `help:"the timeout in milliseconds for webhook calls from engine"`
	WebhooksMaxRetries           int     `help:"the number of times to retry a failed webhook call"`
	WebhooksMaxBodyBytes         int     `help:"the maximum size of bytes to a webhook call response body"`
	WebhooksInitialBackoff       int     `help:"the initial backoff in milliseconds when retrying a failed webhook call"`
	WebhooksBackoffJitter        float64 `help:"the amount of jitter to apply to backoff times"`
	WebhooksHealthyResponseLimit int     `help:"the limit in milliseconds for webhook response to be considered healthy"`

	SMTPServer           string   `help:"the default SMTP configuration for sending flow emails, e.g. smtp://user%40password@server:port/?from=foo%40gmail.com"`
	DisallowedNetworks   []string `help:"comma separated list of IP addresses and networks which engine can't make HTTP calls to"`
	MaxStepsPerSprint    int      `help:"the maximum number of steps allowed per engine sprint"`
	MaxSprintsPerSession int      `help:"the maximum number of sprints allowed per engine session"`
	MaxValueLength       int      `help:"the maximum size in characters for contact field values and run result values"`

	Elastic              string `validate:"url" help:"the URL of your ElasticSearch instance"`
	ElasticUsername      string `help:"the username for ElasticSearch if using basic auth"`
	ElasticPassword      string `help:"the password for ElasticSearch if using basic auth"`
	ElasticContactsIndex string `help:"the name of index alias for contacts"`

	// experimental - multiple indices so we can double write when switching indexes - we would query against the first in the list
	OpenSearchMessagesEndpoint string `name:"opensearch_messages_endpoint" validate:"omitempty,url" help:"the URL of your OpenSearch endpoint for messages"`
	OpenSearchMessagesIndex    string `name:"opensearch_messages_index" help:"the name of index for messages"`

	AWSAccessKeyID     string `help:"access key ID to use for AWS services"`
	AWSSecretAccessKey string `help:"secret access key to use for AWS services"`
	AWSRegion          string `help:"region to use for AWS services, e.g. us-east-1"`

	DynamoEndpoint    string `help:"DynamoDB service endpoint, e.g. https://dynamodb.us-east-1.amazonaws.com"`
	DynamoTablePrefix string `help:"prefix to use for DynamoDB tables"`

	S3Endpoint          string `help:"S3 service endpoint, e.g. https://s3.amazonaws.com"`
	S3AttachmentsBucket string `help:"S3 bucket to write attachments to"`
	S3PathStyle         bool   `help:"S3 should use path style URLs"`

	MetricsReporting    string `validate:"eq=off|eq=basic|eq=advanced"     help:"the level of metrics reporting"`
	CloudwatchNamespace string `help:"the namespace to use for cloudwatch metrics"`
	DeploymentID        string `help:"the deployment identifier to use for metrics"`
	InstanceID          string `help:"the instance identifier to use for metrics"`

	CourierAuthToken       string `help:"the authentication token used for requests to Courier"`
	AndroidCredentialsFile string `help:"path to JSON file with FCM service account credentials used to sync Android relayers"`
	IDObfuscationKey       string `help:"key used to decode obfuscated IDs, as 4 comma separated integers" validate:"omitempty,hexadecimal,len=32"`

	LogLevel slog.Level `help:"the logging level courier should use"`
	UUIDSeed int        `help:"seed to use for UUID generation in a testing environment"`
	Version  string     `help:"the version of this mailroom install"`

	// parsed values that can't be set directly
	DisallowedIPs          []net.IP
	DisallowedNets         []*net.IPNet
	IDObfuscationKeyParsed [4]uint32
}

// NewDefaultConfig returns a new default configuration object
func NewDefaultConfig() *Config {
	hostname, _ := os.Hostname()

	return &Config{
		DB:         "postgres://temba:temba@postgres/temba?sslmode=disable&Timezone=UTC",
		ReadonlyDB: "",
		DBPoolSize: 36,
		Valkey:     "valkey://valkey:6379/15",

		Address:  "localhost",
		Port:     8090,
		SpoolDir: "/var/spool/mailroom",

		WorkersRealtime:  32,
		WorkersBatch:     8,
		WorkersThrottled: 8,
		WorkerOwnerLimit: 0.5,

		WebhooksTimeout:              15000,
		WebhooksMaxBodyBytes:         256 * 1024, // 256 KiB
		WebhooksHealthyResponseLimit: 10000,

		SMTPServer:           "",
		DisallowedNetworks:   []string{`127.0.0.1`, `::1`, `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `169.254.0.0/16`, `fe80::/10`},
		MaxStepsPerSprint:    250,
		MaxSprintsPerSession: 250,
		MaxValueLength:       640,

		Elastic:              "http://elastic:9200",
		ElasticUsername:      "",
		ElasticPassword:      "",
		ElasticContactsIndex: "contacts",

		// not enabled by default.. still at experimental stage
		OpenSearchMessagesEndpoint: "",
		OpenSearchMessagesIndex:    "messages-tickets-v1",

		AWSAccessKeyID:     "",
		AWSSecretAccessKey: "",
		AWSRegion:          "us-east-1",

		DynamoEndpoint:    "", // let library generate it
		DynamoTablePrefix: "Temba",

		S3Endpoint:          "https://s3.amazonaws.com",
		S3AttachmentsBucket: "temba-attachments",

		MetricsReporting:    "off",
		CloudwatchNamespace: "Temba/Mailroom",
		DeploymentID:        "dev",
		InstanceID:          hostname,

		IDObfuscationKey: "000A3B1C000D2E3F0001A2B300C0FFEE",

		LogLevel: slog.LevelWarn,
		UUIDSeed: 0,
		Version:  "Dev",
	}
}

func LoadConfig(args ...string) (*Config, error) {
	c := NewDefaultConfig()
	loader := ezconf.NewLoader(c, "mailroom", "Mailroom - handler for RapidPro", []string{"mailroom.toml"})
	if len(args) > 0 { // allow tests to pass in args
		loader.SetArgs(args...)
	}
	if err := loader.Load(); err != nil {
		return nil, fmt.Errorf("error loading configuration: %w", err)
	}

	if err := c.Parse(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Config) Parse() error {
	// ensure config is valid
	if err := utils.Validate(c); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// parse our disallowed networks
	if err := c.parseDisallowedNetworks(); err != nil {
		return fmt.Errorf("invalid disallowed networks: %w", err)
	}

	// parse our ID obfuscation key
	bytes, err := hex.DecodeString(c.IDObfuscationKey)
	if err != nil {
		return fmt.Errorf("invalid hex string: %v", err)
	}

	var key [4]uint32
	for i := range 4 { // convert 4 bytes to uint32 (big endian)
		key[i] = uint32(bytes[i*4])<<24 | uint32(bytes[i*4+1])<<16 | uint32(bytes[i*4+2])<<8 | uint32(bytes[i*4+3])
	}
	c.IDObfuscationKeyParsed = key

	return nil
}

// parses the list of IPs and IP networks (written in CIDR notation)
func (c *Config) parseDisallowedNetworks() error {
	ips, nets, err := httpx.ParseNetworks(c.DisallowedNetworks...)
	if err != nil {
		return err
	}
	c.DisallowedIPs = ips
	c.DisallowedNets = nets
	return nil
}
