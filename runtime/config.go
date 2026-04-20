package runtime

import (
	"encoding/csv"
	"io"
	"net"
	"os"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/utils"
	"github.com/pkg/errors"
)

func init() {
	utils.RegisterValidatorAlias("session_storage", "eq=db|eq=s3", func(e validator.FieldError) string { return "is not a valid session storage mode" })
}

// Config is our top level configuration object
type Config struct {
	DB         string `validate:"url,startswith=postgres:"           help:"URL for your Postgres database"`
	ReadonlyDB string `validate:"omitempty,url,startswith=postgres:" help:"URL of optional connection to readonly database instance"`
	DBPoolSize int    `                                              help:"the size of our db pool"`
	Redis      string `validate:"url,startswith=redis:"              help:"URL for your Redis instance"`
	SentryDSN  string `                                              help:"the DSN used for logging errors to Sentry"`

	Address          string `help:"the address to bind our web server to"`
	Port             int    `help:"the port to bind our web server to"`
	AuthToken        string `help:"the token clients will need to authenticate web requests"`
	Domain           string `help:"the domain that mailroom is listening on"`
	AttachmentDomain string `help:"the domain that will be used for relative attachment"`

	BatchWorkers         int  `help:"the number of go routines that will be used to handle batch events"`
	HandlerWorkers       int  `help:"the number of go routines that will be used to handle messages"`
	RetryPendingMessages bool `help:"whether to requeue pending messages older than five minutes to retry"`

	WebhooksTimeout              int     `help:"the timeout in milliseconds for webhook calls from engine"`
	WebhooksMaxRetries           int     `help:"the number of times to retry a failed webhook call"`
	WebhooksMaxBodyBytes         int     `help:"the maximum size of bytes to a webhook call response body"`
	WebhooksInitialBackoff       int     `help:"the initial backoff in milliseconds when retrying a failed webhook call"`
	WebhooksBackoffJitter        float64 `help:"the amount of jitter to apply to backoff times"`
	WebhooksHealthyResponseLimit int     `help:"the limit in milliseconds for webhook response to be considered healthy"`

	SMTPServer           string `help:"the smtp configuration for sending emails ex: smtp://user%40password@server:port/?from=foo%40gmail.com"`
	DisallowedNetworks   string `help:"comma separated list of IP addresses and networks which engine can't make HTTP calls to"`
	MaxStepsPerSprint    int    `help:"the maximum number of steps allowed per engine sprint"`
	MaxResumesPerSession int    `help:"the maximum number of resumes allowed per engine session"`
	MaxValueLength       int    `help:"the maximum size in characters for contact field values and run result values"`
	SessionStorage       string `validate:"omitempty,session_storage"         help:"where to store session output (s3|db)"`

	Elastic              string `validate:"url" help:"the URL of your ElasticSearch instance"`
	ElasticUsername      string `help:"the username for ElasticSearch if using basic auth"`
	ElasticPassword      string `help:"the password for ElasticSearch if using basic auth"`
	ElasticContactsIndex string `help:"the name of index alias for contacts"`

	S3Endpoint          string `help:"the S3 endpoint we will write attachments to"`
	S3Region            string `help:"the S3 region we will write attachments to"`
	S3AttachmentsBucket string `help:"the S3 bucket we will write attachments to"`
	S3AttachmentsPrefix string `help:"the prefix that will be added to attachment filenames"`
	S3SessionsBucket    string `help:"the S3 bucket we will write attachments to"`
	S3LogsBucket        string `help:"the S3 bucket we will write logs to"`
	S3DisableSSL        bool   `help:"whether we disable SSL when accessing S3. Should always be set to False unless you're hosting an S3 compatible service within a secure internal network"`
	S3ForcePathStyle    bool   `help:"whether we force S3 path style. Should generally need to default to False unless you're hosting an S3 compatible service"`

	AWSAccessKeyID     string `help:"the access key id to use when authenticating S3"`
	AWSSecretAccessKey string `help:"the secret access key id to use when authenticating S3"`
	AWSUseCredChain    bool   `help:"whether to use the AWS credentials chain. Defaults to false."`

	CourierAuthToken  string `help:"the authentication token used for requests to Courier"`
	LibratoUsername   string `help:"the username that will be used to authenticate to Librato"`
	LibratoToken      string `help:"the token that will be used to authenticate to Librato"`
	FCMKey            string `help:"the FCM API key used to notify Android relayers to sync"`
	MailgunSigningKey string `help:"the signing key used to validate requests from mailgun"`

	InstanceName string `help:"the unique name of this instance used for analytics"`
	LogLevel     string `help:"the logging level courier should use"`
	UUIDSeed     int    `help:"seed to use for UUID generation in a testing environment"`
	Version      string `help:"the version of this mailroom install"`
}

// NewDefaultConfig returns a new default configuration object
func NewDefaultConfig() *Config {
	hostname, _ := os.Hostname()

	return &Config{
		DB:         "postgres://temba:temba@localhost/temba?sslmode=disable&Timezone=UTC",
		ReadonlyDB: "",
		DBPoolSize: 36,
		Redis:      "redis://localhost:6379/15",

		Address: "localhost",
		Port:    8090,

		BatchWorkers:         4,
		HandlerWorkers:       32,
		RetryPendingMessages: true,

		WebhooksTimeout:              15000,
		WebhooksMaxRetries:           2,
		WebhooksMaxBodyBytes:         1024 * 1024, // 1MB
		WebhooksInitialBackoff:       5000,
		WebhooksBackoffJitter:        0.5,
		WebhooksHealthyResponseLimit: 10000,

		SMTPServer:           "",
		DisallowedNetworks:   `127.0.0.1,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,169.254.0.0/16,fe80::/10`,
		MaxStepsPerSprint:    200,
		MaxResumesPerSession: 250,
		MaxValueLength:       640,
		SessionStorage:       "db",

		Elastic:              "http://localhost:9200",
		ElasticUsername:      "",
		ElasticPassword:      "",
		ElasticContactsIndex: "contacts",

		S3Endpoint:          "https://s3.amazonaws.com",
		S3Region:            "us-east-1",
		S3AttachmentsBucket: "attachments-bucket",
		S3AttachmentsPrefix: "attachments/",
		S3SessionsBucket:    "sessions-bucket",
		S3LogsBucket:        "logs-bucket",
		S3DisableSSL:        false,
		S3ForcePathStyle:    false,

		AWSAccessKeyID:     "",
		AWSSecretAccessKey: "",
		AWSUseCredChain:    false,

		InstanceName: hostname,
		LogLevel:     "error",
		UUIDSeed:     0,
		Version:      "Dev",
	}
}

// Validate validates the config
func (c *Config) Validate() error {
	if err := utils.Validate(c); err != nil {
		return err
	}

	if _, _, err := c.ParseDisallowedNetworks(); err != nil {
		return errors.Wrap(err, "unable to parse 'DisallowedNetworks'")
	}
	return nil
}

// ParseDisallowedNetworks parses the list of IPs and IP networks (written in CIDR notation)
func (c *Config) ParseDisallowedNetworks() ([]net.IP, []*net.IPNet, error) {
	addrs, err := csv.NewReader(strings.NewReader(c.DisallowedNetworks)).Read()
	if err != nil && err != io.EOF {
		return nil, nil, err
	}

	return httpx.ParseNetworks(addrs...)
}
