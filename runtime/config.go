package runtime

import (
	"encoding/csv"
	"io"
	"net"
	"os"
	"strings"

	"github.com/nyaruka/goflow/utils"
	"github.com/pkg/errors"
	"gopkg.in/go-playground/validator.v9"
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
	Elastic    string `validate:"url"                                help:"URL for your ElasticSearch service"`
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

	S3Endpoint           string `help:"the S3 endpoint we will write attachments to"`
	S3Region             string `help:"the S3 region we will write attachments to"`
	S3MediaBucket        string `help:"the S3 bucket we will write attachments to"`
	S3MediaPrefix        string `help:"the prefix that will be added to attachment filenames"`
	S3MediaPrefixZendesk string `help:"the prefix that will be added to file attachment names for Zendesk tickets"`
	S3SessionBucket      string `help:"the S3 bucket we will write attachments to"`
	S3SessionPrefix      string `help:"the prefix that will be added to attachment filenames"`
	S3DisableSSL         bool   `help:"whether we disable SSL when accessing S3. Should always be set to False unless you're hosting an S3 compatible service within a secure internal network"`
	S3ForcePathStyle     bool   `help:"whether we force S3 path style. Should generally need to default to False unless you're hosting an S3 compatible service"`

	AWSAccessKeyID     string `help:"the access key id to use when authenticating S3"`
	AWSSecretAccessKey string `help:"the secret access key id to use when authenticating S3"`

	LibratoUsername string `help:"the username that will be used to authenticate to Librato"`
	LibratoToken    string `help:"the token that will be used to authenticate to Librato"`

	FCMKey            string `help:"the FCM API key used to notify Android relayers to sync"`
	MailgunSigningKey string `help:"the signing key used to validate requests from mailgun"`

	InstanceName        string `help:"the unique name of this instance used for analytics"`
	LogLevel            string `help:"the logging level courier should use"`
	UUIDSeed            int    `help:"seed to use for UUID generation in a testing environment"`
	Version             string `help:"the version of this mailroom install"`
	TimeoutTime         int    `help:"the amount of time to between every timeout queued"`
	WenichatsServiceURL string `help:"wenichats external api url for ticketer service integration"`
}

// NewDefaultConfig returns a new default configuration object
func NewDefaultConfig() *Config {
	hostname, _ := os.Hostname()

	return &Config{
		DB:         "postgres://temba:temba@localhost/temba?sslmode=disable&Timezone=UTC",
		ReadonlyDB: "",
		DBPoolSize: 36,
		Redis:      "redis://localhost:6379/15",
		Elastic:    "http://localhost:9200",

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
		MaxStepsPerSprint:    100,
		MaxResumesPerSession: 250,
		MaxValueLength:       640,
		SessionStorage:       "db",

		S3Endpoint:       "https://s3.amazonaws.com",
		S3Region:         "us-east-1",
		S3MediaBucket:    "mailroom-media",
		S3MediaPrefix:    "/media/",
		S3SessionBucket:  "mailroom-sessions",
		S3SessionPrefix:  "/",
		S3DisableSSL:     false,
		S3ForcePathStyle: false,

		AWSAccessKeyID:     "",
		AWSSecretAccessKey: "",

		InstanceName:        hostname,
		LogLevel:            "error",
		UUIDSeed:            0,
		Version:             "Dev",
		TimeoutTime:         15,
		WenichatsServiceURL: "https://chats-engine.dev.cloud.weni.ai/v1/external",
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

	ips := make([]net.IP, 0, len(addrs))
	ipNets := make([]*net.IPNet, 0, len(addrs))

	for _, addr := range addrs {
		if strings.Contains(addr, "/") {
			_, ipNet, err := net.ParseCIDR(addr)
			if err != nil {
				return nil, nil, errors.Errorf("couldn't parse '%s' as an IP network", addr)
			}
			ipNets = append(ipNets, ipNet)
		} else {
			ip := net.ParseIP(addr)
			if ip == nil {
				return nil, nil, errors.Errorf("couldn't parse '%s' as an IP address", addr)
			}
			ips = append(ips, ip)
		}
	}

	return ips, ipNets, nil
}
