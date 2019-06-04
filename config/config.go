package config

var Mailroom *Config

func init() {
	Mailroom = NewMailroomConfig()
}

// Config is our top level configuration object
type Config struct {
	SentryDSN  string `help:"the DSN used for logging errors to Sentry"`
	DB         string `help:"URL describing how to connect to the RapidPro database"`
	DBPoolSize int    `help:"the size of our db pool"`
	Redis      string `help:"URL describing how to connect to Redis"`
	Version    string `help:"the version of this mailroom install"`
	LogLevel   string `help:"the logging level courier should use"`
	SMTPServer string `help:"the smtp configuration for sending emails ex: smtp://user%40password@server:port/?from=foo%40gmail.com"`

	BatchWorkers   int `help:"the number of go routines that will be used to handle batch events"`
	HandlerWorkers int `help:"the number of go routines that will be used to handle messages"`

	RetryPendingMessages bool `help:"whether to requeue pending messages older than five minutes to retry"`

	MaxValueLength    int `help:"the maximum size in characters for contact field values and run result values"`
	MaxStepsPerSprint int `help:"the maximum number of steps allowed per engine sprint"`

	LibratoUsername string `help:"the username that will be used to authenticate to Librato"`
	LibratoToken    string `help:"the token that will be used to authenticate to Librato"`

	Domain           string `help:"the domain that mailroom is listening on"`
	AttachmentDomain string `help:"the domain that will be used for relative attachment"`

	S3Endpoint         string `help:"the S3 endpoint we will write attachments to"`
	S3Region           string `help:"the S3 region we will write attachments to"`
	S3MediaBucket      string `help:"the S3 bucket we will write attachments to"`
	S3MediaPrefix      string `help:"the prefix that will be added to attachment filenames"`
	S3DisableSSL       bool   `help:"whether we disable SSL when accessing S3. Should always be set to False unless you're hosting an S3 compatible service within a secure internal network"`
	S3ForcePathStyle   bool   `help:"whether we force S3 path style. Should generally need to default to False unless you're hosting an S3 compatible service"`
	AWSAccessKeyID     string `help:"the access key id to use when authenticating S3"`
	AWSSecretAccessKey string `help:"the secret access key id to use when authenticating S3"`

	FCMKey string `help:"the FCM API key used to notify Android relayers to sync"`

	AuthToken string `help:"the token clients will need to authenticate web requests"`
	Address   string `help:"the address to bind our web server to"`
	Port      int    `help:"the port to bind our web server to"`
}

// NewMailroomConfig returns a new default configuration object
func NewMailroomConfig() *Config {
	return &Config{
		DB:                "postgres://temba:temba@localhost/temba?sslmode=disable",
		DBPoolSize:        36,
		Redis:             "redis://localhost:6379/15",
		BatchWorkers:      4,
		HandlerWorkers:    32,
		LogLevel:          "error",
		Version:           "Dev",
		SMTPServer:        "",
		MaxValueLength:    640,
		MaxStepsPerSprint: 100,

		S3Endpoint:         "https://s3.amazonaws.com",
		S3Region:           "us-east-1",
		S3MediaBucket:      "mailroom-media",
		S3MediaPrefix:      "/media/",
		S3DisableSSL:       false,
		S3ForcePathStyle:   false,
		AWSAccessKeyID:     "missing_aws_access_key_id",
		AWSSecretAccessKey: "missing_aws_secret_access_key",

		RetryPendingMessages: true,

		Address: "localhost",
		Port:    8090,
	}
}
