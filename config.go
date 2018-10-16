package mailroom

// MailroomConfig is our top level configuration object
type MailroomConfig struct {
	SentryDSN  string `help:"the DSN used for logging errors to Sentry"`
	DB         string `help:"URL describing how to connect to the RapidPro database"`
	DBPoolSize int    `help:"the size of our db pool"`
	Redis      string `help:"URL describing how to connect to Redis"`
	Version    string `help:"the version of this mailroom install"`
	LogLevel   string `help:"the logging level courier should use"`

	BatchWorkers   int `help:"the number of go routines that will be used to handle batch events"`
	HandlerWorkers int `help:"the number of go routines that will be used to handle messages"`

	LibratoUsername string `help:"the username that will be used to authenticate to Librato"`
	LibratoToken    string `help:"the token that will be used to authenticate to Librato"`

	AttachmentDomain string `help:"the domain that will be used for relative attachment"`
}

// NewMailroomConfig returns a new default configuration object
func NewMailroomConfig() *MailroomConfig {
	return &MailroomConfig{
		DB:             "postgres://temba@localhost/temba?sslmode=disable",
		DBPoolSize:     8,
		Redis:          "redis://localhost:6379/0",
		BatchWorkers:   4,
		HandlerWorkers: 128,
		LogLevel:       "error",
		Version:        "Dev",
	}
}
