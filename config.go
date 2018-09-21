package mailroom

// Config is our top level configuration object
type Config struct {
	SentryDSN  string `help:"the DSN used for logging errors to Sentry"`
	DB         string `help:"URL describing how to connect to the RapidPro database"`
	Redis      string `help:"URL describing how to connect to Redis"`
	MaxWorkers int    `help:"the maximum number of go routines that will be used to handle events"`
	Version    string `help:"the version of this mailroom install"`
	LogLevel   string `help:"the logging level courier should use"`

	LibratoUsername string `help:"the username that will be used to authenticate to Librato"`
	LibratoToken    string `help:"the token that will be used to authenticate to Librato"`
}

// NewConfig returns a new default configuration object
func NewConfig() *Config {
	return &Config{
		DB:         "postgres://temba@localhost/temba?sslmode=disable",
		Redis:      "redis://localhost:6379/0",
		MaxWorkers: 32,
		LogLevel:   "error",
		Version:    "Dev",
	}
}
