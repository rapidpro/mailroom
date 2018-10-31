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

	LibratoUsername string `help:"the username that will be used to authenticate to Librato"`
	LibratoToken    string `help:"the token that will be used to authenticate to Librato"`

	AttachmentDomain string `help:"the domain that will be used for relative attachment"`

	AuthToken string `help:"the token clients will need to authenticate web requests"`
	Address   string `help:"the address to bind our web server to"`
	Port      int    `help:"the port to bind our web server to"`
}

// NewMailroomConfig returns a new default configuration object
func NewMailroomConfig() *Config {
	return &Config{
		DB:             "postgres://temba@localhost/temba?sslmode=disable",
		DBPoolSize:     8,
		Redis:          "redis://localhost:6379/15",
		BatchWorkers:   4,
		HandlerWorkers: 128,
		LogLevel:       "error",
		Version:        "Dev",
		SMTPServer:     "",

		Address: "localhost",
		Port:    8090,
	}
}
