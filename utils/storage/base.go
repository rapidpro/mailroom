package storage

import (
	"github.com/nyaruka/mailroom/config"
)

// Storage is the interface that provides storage of atttachments etc
type Storage interface {
	Name() string
	Test() error
	Put(path string, contentType string, contents []byte) (string, error)
}

// New creates a new storage service
func New(cfg *config.Config) (Storage, error) {
	if cfg.AWSAccessKeyID != "" && cfg.AWSSecretAccessKey != "" {
		return NewS3(cfg)
	}
	return NewFS("_storage"), nil
}
