package storage

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/nyaruka/goflow/utils"
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

// StoreAttachment saves an attachment to storage
func StoreAttachment(s Storage, prefix string, ownerID int, filename string, content []byte) (utils.Attachment, error) {
	contentType := http.DetectContentType(content)

	path := attachmentPath(prefix, ownerID, filename)

	url, err := s.Put(path, contentType, content)
	if err != nil {
		return "", err
	}

	return utils.Attachment(contentType + ":" + url), nil
}

func attachmentPath(prefix string, ownerID int, filename string) string {
	parts := []string{prefix, fmt.Sprintf("%d", ownerID)}

	// not all filesystems like having a directory with a huge number of files, so if filename is long enough,
	// use parts of it to create intermediate subdirectories
	if len(filename) > 4 {
		parts = append(parts, filename[:4])

		if len(filename) > 8 {
			parts = append(parts, filename[4:8])
		}
	}
	parts = append(parts, filename)

	path := filepath.Join(parts...)

	// ensure path begins with /
	if !strings.HasPrefix(path, "/") {
		path = fmt.Sprintf("/%s", path)
	}

	return path
}
