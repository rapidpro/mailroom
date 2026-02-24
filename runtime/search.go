package runtime

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/nyaruka/gocommon/aws/osearch"
)

type OpenSearch struct {
	Messages      *osearch.Writer
	MessagesSpool *osearch.Spool
}

func newOpenSearch(cfg *Config) (*OpenSearch, error) {
	client, err := osearch.NewClient(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSRegion, cfg.OSSeriesEndpoint)
	if err != nil {
		return nil, fmt.Errorf("error creating OpenSearch messages client: %w", err)
	}

	spool := osearch.NewSpool(client, filepath.Join(cfg.SpoolDir, "opensearch-messages"), 30*time.Second)

	return &OpenSearch{
		Messages:      osearch.NewWriter(client, cfg.OSMessagesTicketsIndex, osearch.ActionCreate, 500, 250*time.Millisecond, 1000, spool),
		MessagesSpool: spool,
	}, nil
}

func (s *OpenSearch) start() error {
	// TEMP until search is required in config
	if s == nil {
		return nil
	}

	if err := s.MessagesSpool.Start(); err != nil {
		return fmt.Errorf("error starting opensearch spool: %w", err)
	}

	s.Messages.Start()
	return nil
}

func (s *OpenSearch) stop() {
	// TEMP until search is required in config
	if s == nil {
		return
	}

	s.Messages.Stop()
	s.MessagesSpool.Stop()
}
