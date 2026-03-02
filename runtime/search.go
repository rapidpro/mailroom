package runtime

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/nyaruka/gocommon/aws/osearch"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

type OpenSearch struct {
	Client *opensearchapi.Client
	Writer *osearch.Writer
	Spool  *osearch.Spool
}

func newOpenSearch(cfg *Config) (*OpenSearch, error) {
	client, err := osearch.NewClient(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSRegion, cfg.OSEndpoint)
	if err != nil {
		return nil, fmt.Errorf("error creating OpenSearch client: %w", err)
	}

	spool := osearch.NewSpool(client, filepath.Join(cfg.SpoolDir, "opensearch"), 30*time.Second)

	return &OpenSearch{
		Client: client,
		Writer: osearch.NewWriter(client, 500, 250*time.Millisecond, 1000, spool),
		Spool:  spool,
	}, nil
}

func (s *OpenSearch) start() error {
	if err := s.Spool.Start(); err != nil {
		return fmt.Errorf("error starting opensearch spool: %w", err)
	}

	s.Writer.Start()
	return nil
}

func (s *OpenSearch) stop() {
	s.Writer.Stop()
	s.Spool.Stop()
}
