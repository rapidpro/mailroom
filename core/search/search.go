package search

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/contactql/es"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
)

// AssetMapper maps resolved assets in queries to how we identify them in ES which in the case
// of flows and groups is their ids. We can do this by just type cracking them to their models.
type AssetMapper struct{}

func (m *AssetMapper) Flow(f assets.Flow) int64 {
	return int64(f.(*models.Flow).ID())
}

func (m *AssetMapper) Group(g assets.Group) int64 {
	return int64(g.(*models.Group).ID())
}

var assetMapper = &AssetMapper{}

// BuildElasticQuery turns the passed in contact ql query into an elastic query
func BuildElasticQuery(oa *models.OrgAssets, group *models.Group, status models.ContactStatus, excludeIDs []models.ContactID, query *contactql.ContactQuery) elastic.Query {
	// filter by org and active contacts
	eq := elastic.NewBoolQuery().Must(
		elastic.NewTermQuery("org_id", oa.OrgID()),
		elastic.NewTermQuery("is_active", true),
	)

	// our group if present
	if group != nil {
		eq = eq.Must(elastic.NewTermQuery("group_ids", group.ID()))
	}

	// our status is present
	if status != models.NilContactStatus {
		eq = eq.Must(elastic.NewTermQuery("status", status))
	}

	// exclude ids if present
	if len(excludeIDs) > 0 {
		ids := make([]string, len(excludeIDs))
		for i := range excludeIDs {
			ids[i] = fmt.Sprintf("%d", excludeIDs[i])
		}
		eq = eq.MustNot(elastic.NewIdsQuery("_doc").Ids(ids...))
	}

	// and by our query if present
	if query != nil {
		q := es.ToElasticQuery(oa.Env(), assetMapper, query)
		eq = eq.Must(q)
	}

	return eq
}

// GetContactTotal returns the total count of matching contacts for the given query
func GetContactTotal(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, query string) (*contactql.ContactQuery, int64, error) {
	env := oa.Env()
	var parsed *contactql.ContactQuery
	var err error

	if rt.ES == nil {
		return nil, 0, errors.Errorf("no elastic client available, check your configuration")
	}

	if query != "" {
		parsed, err = contactql.ParseQuery(env, query, oa.SessionAssets())
		if err != nil {
			return nil, 0, errors.Wrapf(err, "error parsing query: %s", query)
		}
	}

	eq := BuildElasticQuery(oa, nil, models.NilContactStatus, nil, parsed)

	count, err := rt.ES.Count(rt.Config.ElasticContactsIndex).Routing(strconv.FormatInt(int64(oa.OrgID()), 10)).Query(eq).Do(ctx)
	if err != nil {
		// Get *elastic.Error which contains additional information
		ee, ok := err.(*elastic.Error)
		if !ok {
			return nil, 0, errors.Wrap(err, "error performing query")
		}

		return nil, 0, errors.Wrapf(err, "error performing query: %s", ee.Details.Reason)
	}

	return parsed, count, nil
}

// GetContactIDsForQueryPage returns a page of contact ids for the given query and sort
func GetContactIDsForQueryPage(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, group *models.Group, excludeIDs []models.ContactID, query string, sort string, offset int, pageSize int) (*contactql.ContactQuery, []models.ContactID, int64, error) {
	env := oa.Env()
	index := rt.Config.ElasticContactsIndex
	start := time.Now()
	var parsed *contactql.ContactQuery
	var err error

	if rt.ES == nil {
		return nil, nil, 0, errors.Errorf("no elastic client available, check your configuration")
	}

	if query != "" {
		parsed, err = contactql.ParseQuery(env, query, oa.SessionAssets())
		if err != nil {
			return nil, nil, 0, errors.Wrapf(err, "error parsing query: %s", query)
		}
	}

	eq := BuildElasticQuery(oa, group, models.NilContactStatus, excludeIDs, parsed)

	fieldSort, err := es.ToElasticFieldSort(sort, oa.SessionAssets())
	if err != nil {
		return nil, nil, 0, errors.Wrapf(err, "error parsing sort")
	}

	s := rt.ES.Search(index).TrackTotalHits(true).Routing(strconv.FormatInt(int64(oa.OrgID()), 10))
	s = s.Size(pageSize).From(offset).Query(eq).SortBy(fieldSort).FetchSource(false)

	results, err := s.Do(ctx)
	if err != nil {
		// Get *elastic.Error which contains additional information
		ee, ok := err.(*elastic.Error)
		if !ok {
			return nil, nil, 0, errors.Wrapf(err, "error performing query")
		}

		return nil, nil, 0, errors.Wrapf(err, "error performing query: %s", ee.Details.Reason)
	}

	ids := make([]models.ContactID, 0, pageSize)
	ids, err = appendIDsFromHits(ids, results.Hits.Hits)
	if err != nil {
		return nil, nil, 0, err
	}

	slog.Debug("paged contact query complete",
		"org_id", oa.OrgID(),
		"query", query,
		"elapsed", time.Since(start),
		"page_count", len(ids),
		"total_count", results.Hits.TotalHits.Value,
	)

	return parsed, ids, results.Hits.TotalHits.Value, nil
}

// GetContactIDsForQuery returns up to limit the contact ids that match the given query without sorting. Limit of -1 means return all.
func GetContactIDsForQuery(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, query string, limit int) ([]models.ContactID, error) {
	env := oa.Env()
	index := rt.Config.ElasticContactsIndex
	start := time.Now()

	if rt.ES == nil {
		return nil, errors.Errorf("no elastic client available, check your configuration")
	}

	// turn into elastic query
	parsed, err := contactql.ParseQuery(env, query, oa.SessionAssets())
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing query: %s", query)
	}

	routing := strconv.FormatInt(int64(oa.OrgID()), 10)
	eq := BuildElasticQuery(oa, nil, models.ContactStatusActive, nil, parsed)
	ids := make([]models.ContactID, 0, 100)

	// if limit provided that can be done with regular search, do that
	if limit >= 0 && limit <= 10000 {
		results, err := rt.ES.Search(index).Routing(routing).From(0).Size(limit).Query(eq).FetchSource(false).Do(ctx)
		if err != nil {
			return nil, err
		}
		return appendIDsFromHits(ids, results.Hits.Hits)
	}

	// for larger limits, use scroll service
	// note that this is no longer recommended, see https://www.elastic.co/guide/en/elasticsearch/reference/current/scroll-api.html
	scroll := rt.ES.Scroll(index).Routing(routing).KeepAlive("15m").Size(10000).Query(eq).FetchSource(false)
	for {
		results, err := scroll.Do(ctx)
		if err == io.EOF {
			slog.Debug("contact query complete",
				"org_id", oa.OrgID(),
				"query", query,
				"elapsed", time.Since(start),
				"match_count", len(ids),
			)

			return ids, nil
		}
		if err != nil {
			return nil, errors.Wrapf(err, "error scrolling through results for search: %s", query)
		}

		ids, err = appendIDsFromHits(ids, results.Hits.Hits)
		if err != nil {
			return nil, err
		}
	}
}

// utility to convert search hits to contact IDs and append them to the given slice
func appendIDsFromHits(ids []models.ContactID, hits []*elastic.SearchHit) ([]models.ContactID, error) {
	for _, hit := range hits {
		id, err := strconv.Atoi(hit.Id)
		if err != nil {
			return nil, errors.Wrapf(err, "unexpected non-integer contact id: %s", hit.Id)
		}

		ids = append(ids, models.ContactID(id))
	}
	return ids, nil
}
