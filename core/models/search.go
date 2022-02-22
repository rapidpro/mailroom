package models

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/contactql/es"

	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// BuildElasticQuery turns the passed in contact ql query into an elastic query
func BuildElasticQuery(oa *OrgAssets, group assets.GroupUUID, status ContactStatus, excludeIDs []ContactID, query *contactql.ContactQuery) elastic.Query {
	// filter by org and active contacts
	eq := elastic.NewBoolQuery().Must(
		elastic.NewTermQuery("org_id", oa.OrgID()),
		elastic.NewTermQuery("is_active", true),
	)

	// our group if present
	if group != "" {
		eq = eq.Must(elastic.NewTermQuery("groups", group))
	}

	// our status is present
	if status != NilContactStatus {
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
		q := es.ToElasticQuery(oa.Env(), query)
		eq = eq.Must(q)
	}

	return eq
}

// GetContactIDsForQueryPage returns a page of contact ids for the given query and sort
func GetContactIDsForQueryPage(ctx context.Context, client *elastic.Client, oa *OrgAssets, group assets.GroupUUID, excludeIDs []ContactID, query string, sort string, offset int, pageSize int) (*contactql.ContactQuery, []ContactID, int64, error) {
	env := oa.Env()
	start := time.Now()
	var parsed *contactql.ContactQuery
	var err error

	if client == nil {
		return nil, nil, 0, errors.Errorf("no elastic client available, check your configuration")
	}

	if query != "" {
		parsed, err = contactql.ParseQuery(env, query, oa.SessionAssets())
		if err != nil {
			return nil, nil, 0, errors.Wrapf(err, "error parsing query: %s", query)
		}
	}

	eq := BuildElasticQuery(oa, group, NilContactStatus, excludeIDs, parsed)

	fieldSort, err := es.ToElasticFieldSort(sort, oa.SessionAssets())
	if err != nil {
		return nil, nil, 0, errors.Wrapf(err, "error parsing sort")
	}

	s := client.Search("contacts").TrackTotalHits(true).Routing(strconv.FormatInt(int64(oa.OrgID()), 10))
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

	ids := make([]ContactID, 0, pageSize)
	ids, err = appendIDsFromHits(ids, results.Hits.Hits)
	if err != nil {
		return nil, nil, 0, err
	}

	logrus.WithFields(logrus.Fields{
		"org_id":      oa.OrgID(),
		"parsed":      parsed,
		"group_uuid":  group,
		"query":       query,
		"elapsed":     time.Since(start),
		"page_count":  len(ids),
		"total_count": results.Hits.TotalHits,
	}).Debug("paged contact query complete")

	return parsed, ids, results.Hits.TotalHits.Value, nil
}

// GetContactIDsForQuery returns up to limit the contact ids that match the given query without sorting. Limit of -1 means return all.
func GetContactIDsForQuery(ctx context.Context, client *elastic.Client, oa *OrgAssets, query string, limit int) ([]ContactID, error) {
	env := oa.Env()
	start := time.Now()

	if client == nil {
		return nil, errors.Errorf("no elastic client available, check your configuration")
	}

	// turn into elastic query
	parsed, err := contactql.ParseQuery(env, query, oa.SessionAssets())
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing query: %s", query)
	}

	routing := strconv.FormatInt(int64(oa.OrgID()), 10)
	eq := BuildElasticQuery(oa, "", ContactStatusActive, nil, parsed)
	ids := make([]ContactID, 0, 100)

	// if limit provided that can be done with regular search, do that
	if limit >= 0 && limit <= 10000 {
		results, err := client.Search("contacts").Routing(routing).From(0).Size(limit).Query(eq).FetchSource(false).Do(ctx)
		if err != nil {
			return nil, err
		}
		return appendIDsFromHits(ids, results.Hits.Hits)
	}

	// for larger limits, use scroll service
	// note that this is no longer recommended, see https://www.elastic.co/guide/en/elasticsearch/reference/current/scroll-api.html
	scroll := client.Scroll("contacts").Routing(routing).KeepAlive("15m").Size(10000).Query(eq).FetchSource(false)
	for {
		results, err := scroll.Do(ctx)
		if err == io.EOF {
			logrus.WithFields(logrus.Fields{
				"org_id":      oa.OrgID(),
				"query":       query,
				"elapsed":     time.Since(start),
				"match_count": len(ids),
			}).Debug("contact query complete")

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
func appendIDsFromHits(ids []ContactID, hits []*elastic.SearchHit) ([]ContactID, error) {
	for _, hit := range hits {
		id, err := strconv.Atoi(hit.Id)
		if err != nil {
			return nil, errors.Wrapf(err, "unexpected non-integer contact id: %s", hit.Id)
		}

		ids = append(ids, ContactID(id))
	}
	return ids, nil
}
