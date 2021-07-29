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
func BuildElasticQuery(oa *OrgAssets, group assets.GroupUUID, status ContactStatus, excludeIDs []ContactID, query *contactql.ContactQuery) (elastic.Query, error) {
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
		q, err := es.ToElasticQuery(oa.Env(), oa.SessionAssets(), query)
		if err != nil {
			return nil, errors.Wrap(err, "error translating query to elastic")
		}

		eq = eq.Must(q)
	}

	return eq, nil
}

// ContactIDsForQueryPage returns the ids of the contacts for the passed in query page
func ContactIDsForQueryPage(ctx context.Context, client *elastic.Client, oa *OrgAssets, group assets.GroupUUID, excludeIDs []ContactID, query string, sort string, offset int, pageSize int) (*contactql.ContactQuery, []ContactID, int64, error) {
	if client == nil {
		return nil, nil, 0, errors.Errorf("no elastic client available, check your configuration")
	}

	start := time.Now()
	var parsed *contactql.ContactQuery
	var err error

	if query != "" {
		parsed, err = parseAndValidateQuery(oa, query)
		if err != nil {
			return nil, nil, 0, errors.Wrapf(err, "error parsing query: %s", query)
		}
	}

	// turn into elastic query
	eq, err := BuildElasticQuery(oa, group, NilContactStatus, excludeIDs, parsed)
	if err != nil {
		return nil, nil, 0, errors.Wrapf(err, "error building elastic query: %s", query)
	}

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
	for _, hit := range results.Hits.Hits {
		id, err := strconv.Atoi(hit.Id)
		if err != nil {
			return nil, nil, 0, errors.Wrapf(err, "unexpected non-integer contact id: %s for search: %s", hit.Id, query)
		}
		ids = append(ids, ContactID(id))
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

// ContactIDsForQuery returns the ids of all the contacts that match the passed in query
func ContactIDsForQuery(ctx context.Context, client *elastic.Client, oa *OrgAssets, query string) ([]ContactID, error) {
	start := time.Now()

	if client == nil {
		return nil, errors.Errorf("no elastic client available, check your configuration")
	}

	parsed, err := parseAndValidateQuery(oa, query)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing query: %s", query)
	}

	// turn into elastic query
	eq, err := BuildElasticQuery(oa, "", ContactStatusActive, nil, parsed)
	if err != nil {
		return nil, errors.Wrapf(err, "error building elastic query: %s", query)
	}

	ids := make([]ContactID, 0, 100)

	// iterate across our results, building up our contact ids
	scroll := client.Scroll("contacts").Routing(strconv.FormatInt(int64(oa.OrgID()), 10))
	scroll = scroll.KeepAlive("15m").Size(10000).Query(eq).FetchSource(false)
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

		for _, hit := range results.Hits.Hits {
			id, err := strconv.Atoi(hit.Id)
			if err != nil {
				return nil, errors.Wrapf(err, "unexpected non-integer contact id: %s for search: %s", hit.Id, query)
			}

			ids = append(ids, ContactID(id))
		}
	}
}

func parseAndValidateQuery(oa *OrgAssets, query string) (*contactql.ContactQuery, error) {
	parsed, err := contactql.ParseQuery(oa.Env(), query)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing query: %s", query)
	}

	err = parsed.Validate(oa.Env(), oa.SessionAssets())
	if err != nil {
		return nil, err
	}

	return parsed, nil
}
