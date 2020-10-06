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

	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// BuildElasticQuery turns the passed in contact ql query into an elastic query
func BuildElasticQuery(org *OrgAssets, group assets.GroupUUID, status ContactStatus, excludeIDs []ContactID, query *contactql.ContactQuery) elastic.Query {
	// filter by org and active contacts
	eq := elastic.NewBoolQuery().Must(
		elastic.NewTermQuery("org_id", org.OrgID()),
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
		q := es.ToElasticQuery(org.Env(), query)
		eq = eq.Must(q)
	}

	return eq
}

// ContactIDsForQueryPage returns the ids of the contacts for the passed in query page
func ContactIDsForQueryPage(ctx context.Context, client *elastic.Client, org *OrgAssets, group assets.GroupUUID, excludeIDs []ContactID, query string, sort string, offset int, pageSize int) (*contactql.ContactQuery, []ContactID, int64, error) {
	env := org.Env()
	start := time.Now()
	var parsed *contactql.ContactQuery
	var err error

	if client == nil {
		return nil, nil, 0, errors.Errorf("no elastic client available, check your configuration")
	}

	if query != "" {
		parsed, err = contactql.ParseQuery(env, query, org.SessionAssets())
		if err != nil {
			return nil, nil, 0, errors.Wrapf(err, "error parsing query: %s", query)
		}
	}

	eq := BuildElasticQuery(org, group, NilContactStatus, excludeIDs, parsed)

	fieldSort, err := es.ToElasticFieldSort(sort, org.SessionAssets())
	if err != nil {
		return nil, nil, 0, errors.Wrapf(err, "error parsing sort")
	}

	s := client.Search("contacts").Routing(strconv.FormatInt(int64(org.OrgID()), 10))
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
		"org_id":      org.OrgID(),
		"parsed":      parsed,
		"group_uuid":  group,
		"query":       query,
		"elapsed":     time.Since(start),
		"page_count":  len(ids),
		"total_count": results.Hits.TotalHits,
	}).Debug("paged contact query complete")

	return parsed, ids, results.Hits.TotalHits, nil
}

// ContactIDsForQuery returns the ids of all the contacts that match the passed in query
func ContactIDsForQuery(ctx context.Context, client *elastic.Client, org *OrgAssets, query string) ([]ContactID, error) {
	env := org.Env()
	start := time.Now()

	if client == nil {
		return nil, errors.Errorf("no elastic client available, check your configuration")
	}

	// turn into elastic query
	parsed, err := contactql.ParseQuery(env, query, org.SessionAssets())
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing query: %s", query)
	}

	eq := BuildElasticQuery(org, "", ContactStatusActive, nil, parsed)

	ids := make([]ContactID, 0, 100)

	// iterate across our results, building up our contact ids
	scroll := client.Scroll("contacts").Routing(strconv.FormatInt(int64(org.OrgID()), 10))
	scroll = scroll.KeepAlive("15m").Size(10000).Query(eq).FetchSource(false)
	for {
		results, err := scroll.Do(ctx)
		if err == io.EOF {
			logrus.WithFields(logrus.Fields{
				"org_id":      org.OrgID(),
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
