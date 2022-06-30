package search

import (
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/pkg/errors"
)

// Exclusions are preset exclusion conditions
type Exclusions struct {
	NonActive         bool `json:"non_active"`          // contacts who are blocked, stopped or archived
	InAFlow           bool `json:"in_a_flow"`           // contacts who are currently in a flow (including this one)
	StartedPreviously bool `json:"started_previously"`  // contacts who have been in this flow in the last 90 days
	NotSeenSinceDays  int  `json:"not_seen_since_days"` // contacts who have not been seen for more than this number of days
}

// BuildStartQuery builds a start query for the given flow and start options
func BuildStartQuery(oa *models.OrgAssets, flow *models.Flow, groups []*models.Group, contactUUIDs []flows.ContactUUID, urnz []urns.URN, userQuery string, excs Exclusions) (string, error) {
	var parsedQuery *contactql.ContactQuery
	var err error

	if userQuery != "" {
		parsedQuery, err = contactql.ParseQuery(oa.Env(), userQuery, oa.SessionAssets())
		if err != nil {
			return "", errors.Wrap(err, "invalid user query")
		}
	}

	return contactql.Stringify(buildStartQuery(oa.Env(), flow, groups, contactUUIDs, urnz, parsedQuery, excs)), nil
}

func buildStartQuery(env envs.Environment, flow *models.Flow, groups []*models.Group, contactUUIDs []flows.ContactUUID, urnz []urns.URN, userQuery *contactql.ContactQuery, excs Exclusions) contactql.QueryNode {
	inclusions := make([]contactql.QueryNode, 0, 10)

	for _, group := range groups {
		inclusions = append(inclusions, contactql.NewCondition("group", contactql.PropertyTypeAttribute, contactql.OpEqual, group.Name()))
	}
	for _, contactUUID := range contactUUIDs {
		inclusions = append(inclusions, contactql.NewCondition("uuid", contactql.PropertyTypeAttribute, contactql.OpEqual, string(contactUUID)))
	}
	for _, urn := range urnz {
		scheme, path, _, _ := urn.ToParts()
		inclusions = append(inclusions, contactql.NewCondition(scheme, contactql.PropertyTypeScheme, contactql.OpEqual, path))
	}
	if userQuery != nil {
		inclusions = append(inclusions, userQuery.Root())
	}

	exclusions := make([]contactql.QueryNode, 0, 10)
	if excs.NonActive {
		exclusions = append(exclusions, contactql.NewCondition("status", contactql.PropertyTypeAttribute, contactql.OpEqual, "active"))
	}
	if excs.InAFlow {
		exclusions = append(exclusions, contactql.NewCondition("flow", contactql.PropertyTypeAttribute, contactql.OpEqual, ""))
	}
	if excs.StartedPreviously {
		exclusions = append(exclusions, contactql.NewCondition("history", contactql.PropertyTypeAttribute, contactql.OpNotEqual, flow.Name()))
	}
	if excs.NotSeenSinceDays > 0 {
		seenSince := dates.Now().Add(-time.Hour * time.Duration(24*excs.NotSeenSinceDays))
		exclusions = append(exclusions, contactql.NewCondition("last_seen_on", contactql.PropertyTypeAttribute, contactql.OpGreaterThan, formatQueryDate(env, seenSince)))
	}

	return contactql.NewBoolCombination(contactql.BoolOperatorAnd,
		contactql.NewBoolCombination(contactql.BoolOperatorOr, inclusions...),
		contactql.NewBoolCombination(contactql.BoolOperatorAnd, exclusions...),
	).Simplify()
}

// formats a date for use in a query
func formatQueryDate(env envs.Environment, t time.Time) string {
	d := dates.ExtractDate(t.In(env.Timezone()))
	s, _ := d.Format(string(env.DateFormat()), env.DefaultLocale().ToBCP47())
	return s
}
