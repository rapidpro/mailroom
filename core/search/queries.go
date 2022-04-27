package search

import (
	"fmt"
	"strings"
	"time"

	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
)

// Exclusions are preset exclusion conditions
type Exclusions struct {
	NonActive         bool `json:"non_active"`          // contacts who are blocked, stopped or archived
	InAFlow           bool `json:"in_a_flow"`           // contacts who are currently in a flow (including this one)
	StartedPreviously bool `json:"started_previously"`  // contacts who have been in this flow in the last 90 days
	NotSeenSinceDays  int  `json:"not_seen_since_days"` // contacts who have not been seen for more than this number of days
}

// BuildStartQuery builds a start query for the given flow and start options
func BuildStartQuery(env envs.Environment, flow *models.Flow, groups []*models.Group, contactUUIDs []flows.ContactUUID, urnz []urns.URN, userQuery string, excs Exclusions) string {
	inclusions := make([]string, 0, 10)

	for _, group := range groups {
		inclusions = append(inclusions, fmt.Sprintf("group = \"%s\"", group.Name()))
	}
	for _, contactUUID := range contactUUIDs {
		inclusions = append(inclusions, fmt.Sprintf("uuid = \"%s\"", contactUUID))
	}
	for _, urn := range urnz {
		scheme, path, _, _ := urn.ToParts()
		inclusions = append(inclusions, fmt.Sprintf("%s = \"%s\"", scheme, path))
	}
	if userQuery != "" {
		if len(inclusions) > 0 {
			userQuery = fmt.Sprintf("(%s)", userQuery)
		}
		inclusions = append(inclusions, userQuery)
	}

	exclusions := make([]string, 0, 10)
	if excs.NonActive {
		exclusions = append(exclusions, "status = \"active\"")
	}
	if excs.InAFlow {
		exclusions = append(exclusions, "flow = \"\"")
	}
	if excs.StartedPreviously {
		exclusions = append(exclusions, fmt.Sprintf("history != \"%s\"", flow.Name()))
	}
	if excs.NotSeenSinceDays > 0 {
		seenSince := dates.Now().Add(-time.Hour * time.Duration(24*excs.NotSeenSinceDays))
		exclusions = append(exclusions, fmt.Sprintf("last_seen_on > %s", formatQueryDate(env, seenSince)))
	}

	inclusionCmp := strings.Join(inclusions, " OR ")
	exclusionCmp := strings.Join(exclusions, " AND ")

	if len(inclusions) > 1 && len(exclusions) > 0 {
		inclusionCmp = fmt.Sprintf("(%s)", inclusionCmp)
	}

	conditions := make([]string, 0, 2)
	if inclusionCmp != "" {
		conditions = append(conditions, inclusionCmp)
	}
	if exclusionCmp != "" {
		conditions = append(conditions, exclusionCmp)
	}
	return strings.Join(conditions, " AND ")
}

// formats a date for use in a query
func formatQueryDate(env envs.Environment, t time.Time) string {
	d := dates.ExtractDate(t.In(env.Timezone()))
	s, _ := d.Format(string(env.DateFormat()), env.DefaultLocale().ToBCP47())
	return s
}
