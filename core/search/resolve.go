package search

import (
	"context"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
)

type Recipients struct {
	ContactIDs      []models.ContactID
	GroupIDs        []models.GroupID
	URNs            []urns.URN
	Query           string
	Exclusions      models.Exclusions
	ExcludeGroupIDs []models.GroupID
}

// ResolveRecipients resolves a set of contacts, groups, urns etc into a set of unique contacts
func ResolveRecipients(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, flow *models.Flow, recipients *Recipients, limit int) ([]models.ContactID, error) {
	// start by loading the explicitly listed contacts
	includeContacts, err := models.LoadContacts(ctx, rt.DB, oa, recipients.ContactIDs)
	if err != nil {
		return nil, err
	}

	// created contacts are handled separately because they won't be indexed
	var createdContacts map[urns.URN]*models.Contact

	// resolve any raw URNs
	if len(recipients.URNs) > 0 {
		fetchedByURN, createdByURN, err := models.GetOrCreateContactsFromURNs(ctx, rt.DB, oa, recipients.URNs)
		if err != nil {
			return nil, errors.Wrap(err, "error getting contact ids from urns")
		}
		for _, c := range fetchedByURN {
			includeContacts = append(includeContacts, c)
		}

		createdContacts = createdByURN
	}

	includeGroups := make([]*models.Group, len(recipients.GroupIDs))
	excludeGroups := make([]*models.Group, len(recipients.ExcludeGroupIDs))

	for i, groupID := range recipients.GroupIDs {
		group := oa.GroupByID(groupID)
		if group != nil {
			includeGroups[i] = group
		}
	}
	for i, groupID := range recipients.ExcludeGroupIDs {
		group := oa.GroupByID(groupID)
		if group != nil {
			excludeGroups[i] = group
		}
	}

	// reduce contacts to unique UUIDs
	includeContactUUIDs := make(map[flows.ContactUUID]bool, len(includeContacts))
	for _, contact := range includeContacts {
		includeContactUUIDs[contact.UUID()] = true
	}

	var matches []models.ContactID

	if len(includeContactUUIDs) > 0 || len(includeGroups) > 0 || recipients.Query != "" {
		query, err := BuildStartQuery(oa, flow, includeGroups, maps.Keys(includeContactUUIDs), recipients.Query, recipients.Exclusions, excludeGroups)
		if err != nil {
			return nil, errors.Wrap(err, "error building query")
		}

		matches, err = GetContactIDsForQuery(ctx, rt, oa, query, limit)
		if err != nil {
			return nil, errors.Wrap(err, "error performing contact search")
		}
	}

	// only add created contacts if not excluding contacts based on last seen - other exclusions can't apply to a newly
	// created contact
	if recipients.Exclusions.NotSeenSinceDays == 0 {
		for _, c := range createdContacts {
			matches = append(matches, c.ID())
		}
	}

	return matches, nil
}
