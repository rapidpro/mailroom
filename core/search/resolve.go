package search

import (
	"context"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
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
	idsSeen := make(map[models.ContactID]bool)

	// start by loading the explicitly listed contacts
	includeContacts, err := models.LoadContacts(ctx, rt.DB, oa, recipients.ContactIDs)
	if err != nil {
		return nil, err
	}
	for _, c := range includeContacts {
		idsSeen[c.ID()] = true
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
			if !idsSeen[c.ID()] {
				includeContacts = append(includeContacts, c)
				idsSeen[c.ID()] = true
			}
		}

		createdContacts = createdByURN
	}

	includeGroups := make([]*models.Group, 0, len(recipients.GroupIDs))
	excludeGroups := make([]*models.Group, 0, len(recipients.ExcludeGroupIDs))

	for _, groupID := range recipients.GroupIDs {
		group := oa.GroupByID(groupID)
		if group != nil {
			includeGroups = append(includeGroups, group)
		}
	}
	for _, groupID := range recipients.ExcludeGroupIDs {
		group := oa.GroupByID(groupID)
		if group != nil {
			excludeGroups = append(excludeGroups, group)
		}
	}

	var matches []models.ContactID

	// if we're only including individual contacts and there are no exclusions, we can just return those contacts
	if len(includeGroups) == 0 && recipients.Query == "" && recipients.Exclusions == models.NoExclusions && len(excludeGroups) == 0 {
		matches := make([]models.ContactID, 0, len(includeContacts)+len(createdContacts))
		for _, c := range includeContacts {
			matches = append(matches, c.ID())
		}
		for _, c := range createdContacts {
			matches = append(matches, c.ID())
		}
		return matches, nil
	}

	if len(includeContacts) > 0 || len(includeGroups) > 0 || recipients.Query != "" {
		// reduce contacts to UUIDs
		includeContactUUIDs := make([]flows.ContactUUID, len(includeContacts))
		for i, contact := range includeContacts {
			includeContactUUIDs[i] = contact.UUID()
		}

		query, err := BuildStartQuery(oa, flow, includeGroups, includeContactUUIDs, recipients.Query, recipients.Exclusions, excludeGroups)
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
