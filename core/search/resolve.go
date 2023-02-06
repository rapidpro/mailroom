package search

import (
	"context"

	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/urns"
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
	QueryLimit      int
	ExcludeGroupIDs []models.GroupID
}

// ResolveRecipients resolves a set of contacts, groups, urns etc into a set of unique contacts
func ResolveRecipients(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, recipients *Recipients) ([]models.ContactID, []models.ContactID, error) {
	allIDs := make(map[models.ContactID]bool) // ids of all contacts we're including
	createdIDs := make([]models.ContactID, 0) // ids of new contacts we've created

	// we are building a set of contact ids, start with the explicit ones
	for _, id := range recipients.ContactIDs {
		allIDs[id] = true
	}

	// resolve any raw URNs
	if len(recipients.URNs) > 0 {
		urnContactIDs, err := models.GetOrCreateContactIDsFromURNs(ctx, rt.DB, oa, recipients.URNs)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error getting contact ids from urns")
		}
		for _, id := range urnContactIDs {
			if !allIDs[id] {
				createdIDs = append(createdIDs, id)
			}
			allIDs[id] = true
		}
	}

	// if we have inclusion groups, add all the contact ids from those groups
	if len(recipients.GroupIDs) > 0 {
		rows, err := rt.DB.QueryxContext(ctx, `SELECT contact_id FROM contacts_contactgroup_contacts WHERE contactgroup_id = ANY($1)`, pq.Array(recipients.GroupIDs))
		if err != nil {
			return nil, nil, errors.Wrap(err, "error querying contacts from inclusion groups")
		}
		defer rows.Close()

		var contactID models.ContactID
		for rows.Next() {
			err := rows.Scan(&contactID)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error scanning contact id")
			}
			allIDs[contactID] = true
		}
	}

	// if we have a query, add the contacts that match that as well
	if recipients.Query != "" {
		matches, err := GetContactIDsForQuery(ctx, rt.ES, oa, recipients.Query, recipients.QueryLimit)
		if err != nil {
			return nil, nil, errors.Wrap(err, "error performing contact search")
		}

		for _, contactID := range matches {
			allIDs[contactID] = true
		}
	}

	// finally, if we have exclusion groups, remove all the contact ids from those groups
	if len(recipients.ExcludeGroupIDs) > 0 {
		rows, err := rt.DB.QueryxContext(ctx, `SELECT contact_id FROM contacts_contactgroup_contacts WHERE contactgroup_id = ANY($1)`, pq.Array(recipients.ExcludeGroupIDs))
		if err != nil {
			return nil, nil, errors.Wrap(err, "error querying contacts from exclusion groups")
		}
		defer rows.Close()

		var excludeID models.ContactID
		for rows.Next() {
			err := rows.Scan(&excludeID)
			if err != nil {
				return nil, nil, errors.Wrap(err, "error scanning contact id")
			}
			delete(allIDs, excludeID)
		}
	}

	return maps.Keys(allIDs), createdIDs, nil
}
