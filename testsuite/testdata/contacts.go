package testdata

import (
	"context"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null/v2"
)

type Contact struct {
	ID    models.ContactID
	UUID  flows.ContactUUID
	URN   urns.URN
	URNID models.URNID
}

func (c *Contact) Load(rt *runtime.Runtime, oa *models.OrgAssets) (*models.Contact, *flows.Contact) {
	contacts, err := models.LoadContacts(context.Background(), rt.DB, oa, []models.ContactID{c.ID})
	must(err, len(contacts) == 1)

	flowContact, err := contacts[0].FlowContact(oa)
	must(err)

	return contacts[0], flowContact
}

type Group struct {
	ID   models.GroupID
	UUID assets.GroupUUID
}

func (g *Group) Add(rt *runtime.Runtime, contacts ...*Contact) {
	for _, c := range contacts {
		rt.DB.MustExec(`INSERT INTO contacts_contactgroup_contacts(contactgroup_id, contact_id) VALUES($1, $2)`, g.ID, c.ID)
	}
}

type Field struct {
	ID   models.FieldID
	UUID assets.FieldUUID
}

// InsertContact inserts a contact
func InsertContact(rt *runtime.Runtime, org *Org, uuid flows.ContactUUID, name string, language envs.Language, status models.ContactStatus) *Contact {
	var id models.ContactID
	must(rt.DB.Get(&id,
		`INSERT INTO contacts_contact (org_id, is_active, ticket_count, uuid, name, language, status, created_on, modified_on, created_by_id, modified_by_id) 
		VALUES($1, TRUE, 0, $2, $3, $4, $5, NOW(), NOW(), 1, 1) RETURNING id`, org.ID, uuid, name, language, status,
	))
	return &Contact{id, uuid, "", models.NilURNID}
}

// InsertContactGroup inserts a contact group
func InsertContactGroup(rt *runtime.Runtime, org *Org, uuid assets.GroupUUID, name, query string, contacts ...*Contact) *Group {
	groupType := "M"
	if query != "" {
		groupType = "Q"
	}

	var id models.GroupID
	must(rt.DB.Get(&id,
		`INSERT INTO contacts_contactgroup(uuid, org_id, group_type, name, query, status, is_system, is_active, created_by_id, created_on, modified_by_id, modified_on) 
		 VALUES($1, $2, $3, $4, $5, 'R', FALSE, TRUE, 1, NOW(), 1, NOW()) RETURNING id`, uuid, org.ID, groupType, name, null.String(query),
	))

	for _, contact := range contacts {
		rt.DB.MustExec(`INSERT INTO contacts_contactgroup_contacts(contactgroup_id, contact_id) VALUES($1, $2)`, id, contact.ID)
		rt.DB.MustExec(`UPDATE contacts_contact SET modified_on = NOW() WHERE id = $1`, contact.ID)
	}

	return &Group{id, uuid}
}

// InsertContactURN inserts a contact URN
func InsertContactURN(rt *runtime.Runtime, org *Org, contact *Contact, urn urns.URN, priority int) models.URNID {
	scheme, path, _, display := urn.ToParts()

	contactID := models.NilContactID
	if contact != nil {
		contactID = contact.ID
	}

	var id models.URNID
	must(rt.DB.Get(&id,
		`INSERT INTO contacts_contacturn(org_id, contact_id, scheme, path, display, identity, priority) 
		 VALUES($1, $2, $3, $4, $5, $6, $7) RETURNING id`, org.ID, contactID, scheme, path, display, urn.Identity(), priority,
	))
	return id
}
