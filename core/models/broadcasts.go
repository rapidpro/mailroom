package models

import (
	"context"
	"time"

	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/excellent"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null/v3"
	"github.com/pkg/errors"
)

// BroadcastID is our internal type for broadcast ids, which can be null/0
type BroadcastID int

// NilBroadcastID is our constant for a nil broadcast id
const NilBroadcastID = BroadcastID(0)

// TemplateState represents what state are templates are in, either already evaluated or unevaluated
type TemplateState string

const (
	TemplateStateEvaluated   = TemplateState("evaluated")
	TemplateStateUnevaluated = TemplateState("unevaluated")
)

// Broadcast represents a broadcast that needs to be sent
type Broadcast struct {
	ID            BroadcastID                 `json:"broadcast_id,omitempty"`
	OrgID         OrgID                       `json:"org_id"`
	Translations  flows.BroadcastTranslations `json:"translations"`
	TemplateState TemplateState               `json:"template_state"`
	BaseLanguage  i18n.Language               `json:"base_language"`
	OptInID       OptInID                     `json:"optin_id"`
	URNs          []urns.URN                  `json:"urns,omitempty"`
	ContactIDs    []ContactID                 `json:"contact_ids,omitempty"`
	GroupIDs      []GroupID                   `json:"group_ids,omitempty"`
	Query         string                      `json:"query,omitempty"`
	CreatedByID   UserID                      `json:"created_by_id,omitempty"`
	ParentID      BroadcastID                 `json:"parent_id,omitempty"`
}

type dbBroadcast struct {
	ID           BroadcastID                 `db:"id"`
	OrgID        OrgID                       `db:"org_id"`
	Translations flows.BroadcastTranslations `db:"translations"`
	BaseLanguage i18n.Language               `db:"base_language"`
	OptInID      OptInID                     `db:"optin_id"`
	URNs         pq.StringArray              `db:"urns"`
	Query        null.String                 `db:"query"`
	CreatedByID  UserID                      `db:"created_by_id"`
	ParentID     BroadcastID                 `db:"parent_id"`
}

// NewBroadcast creates a new broadcast with the passed in parameters
func NewBroadcast(orgID OrgID, translations flows.BroadcastTranslations,
	state TemplateState, baseLanguage i18n.Language, optInID OptInID, urns []urns.URN, contactIDs []ContactID, groupIDs []GroupID, query string, createdByID UserID) *Broadcast {

	return &Broadcast{
		OrgID:         orgID,
		Translations:  translations,
		TemplateState: state,
		BaseLanguage:  baseLanguage,
		OptInID:       optInID,
		URNs:          urns,
		ContactIDs:    contactIDs,
		GroupIDs:      groupIDs,
		Query:         query,
		CreatedByID:   createdByID,
	}
}

// NewBroadcastFromEvent creates a broadcast object from the passed in broadcast event
func NewBroadcastFromEvent(ctx context.Context, tx DBorTx, oa *OrgAssets, event *events.BroadcastCreatedEvent) (*Broadcast, error) {
	// resolve our contact references
	contactIDs, err := GetContactIDsFromReferences(ctx, tx, oa.OrgID(), event.Contacts)
	if err != nil {
		return nil, errors.Wrapf(err, "error resolving contact references")
	}

	// and our groups
	groupIDs := make([]GroupID, 0, len(event.Groups))
	for i := range event.Groups {
		group := oa.GroupByUUID(event.Groups[i].UUID)
		if group != nil {
			groupIDs = append(groupIDs, group.ID())
		}
	}

	return NewBroadcast(oa.OrgID(), event.Translations, TemplateStateEvaluated, event.BaseLanguage, NilOptInID, event.URNs, contactIDs, groupIDs, event.ContactQuery, NilUserID), nil
}

func (b *Broadcast) CreateBatch(contactIDs []ContactID, isLast bool) *BroadcastBatch {
	return &BroadcastBatch{
		BroadcastID:   b.ID,
		OrgID:         b.OrgID,
		BaseLanguage:  b.BaseLanguage,
		Translations:  b.Translations,
		TemplateState: b.TemplateState,
		OptInID:       b.OptInID,
		CreatedByID:   b.CreatedByID,
		ContactIDs:    contactIDs,
		IsLast:        isLast,
	}
}

// MarkBroadcastSent marks the given broadcast as sent
func MarkBroadcastSent(ctx context.Context, db DBorTx, id BroadcastID) error {
	_, err := db.ExecContext(ctx, `UPDATE msgs_broadcast SET status = 'S', modified_on = now() WHERE id = $1`, id)
	return errors.Wrapf(err, "error marking broadcast #%d as sent", id)
}

// MarkBroadcastFailed marks the given broadcast as failed
func MarkBroadcastFailed(ctx context.Context, db DBorTx, id BroadcastID) error {
	_, err := db.ExecContext(ctx, `UPDATE msgs_broadcast SET status = 'S', modified_on = now() WHERE id = $1`, id)
	return errors.Wrapf(err, "error marking broadcast #%d as failed", id)
}

// InsertBroadcast inserts the given broadcast into the DB
func InsertBroadcast(ctx context.Context, db DBorTx, bcast *Broadcast) error {
	ua := make(pq.StringArray, len(bcast.URNs))
	for i := range bcast.URNs {
		ua[i] = string(bcast.URNs[i])
	}
	dbb := &dbBroadcast{
		ID:           bcast.ID,
		OrgID:        bcast.OrgID,
		Translations: bcast.Translations,
		BaseLanguage: bcast.BaseLanguage,
		OptInID:      bcast.OptInID,
		URNs:         ua,
		Query:        null.String(bcast.Query),
		CreatedByID:  bcast.CreatedByID,
		ParentID:     bcast.ParentID,
	}

	err := BulkQuery(ctx, "inserting broadcast", db, sqlInsertBroadcast, []*dbBroadcast{dbb})
	if err != nil {
		return errors.Wrap(err, "error inserting broadcast")
	}

	bcast.ID = dbb.ID

	// build up all our contact associations
	contacts := make([]*broadcastContact, 0, len(bcast.ContactIDs))
	for _, contactID := range bcast.ContactIDs {
		contacts = append(contacts, &broadcastContact{BroadcastID: bcast.ID, ContactID: contactID})
	}

	// insert our contacts
	err = BulkQuery(ctx, "inserting broadcast contacts", db, sqlInsertBroadcastContacts, contacts)
	if err != nil {
		return errors.Wrapf(err, "error inserting contacts for broadcast")
	}

	// build up all our group associations
	groups := make([]*broadcastGroup, 0, len(bcast.GroupIDs))
	for _, groupID := range bcast.GroupIDs {
		groups = append(groups, &broadcastGroup{BroadcastID: bcast.ID, GroupID: groupID})
	}

	// insert our groups
	err = BulkQuery(ctx, "inserting broadcast groups", db, sqlInsertBroadcastGroups, groups)
	if err != nil {
		return errors.Wrapf(err, "error inserting groups for broadcast")
	}

	return nil
}

// InsertChildBroadcast clones the passed in broadcast as a parent, then inserts that broadcast into the DB
func InsertChildBroadcast(ctx context.Context, db DBorTx, parent *Broadcast) (*Broadcast, error) {
	child := NewBroadcast(
		parent.OrgID,
		parent.Translations,
		parent.TemplateState,
		parent.BaseLanguage,
		parent.OptInID,
		parent.URNs,
		parent.ContactIDs,
		parent.GroupIDs,
		parent.Query,
		parent.CreatedByID,
	)
	child.ParentID = parent.ID

	return child, InsertBroadcast(ctx, db, child)
}

type broadcastContact struct {
	BroadcastID BroadcastID `db:"broadcast_id"`
	ContactID   ContactID   `db:"contact_id"`
}

type broadcastGroup struct {
	BroadcastID BroadcastID `db:"broadcast_id"`
	GroupID     GroupID     `db:"contactgroup_id"`
}

const sqlInsertBroadcast = `
INSERT INTO
	msgs_broadcast( org_id,  parent_id, created_on, modified_on, status,  translations,  base_language,  urns,  optin_id,  query, is_active)
			VALUES(:org_id, :parent_id, NOW()     , NOW(),       'Q',    :translations, :base_language, :urns, :optin_id, :query,      TRUE)
RETURNING id`

const sqlInsertBroadcastContacts = `INSERT INTO msgs_broadcast_contacts(broadcast_id, contact_id) VALUES(:broadcast_id, :contact_id)`
const sqlInsertBroadcastGroups = `INSERT INTO msgs_broadcast_groups(broadcast_id, contactgroup_id) VALUES(:broadcast_id, :contactgroup_id)`

// BroadcastBatch represents a batch of contacts that need messages sent for
type BroadcastBatch struct {
	BroadcastID   BroadcastID                 `json:"broadcast_id,omitempty"`
	OrgID         OrgID                       `json:"org_id"`
	Translations  flows.BroadcastTranslations `json:"translations"`
	BaseLanguage  i18n.Language               `json:"base_language"`
	TemplateState TemplateState               `json:"template_state"`
	OptInID       OptInID                     `json:"optin_id"`
	ContactIDs    []ContactID                 `json:"contact_ids,omitempty"`
	CreatedByID   UserID                      `json:"created_by_id"`
	IsLast        bool                        `json:"is_last"`
}

func (b *BroadcastBatch) CreateMessages(ctx context.Context, rt *runtime.Runtime, oa *OrgAssets) ([]*Msg, error) {
	// load all our contacts
	contacts, err := LoadContacts(ctx, rt.DB, oa, b.ContactIDs)
	if err != nil {
		return nil, errors.Wrap(err, "error loading contacts for broadcast")
	}

	// for each contact, build our message
	msgs := make([]*Msg, 0, len(contacts))

	// run through all our contacts to create our messages
	for _, c := range contacts {
		msg, err := b.createMessage(rt, oa, c)
		if err != nil {
			return nil, errors.Wrap(err, "error creating broadcast message")
		}
		if msg != nil {
			msgs = append(msgs, msg)
		}
	}

	// insert them in a single request
	err = InsertMessages(ctx, rt.DB, msgs)
	if err != nil {
		return nil, errors.Wrap(err, "error inserting broadcast messages")
	}

	return msgs, nil
}

// creates an outgoing message for the given contact - can return nil if resultant message has no content and thus is a noop
func (b *BroadcastBatch) createMessage(rt *runtime.Runtime, oa *OrgAssets, c *Contact) (*Msg, error) {
	contact, err := c.FlowContact(oa)
	if err != nil {
		return nil, errors.Wrap(err, "error creating flow contact for broadcast message")
	}

	trans, lang := b.Translations.ForContact(oa.Env(), contact, b.BaseLanguage)
	if trans == nil {
		// in theory shoud never happen because we shouldn't save a broadcast like this
		return nil, errors.New("broadcast has no translation in base language")
	}

	text := trans.Text
	attachments := trans.Attachments
	quickReplies := trans.QuickReplies
	locale := i18n.NewLocale(lang, i18n.NilCountry)

	if b.TemplateState == TemplateStateUnevaluated {
		// build up the minimum viable context for templates
		templateCtx := types.NewXObject(map[string]types.XValue{
			"contact": flows.Context(oa.Env(), contact),
			"fields":  flows.Context(oa.Env(), contact.Fields()),
			"globals": flows.Context(oa.Env(), oa.SessionAssets().Globals()),
			"urns":    flows.ContextFunc(oa.Env(), contact.URNs().MapContext),
		})
		text, _ = excellent.EvaluateTemplate(oa.Env(), templateCtx, text, nil)
	}

	// don't create a message if we have no content
	if text == "" && len(attachments) == 0 && len(trans.QuickReplies) == 0 {
		return nil, nil
	}

	// create our outgoing message
	out, ch := NewMsgOut(oa, contact, text, attachments, quickReplies, locale)

	msg, err := NewOutgoingBroadcastMsg(rt, oa.Org(), ch, contact, out, time.Now(), b)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating outgoing message")
	}

	return msg, nil
}
