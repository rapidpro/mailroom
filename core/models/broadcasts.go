package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/lib/pq/hstore"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/excellent"
	"github.com/nyaruka/goflow/excellent/types"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/events"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/nyaruka/null"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// BroadcastID is our internal type for broadcast ids, which can be null/0
type BroadcastID null.Int

// NilBroadcastID is our constant for a nil broadcast id
const NilBroadcastID = BroadcastID(0)

// TemplateState represents what state are templates are in, either already evaluated, not evaluated or
// that they are unevaluated legacy templates
type TemplateState string

const (
	TemplateStateEvaluated   = TemplateState("evaluated")
	TemplateStateUnevaluated = TemplateState("unevaluated")
)

// BroadcastTranslation is the translation for the passed in language
type BroadcastTranslation struct {
	Text         string             `json:"text"`
	Attachments  []utils.Attachment `json:"attachments,omitempty"`
	QuickReplies []string           `json:"quick_replies,omitempty"`
}

type BroadcastTranslations map[envs.Language]*BroadcastTranslation

func (t *BroadcastTranslations) Scan(v interface{}) error {
	b, ok := v.([]byte)
	if !ok {
		return errors.New("failed type assertion to []byte")
	}
	return json.Unmarshal(b, &t)
}

func (t BroadcastTranslations) Value() (driver.Value, error) {
	return json.Marshal(t)
}

// Broadcast represents a broadcast that needs to be sent
type Broadcast struct {
	b struct {
		BroadcastID   BroadcastID           `json:"broadcast_id,omitempty"  db:"id"`
		Translations  BroadcastTranslations `json:"translations"            db:"translations"`
		Text          hstore.Hstore         `                               db:"text"` // deprecated
		TemplateState TemplateState         `json:"template_state"`
		BaseLanguage  envs.Language         `json:"base_language"           db:"base_language"`
		URNs          []urns.URN            `json:"urns,omitempty"`
		ContactIDs    []ContactID           `json:"contact_ids,omitempty"`
		GroupIDs      []GroupID             `json:"group_ids,omitempty"`
		OrgID         OrgID                 `json:"org_id"                  db:"org_id"`
		CreatedByID   UserID                `json:"created_by_id,omitempty" db:"created_by_id"`
		ParentID      BroadcastID           `json:"parent_id,omitempty"     db:"parent_id"`
		TicketID      TicketID              `json:"ticket_id,omitempty"     db:"ticket_id"`
	}
}

func (b *Broadcast) ID() BroadcastID                                       { return b.b.BroadcastID }
func (b *Broadcast) OrgID() OrgID                                          { return b.b.OrgID }
func (b *Broadcast) CreatedByID() UserID                                   { return b.b.CreatedByID }
func (b *Broadcast) ContactIDs() []ContactID                               { return b.b.ContactIDs }
func (b *Broadcast) GroupIDs() []GroupID                                   { return b.b.GroupIDs }
func (b *Broadcast) URNs() []urns.URN                                      { return b.b.URNs }
func (b *Broadcast) BaseLanguage() envs.Language                           { return b.b.BaseLanguage }
func (b *Broadcast) Translations() map[envs.Language]*BroadcastTranslation { return b.b.Translations }
func (b *Broadcast) TemplateState() TemplateState                          { return b.b.TemplateState }
func (b *Broadcast) TicketID() TicketID                                    { return b.b.TicketID }

func (b *Broadcast) MarshalJSON() ([]byte, error)    { return json.Marshal(b.b) }
func (b *Broadcast) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, &b.b) }

// NewBroadcast creates a new broadcast with the passed in parameters
func NewBroadcast(
	orgID OrgID, id BroadcastID, translations map[envs.Language]*BroadcastTranslation,
	state TemplateState, baseLanguage envs.Language, urns []urns.URN, contactIDs []ContactID, groupIDs []GroupID, ticketID TicketID, createdByID UserID) *Broadcast {

	bcast := &Broadcast{}
	bcast.b.OrgID = orgID
	bcast.b.BroadcastID = id
	bcast.b.Translations = translations
	bcast.b.TemplateState = state
	bcast.b.BaseLanguage = baseLanguage
	bcast.b.URNs = urns
	bcast.b.ContactIDs = contactIDs
	bcast.b.GroupIDs = groupIDs
	bcast.b.TicketID = ticketID
	bcast.b.CreatedByID = createdByID

	return bcast
}

// InsertChildBroadcast clones the passed in broadcast as a parent, then inserts that broadcast into the DB
func InsertChildBroadcast(ctx context.Context, db Queryer, parent *Broadcast) (*Broadcast, error) {
	child := NewBroadcast(
		parent.OrgID(),
		NilBroadcastID,
		parent.b.Translations,
		parent.b.TemplateState,
		parent.b.BaseLanguage,
		parent.b.URNs,
		parent.b.ContactIDs,
		parent.b.GroupIDs,
		parent.b.TicketID,
		parent.b.CreatedByID,
	)
	child.b.ParentID = parent.ID()

	// populate text from our translations
	child.b.Text.Map = make(map[string]sql.NullString)
	for lang, t := range child.b.Translations {
		child.b.Text.Map[string(lang)] = sql.NullString{String: t.Text, Valid: true}
		if len(t.Attachments) > 0 || len(t.QuickReplies) > 0 {
			return nil, errors.Errorf("cannot clone broadcast with quick replies or attachments")
		}
	}

	// insert our broadcast
	err := BulkQuery(ctx, "inserting broadcast", db, insertBroadcastSQL, []interface{}{&child.b})
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting child broadcast for broadcast: %d", parent.ID())
	}

	// build up all our contact associations
	contacts := make([]interface{}, 0, len(child.b.ContactIDs))
	for _, contactID := range child.b.ContactIDs {
		contacts = append(contacts, &broadcastContact{
			BroadcastID: child.ID(),
			ContactID:   contactID,
		})
	}

	// insert our contacts
	err = BulkQuery(ctx, "inserting broadcast contacts", db, insertBroadcastContactsSQL, contacts)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting contacts for broadcast")
	}

	// build up all our group associations
	groups := make([]interface{}, 0, len(child.b.GroupIDs))
	for _, groupID := range child.b.GroupIDs {
		groups = append(groups, &broadcastGroup{
			BroadcastID: child.ID(),
			GroupID:     groupID,
		})
	}

	// insert our groups
	err = BulkQuery(ctx, "inserting broadcast groups", db, insertBroadcastGroupsSQL, groups)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting groups for broadcast")
	}

	// finally our URNs
	urns := make([]interface{}, 0, len(child.b.URNs))
	for _, urn := range child.b.URNs {
		urnID := GetURNID(urn)
		if urnID == NilURNID {
			return nil, errors.Errorf("attempt to insert new broadcast with URNs that do not have id: %s", urn)
		}
		urns = append(urns, &broadcastURN{
			BroadcastID: child.ID(),
			URNID:       urnID,
		})
	}

	// insert our urns
	err = BulkQuery(ctx, "inserting broadcast urns", db, insertBroadcastURNsSQL, urns)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting URNs for broadcast")
	}

	return child, nil
}

type broadcastURN struct {
	BroadcastID BroadcastID `db:"broadcast_id"`
	URNID       URNID       `db:"contacturn_id"`
}

type broadcastContact struct {
	BroadcastID BroadcastID `db:"broadcast_id"`
	ContactID   ContactID   `db:"contact_id"`
}

type broadcastGroup struct {
	BroadcastID BroadcastID `db:"broadcast_id"`
	GroupID     GroupID     `db:"contactgroup_id"`
}

const insertBroadcastSQL = `
INSERT INTO
	msgs_broadcast( org_id,  parent_id,  ticket_id, created_on, modified_on, status,  translations,  text,  base_language, send_all, is_active)
			VALUES(:org_id, :parent_id, :ticket_id, NOW()     , NOW(),       'Q',    :translations, :text, :base_language, FALSE,    TRUE)
RETURNING id`

const insertBroadcastContactsSQL = `
INSERT INTO msgs_broadcast_contacts(broadcast_id, contact_id) VALUES(:broadcast_id, :contact_id)`

const insertBroadcastGroupsSQL = `
INSERT INTO msgs_broadcast_groups(broadcast_id, contactgroup_id) VALUES(:broadcast_id, :contactgroup_id)`

const insertBroadcastURNsSQL = `
INSERT INTO msgs_broadcast_urns(broadcast_id, contacturn_id) VALUES(:broadcast_id, :contacturn_id)`

// NewBroadcastFromEvent creates a broadcast object from the passed in broadcast event
func NewBroadcastFromEvent(ctx context.Context, tx Queryer, oa *OrgAssets, event *events.BroadcastCreatedEvent) (*Broadcast, error) {
	// converst our translations to our type
	translations := make(map[envs.Language]*BroadcastTranslation)
	for l, t := range event.Translations {
		translations[l] = &BroadcastTranslation{
			Text:         t.Text,
			Attachments:  t.Attachments,
			QuickReplies: t.QuickReplies,
		}
	}

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

	return NewBroadcast(oa.OrgID(), NilBroadcastID, translations, TemplateStateEvaluated, event.BaseLanguage, event.URNs, contactIDs, groupIDs, NilTicketID, NilUserID), nil
}

func (b *Broadcast) CreateBatch(contactIDs []ContactID) *BroadcastBatch {
	return &BroadcastBatch{
		BroadcastID:   b.b.BroadcastID,
		BaseLanguage:  b.b.BaseLanguage,
		Translations:  b.b.Translations,
		TemplateState: b.b.TemplateState,
		OrgID:         b.b.OrgID,
		CreatedByID:   b.b.CreatedByID,
		TicketID:      b.b.TicketID,
		ContactIDs:    contactIDs,
	}
}

// BroadcastBatch represents a batch of contacts that need messages sent for
type BroadcastBatch struct {
	BroadcastID   BroadcastID                             `json:"broadcast_id,omitempty"`
	Translations  map[envs.Language]*BroadcastTranslation `json:"translations"`
	BaseLanguage  envs.Language                           `json:"base_language"`
	TemplateState TemplateState                           `json:"template_state"`
	URNs          map[ContactID]urns.URN                  `json:"urns,omitempty"`
	ContactIDs    []ContactID                             `json:"contact_ids,omitempty"`
	IsLast        bool                                    `json:"is_last"`
	OrgID         OrgID                                   `json:"org_id"`
	CreatedByID   UserID                                  `json:"created_by_id"`
	TicketID      TicketID                                `json:"ticket_id"`
}

func (b *BroadcastBatch) CreateMessages(ctx context.Context, rt *runtime.Runtime, oa *OrgAssets) ([]*Msg, error) {
	repeatedContacts := make(map[ContactID]bool)
	broadcastURNs := b.URNs

	// build our list of contact ids
	contactIDs := b.ContactIDs

	// build a map of the contacts that are present both in our URN list and our contact id list
	if broadcastURNs != nil {
		for _, id := range contactIDs {
			_, found := broadcastURNs[id]
			if found {
				repeatedContacts[id] = true
			}
		}

		// if we have URN we need to send to, add those contacts as well if not already repeated
		for id := range broadcastURNs {
			if !repeatedContacts[id] {
				contactIDs = append(contactIDs, id)
			}
		}
	}

	// load all our contacts
	contacts, err := LoadContacts(ctx, rt.DB, oa, contactIDs)
	if err != nil {
		return nil, errors.Wrapf(err, "error loading contacts for broadcast")
	}

	channels := oa.SessionAssets().Channels()

	// for each contact, build our message
	msgs := make([]*Msg, 0, len(contacts))

	// utility method to build up our message
	buildMessage := func(c *Contact, forceURN urns.URN) (*Msg, error) {
		if c.Status() != ContactStatusActive {
			return nil, nil
		}

		contact, err := c.FlowContact(oa)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating flow contact")
		}

		urn := urns.NilURN
		var channel *Channel

		// we are forcing to send to a non-preferred URN, find the channel
		if forceURN != urns.NilURN {
			for _, u := range contact.URNs() {
				if u.URN().Identity() == forceURN.Identity() {
					c := channels.GetForURN(u, assets.ChannelRoleSend)
					if c == nil {
						return nil, nil
					}
					urn = u.URN()
					channel = oa.ChannelByUUID(c.UUID())
					break
				}
			}
		} else {
			// no forced URN, find the first URN we can send to
			for _, u := range contact.URNs() {
				c := channels.GetForURN(u, assets.ChannelRoleSend)
				if c != nil {
					urn = u.URN()
					channel = oa.ChannelByUUID(c.UUID())
					break
				}
			}
		}

		// no urn and channel? move on
		if channel == nil {
			return nil, nil
		}

		// resolve our translations, the order is:
		//   1) valid contact language
		//   2) org default language
		//   3) broadcast base language
		lang := contact.Language()
		if lang != envs.NilLanguage {
			found := false
			for _, l := range oa.Env().AllowedLanguages() {
				if l == lang {
					found = true
					break
				}
			}
			if !found {
				lang = envs.NilLanguage
			}
		}

		// have a valid contact language, try that
		trans := b.Translations
		t := trans[lang]

		// not found? try org default language
		if t == nil {
			lang = oa.Env().DefaultLanguage()
			t = trans[lang]
		}

		// not found? use broadcast base language
		if t == nil {
			lang = b.BaseLanguage
			t = trans[lang]
		}

		if t == nil {
			logrus.WithField("base_language", b.BaseLanguage).WithField("translations", trans).Error("unable to find translation for broadcast")
			return nil, nil
		}

		template := ""
		if b.TemplateState == TemplateStateUnevaluated {
			template = t.Text
		}

		text := t.Text

		// if we have a template, evaluate it
		if template != "" {
			// build up the minimum viable context for templates
			templateCtx := types.NewXObject(map[string]types.XValue{
				"contact": flows.Context(oa.Env(), contact),
				"fields":  flows.Context(oa.Env(), contact.Fields()),
				"globals": flows.Context(oa.Env(), oa.SessionAssets().Globals()),
				"urns":    flows.ContextFunc(oa.Env(), contact.URNs().MapContext),
			})
			text, _ = excellent.EvaluateTemplate(oa.Env(), templateCtx, template, nil)
		}

		// don't do anything if we have no text or attachments
		if text == "" && len(t.Attachments) == 0 {
			return nil, nil
		}

		unsendableReason := flows.NilUnsendableReason
		if contact.Status() != flows.ContactStatusActive {
			unsendableReason = flows.UnsendableReasonContactStatus
		} else if urn == urns.NilURN || channel == nil {
			unsendableReason = flows.UnsendableReasonNoDestination
		}

		// create our outgoing message
		out := flows.NewMsgOut(urn, channel.ChannelReference(), text, t.Attachments, t.QuickReplies, nil, flows.NilMsgTopic, envs.NewLocale(lang, envs.NilCountry), unsendableReason)
		msg, err := NewOutgoingBroadcastMsg(rt, oa.Org(), channel, contact, out, time.Now(), b.BroadcastID)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating outgoing message")
		}

		return msg, nil
	}

	// run through all our contacts to create our messages
	for _, c := range contacts {
		// use the preferred URN if present
		urn := broadcastURNs[c.ID()]
		msg, err := buildMessage(c, urn)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating broadcast message")
		}
		if msg != nil {
			msgs = append(msgs, msg)
		}

		// if this is a contact that will receive two messages, calculate that one as well
		if repeatedContacts[c.ID()] {
			m2, err := buildMessage(c, urns.NilURN)
			if err != nil {
				return nil, errors.Wrapf(err, "error creating broadcast message")
			}

			// add this message if it isn't a duplicate
			if m2 != nil && m2.URN() != msg.URN() {
				msgs = append(msgs, m2)
			}
		}
	}

	// insert them in a single request
	err = InsertMessages(ctx, rt.DB, msgs)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting broadcast messages")
	}

	// if the broadcast was a ticket reply, update the ticket
	if b.TicketID != NilTicketID {
		if err := b.updateTicket(ctx, rt.DB, oa); err != nil {
			return nil, err
		}
	}

	return msgs, nil
}

func (b *BroadcastBatch) updateTicket(ctx context.Context, db Queryer, oa *OrgAssets) error {
	firstReplySeconds, err := TicketRecordReplied(ctx, db, b.TicketID, dates.Now())
	if err != nil {
		return err
	}

	// record reply counts for org, user and team
	replyCounts := map[string]int{scopeOrg(oa): 1}

	if b.CreatedByID != NilUserID {
		user := oa.UserByID(b.CreatedByID)
		if user != nil {
			replyCounts[scopeUser(oa, user)] = 1
			if user.Team() != nil {
				replyCounts[scopeTeam(user.Team())] = 1
			}
		}
	}

	if err := insertTicketDailyCounts(ctx, db, TicketDailyCountReply, oa.Org().Timezone(), replyCounts); err != nil {
		return err
	}

	if firstReplySeconds >= 0 {
		if err := insertTicketDailyTiming(ctx, db, TicketDailyTimingFirstReply, oa.Org().Timezone(), scopeOrg(oa), firstReplySeconds); err != nil {
			return err
		}
	}
	return nil
}
