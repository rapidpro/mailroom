package models

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"time"

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
	"github.com/nyaruka/null/v2"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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

// BroadcastTranslation is the translation for the passed in language
type BroadcastTranslation struct {
	Text         string             `json:"text"`
	Attachments  []utils.Attachment `json:"attachments,omitempty"`
	QuickReplies []string           `json:"quick_replies,omitempty"`
}

type BroadcastTranslations map[envs.Language]*BroadcastTranslation

func (t *BroadcastTranslations) Scan(value any) error {
	b, ok := value.([]byte)
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
	ID            BroadcastID           `json:"broadcast_id,omitempty"  db:"id"`
	OrgID         OrgID                 `json:"org_id"                  db:"org_id"`
	Translations  BroadcastTranslations `json:"translations"            db:"translations"`
	TemplateState TemplateState         `json:"template_state"`
	BaseLanguage  envs.Language         `json:"base_language"           db:"base_language"`
	URNs          []urns.URN            `json:"urns,omitempty"`
	ContactIDs    []ContactID           `json:"contact_ids,omitempty"`
	GroupIDs      []GroupID             `json:"group_ids,omitempty"`
	Query         null.String           `json:"query,omitempty"         db:"query"`
	CreatedByID   UserID                `json:"created_by_id,omitempty" db:"created_by_id"`
	ParentID      BroadcastID           `json:"parent_id,omitempty"     db:"parent_id"`
	TicketID      TicketID              `json:"ticket_id,omitempty"     db:"ticket_id"`
}

// NewBroadcast creates a new broadcast with the passed in parameters
func NewBroadcast(orgID OrgID, translations map[envs.Language]*BroadcastTranslation,
	state TemplateState, baseLanguage envs.Language, urns []urns.URN, contactIDs []ContactID, groupIDs []GroupID, query string, ticketID TicketID, createdByID UserID) *Broadcast {

	return &Broadcast{
		OrgID:         orgID,
		Translations:  translations,
		TemplateState: state,
		BaseLanguage:  baseLanguage,
		URNs:          urns,
		ContactIDs:    contactIDs,
		GroupIDs:      groupIDs,
		TicketID:      ticketID,
		CreatedByID:   createdByID,
	}
}

// InsertChildBroadcast clones the passed in broadcast as a parent, then inserts that broadcast into the DB
func InsertChildBroadcast(ctx context.Context, db Queryer, parent *Broadcast) (*Broadcast, error) {
	child := NewBroadcast(
		parent.OrgID,
		parent.Translations,
		parent.TemplateState,
		parent.BaseLanguage,
		parent.URNs,
		parent.ContactIDs,
		parent.GroupIDs,
		string(parent.Query),
		parent.TicketID,
		parent.CreatedByID,
	)
	child.ParentID = parent.ID

	// insert our broadcast
	err := BulkQuery(ctx, "inserting broadcast", db, sqlInsertBroadcast, []*Broadcast{child})
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting child broadcast for broadcast: %d", parent.ID)
	}

	// build up all our contact associations
	contacts := make([]*broadcastContact, 0, len(child.ContactIDs))
	for _, contactID := range child.ContactIDs {
		contacts = append(contacts, &broadcastContact{BroadcastID: child.ID, ContactID: contactID})
	}

	// insert our contacts
	err = BulkQuery(ctx, "inserting broadcast contacts", db, sqlInsertBroadcastContacts, contacts)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting contacts for broadcast")
	}

	// build up all our group associations
	groups := make([]*broadcastGroup, 0, len(child.GroupIDs))
	for _, groupID := range child.GroupIDs {
		groups = append(groups, &broadcastGroup{BroadcastID: child.ID, GroupID: groupID})
	}

	// insert our groups
	err = BulkQuery(ctx, "inserting broadcast groups", db, sqlInsertBroadcastGroups, groups)
	if err != nil {
		return nil, errors.Wrapf(err, "error inserting groups for broadcast")
	}

	// finally our URNs
	urns := make([]*broadcastURN, 0, len(child.URNs))
	for _, urn := range child.URNs {
		urnID := GetURNID(urn)
		if urnID == NilURNID {
			return nil, errors.Errorf("attempt to insert new broadcast with URNs that do not have id: %s", urn)
		}
		urns = append(urns, &broadcastURN{BroadcastID: child.ID, URNID: urnID})
	}

	// insert our urns
	err = BulkQuery(ctx, "inserting broadcast urns", db, sqlInsertBroadcastURNs, urns)
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

const sqlInsertBroadcast = `
INSERT INTO
	msgs_broadcast( org_id,  parent_id,  ticket_id, created_on, modified_on, status,  translations,  base_language,  query, send_all, is_active)
			VALUES(:org_id, :parent_id, :ticket_id, NOW()     , NOW(),       'Q',    :translations, :base_language, :query, FALSE,    TRUE)
RETURNING id`

const sqlInsertBroadcastContacts = `INSERT INTO msgs_broadcast_contacts(broadcast_id, contact_id) VALUES(:broadcast_id, :contact_id)`
const sqlInsertBroadcastGroups = `INSERT INTO msgs_broadcast_groups(broadcast_id, contactgroup_id) VALUES(:broadcast_id, :contactgroup_id)`
const sqlInsertBroadcastURNs = `INSERT INTO msgs_broadcast_urns(broadcast_id, contacturn_id) VALUES(:broadcast_id, :contacturn_id)`

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

	return NewBroadcast(oa.OrgID(), translations, TemplateStateEvaluated, event.BaseLanguage, event.URNs, contactIDs, groupIDs, event.ContactQuery, NilTicketID, NilUserID), nil
}

func (b *Broadcast) CreateBatch(contactIDs []ContactID) *BroadcastBatch {
	return &BroadcastBatch{
		BroadcastID:   b.ID,
		OrgID:         b.OrgID,
		BaseLanguage:  b.BaseLanguage,
		Translations:  b.Translations,
		TemplateState: b.TemplateState,
		CreatedByID:   b.CreatedByID,
		TicketID:      b.TicketID,
		ContactIDs:    contactIDs,
	}
}

// BroadcastBatch represents a batch of contacts that need messages sent for
type BroadcastBatch struct {
	BroadcastID   BroadcastID            `json:"broadcast_id,omitempty"`
	OrgID         OrgID                  `json:"org_id"`
	Translations  BroadcastTranslations  `json:"translations"`
	BaseLanguage  envs.Language          `json:"base_language"`
	TemplateState TemplateState          `json:"template_state"`
	URNs          map[ContactID]urns.URN `json:"urns,omitempty"`
	ContactIDs    []ContactID            `json:"contact_ids,omitempty"`
	IsLast        bool                   `json:"is_last"`
	CreatedByID   UserID                 `json:"created_by_id"`
	TicketID      TicketID               `json:"ticket_id"`
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
	firstReplyTime, err := TicketRecordReplied(ctx, db, b.TicketID, dates.Now())
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

	if firstReplyTime >= 0 {
		if err := insertTicketDailyTiming(ctx, db, TicketDailyTimingFirstReply, oa.Org().Timezone(), scopeOrg(oa), firstReplyTime); err != nil {
			return err
		}
	}
	return nil
}
