package models

import (
	"context"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type TriggerType string

type MatchType string

type TriggerID int

const (
	CatchallTriggerType        = TriggerType("C")
	KeywordTriggerType         = TriggerType("K")
	MissedCallTriggerType      = TriggerType("M")
	NewConversationTriggerType = TriggerType("N")
	ReferralTriggerType        = TriggerType("R")
	CallTriggerType            = TriggerType("V")
	ScheduleTriggerType        = TriggerType("S")

	MatchFirst = "F"
	MatchOnly  = "O"

	NilTriggerID = TriggerID(0)
)

// Trigger represents a trigger in an organization
type Trigger struct {
	t struct {
		ID          TriggerID   `json:"id"`
		FlowID      FlowID      `json:"flow_id"`
		TriggerType TriggerType `json:"trigger_type"`
		Keyword     string      `json:"keyword"`
		MatchType   MatchType   `json:"match_type"`
		ChannelID   ChannelID   `json:"channel_id"`
		ReferrerID  string      `json:"referrer_id"`
		GroupIDs    []GroupID   `json:"group_ids"`
		ContactIDs  []ContactID `json:"contact_ids,omitempty"`
	}
}

func (t *Trigger) ID() TriggerID            { return t.t.ID }
func (t *Trigger) FlowID() FlowID           { return t.t.FlowID }
func (t *Trigger) TriggerType() TriggerType { return t.t.TriggerType }
func (t *Trigger) Keyword() string          { return t.t.Keyword }
func (t *Trigger) MatchType() MatchType     { return t.t.MatchType }
func (t *Trigger) ChannelID() ChannelID     { return t.t.ChannelID }
func (t *Trigger) ReferrerID() string       { return t.t.ReferrerID }
func (t *Trigger) GroupIDs() []GroupID      { return t.t.GroupIDs }
func (t *Trigger) ContactIDs() []ContactID  { return t.t.ContactIDs }
func (t *Trigger) KeywordMatchType() triggers.KeywordMatchType {
	if t.t.MatchType == MatchFirst {
		return triggers.KeywordMatchTypeFirstWord
	}
	return triggers.KeywordMatchTypeOnlyWord
}

// Match returns the match for this trigger, if any
func (t *Trigger) Match() *triggers.KeywordMatch {
	if t.Keyword() != "" {
		return &triggers.KeywordMatch{
			Type:    t.KeywordMatchType(),
			Keyword: t.Keyword(),
		}
	}
	return nil
}

// loadTriggers loads all non-schedule triggers for the passed in org
func loadTriggers(ctx context.Context, db *sqlx.DB, orgID OrgID) ([]*Trigger, error) {
	start := time.Now()

	rows, err := db.Queryx(selectTriggersSQL, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying triggers for org: %d", orgID)
	}
	defer rows.Close()

	triggers := make([]*Trigger, 0, 10)
	for rows.Next() {
		trigger := &Trigger{}
		err = readJSONRow(rows, &trigger.t)
		if err != nil {
			return nil, errors.Wrap(err, "error scanning label row")
		}
		triggers = append(triggers, trigger)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", orgID).WithField("count", len(triggers)).Debug("loaded triggers")

	return triggers, nil
}

// FindMatchingNewConversationTrigger returns the matching trigger for the passed in trigger type
func FindMatchingNewConversationTrigger(org *OrgAssets, channel *Channel) *Trigger {
	var match *Trigger
	for _, t := range org.Triggers() {
		if t.TriggerType() == NewConversationTriggerType {
			// exact match? return right away
			if t.ChannelID() == channel.ID() {
				return t
			}

			// trigger has no channel filter, record this as match
			if t.ChannelID() == NilChannelID && match == nil {
				match = t
			}
		}
	}

	return match
}

// FindMatchingMissedCallTrigger finds any trigger set up for incoming calls (these would be IVR flows)
func FindMatchingMissedCallTrigger(org *OrgAssets) *Trigger {
	for _, t := range org.Triggers() {
		if t.TriggerType() == MissedCallTriggerType {
			return t
		}
	}

	return nil
}

// FindMatchingMOCallTrigger finds any trigger set up for incoming calls (these would be IVR flows)
// Contact is needed as this trigger can be filtered by contact group
func FindMatchingMOCallTrigger(org *OrgAssets, contact *Contact) *Trigger {
	// build a set of the groups this contact is in
	groupIDs := make(map[GroupID]bool, 10)
	for _, g := range contact.Groups() {
		groupIDs[g.ID()] = true
	}

	var match *Trigger
	for _, t := range org.Triggers() {
		if t.TriggerType() == CallTriggerType {
			// this trigger has no groups, it's a match!
			if len(t.GroupIDs()) == 0 {
				if match == nil {
					match = t
				}
				continue
			}

			// test whether we are part of this trigger's group
			for _, g := range t.GroupIDs() {
				if groupIDs[g] {
					// group keyword matches always take precedence, can return right away
					return t
				}
			}
		}
	}

	return match
}

// FindMatchingReferralTrigger returns the matching trigger for the passed in trigger type
// Matches are based on referrer_id first (if present), then channel, then any referrer trigger
func FindMatchingReferralTrigger(org *OrgAssets, channel *Channel, referrerID string) *Trigger {
	var match *Trigger
	for _, t := range org.Triggers() {
		if t.TriggerType() == ReferralTriggerType {
			// matches referrer id? that takes top precedence, return right away
			if referrerID != "" && referrerID == t.ReferrerID() && (t.ChannelID() == NilChannelID || t.ChannelID() == channel.ID()) {
				return t
			}

			// if this trigger has no referrer id, maybe we match by channel
			if t.ReferrerID() == "" {
				// matches channel? that is a good match
				if t.ChannelID() == channel.ID() {
					match = t
				} else if match == nil && t.ChannelID() == NilChannelID {
					// otherwise if we haven't been set yet, pick that
					match = t
				}
			}
		}
	}

	return match
}

// FindMatchingMsgTrigger returns the matching trigger (if any) for the passed in text and channel id
// TODO: with a different structure this could probably be a lot faster.. IE, we could have a map
// of list of triggers by keyword that is built when we load the triggers, then just evaluate against that.
func FindMatchingMsgTrigger(org *OrgAssets, contact *flows.Contact, text string) *Trigger {
	// build a set of the groups this contact is in
	groupIDs := make(map[GroupID]bool, 10)
	for _, g := range contact.Groups().All() {
		groupIDs[g.Asset().(*Group).ID()] = true
	}

	// determine our message keyword
	words := utils.TokenizeString(text)
	keyword := ""
	only := false
	if len(words) > 0 {
		// our keyword is our first word
		keyword = strings.ToLower(words[0])
		only = len(words) == 1
	}

	var match, catchAll, groupCatchAll *Trigger
	for _, t := range org.Triggers() {
		if t.TriggerType() == KeywordTriggerType {
			// does this match based on the rules of the trigger?
			matched := (t.Keyword() == keyword && (t.MatchType() == MatchFirst || (t.MatchType() == MatchOnly && only)))

			// no match? move on
			if !matched {
				continue
			}

			// this trigger has no groups, it's a match!
			if len(t.GroupIDs()) == 0 {
				if match == nil {
					match = t
				}
				continue
			}

			// test whether we are part of this trigger's group
			for _, g := range t.GroupIDs() {
				if groupIDs[g] {
					// group keyword matches always take precedence, can return right away
					return t
				}
			}
		} else if t.TriggerType() == CatchallTriggerType {
			// if this catch all is on no groups, save it as our catch all
			if len(t.GroupIDs()) == 0 {
				if catchAll == nil {
					catchAll = t
				}
				continue
			}

			// otherwise see if this catchall matches our group
			if groupCatchAll == nil {
				for _, g := range t.GroupIDs() {
					if groupIDs[g] {
						groupCatchAll = t
						break
					}
				}
			}
		}
	}

	// have a normal match? return that
	if match != nil {
		return match
	}

	// otherwise return our group catch all if we found one
	if groupCatchAll != nil {
		return groupCatchAll
	}

	// or our global catchall
	return catchAll
}

const selectTriggersSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	t.id as id, 
	t.flow_id as flow_id,
	t.trigger_type as trigger_type,
	t.keyword as keyword,
	t.match_type as match_type,
	t.channel_id as channel_id,
	COALESCE(t.referrer_id, '') as referrer_id,
	ARRAY_REMOVE(ARRAY_AGG(g.contactgroup_id), NULL) as group_ids
FROM 
	triggers_trigger t
	LEFT OUTER JOIN triggers_trigger_groups g ON t.id = g.trigger_id
WHERE 
	t.org_id = $1 AND 
	t.is_active = TRUE AND
	t.is_archived = FALSE AND
	t.trigger_type != 'S'
GROUP BY 
	t.id
) r;
`
