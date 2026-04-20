package models

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"strings"

	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/goflow/utils"
	"github.com/pkg/errors"
)

// TriggerType is the type of a trigger
type TriggerType string

// MatchType is used for keyword triggers to specify how they should match
type MatchType string

// TriggerID is the type for trigger database IDs
type TriggerID int

// trigger type constants
const (
	CatchallTriggerType        = TriggerType("C")
	KeywordTriggerType         = TriggerType("K")
	MissedCallTriggerType      = TriggerType("M")
	NewConversationTriggerType = TriggerType("N")
	ReferralTriggerType        = TriggerType("R")
	IncomingCallTriggerType    = TriggerType("V")
	ScheduleTriggerType        = TriggerType("S")
	TicketClosedTriggerType    = TriggerType("T")
	OptInTriggerType           = TriggerType("I")
	OptOutTriggerType          = TriggerType("O")
)

// match type constants
const (
	MatchFirst MatchType = "F"
	MatchOnly  MatchType = "O"
)

// NilTriggerID is the nil value for trigger IDs
const NilTriggerID = TriggerID(0)

// Trigger represents a trigger in an organization
type Trigger struct {
	t struct {
		ID              TriggerID      `json:"id"`
		OrgID           OrgID          `json:"org_id"`
		FlowID          FlowID         `json:"flow_id"`
		TriggerType     TriggerType    `json:"trigger_type"`
		Keywords        pq.StringArray `json:"keywords"`
		MatchType       MatchType      `json:"match_type"`
		ChannelID       ChannelID      `json:"channel_id"`
		ReferrerID      string         `json:"referrer_id"`
		IncludeGroupIDs []GroupID      `json:"include_group_ids"`
		ExcludeGroupIDs []GroupID      `json:"exclude_group_ids"`
		ContactIDs      []ContactID    `json:"contact_ids,omitempty"`
	}
}

// ID returns the id of this trigger
func (t *Trigger) ID() TriggerID              { return t.t.ID }
func (t *Trigger) OrgID() OrgID               { return t.t.OrgID }
func (t *Trigger) FlowID() FlowID             { return t.t.FlowID }
func (t *Trigger) TriggerType() TriggerType   { return t.t.TriggerType }
func (t *Trigger) Keywords() []string         { return []string(t.t.Keywords) }
func (t *Trigger) MatchType() MatchType       { return t.t.MatchType }
func (t *Trigger) ChannelID() ChannelID       { return t.t.ChannelID }
func (t *Trigger) ReferrerID() string         { return t.t.ReferrerID }
func (t *Trigger) IncludeGroupIDs() []GroupID { return t.t.IncludeGroupIDs }
func (t *Trigger) ExcludeGroupIDs() []GroupID { return t.t.ExcludeGroupIDs }
func (t *Trigger) ContactIDs() []ContactID    { return t.t.ContactIDs }
func (t *Trigger) KeywordMatchType() triggers.KeywordMatchType {
	if t.t.MatchType == MatchFirst {
		return triggers.KeywordMatchTypeFirstWord
	}
	return triggers.KeywordMatchTypeOnlyWord
}

func (t *Trigger) UnmarshalJSON(b []byte) error { return json.Unmarshal(b, &t.t) }

// CreateStart generates an insertable flow start for scheduled trigger
func (t *Trigger) CreateStart() *FlowStart {
	return NewFlowStart(t.t.OrgID, StartTypeTrigger, t.t.FlowID).
		WithContactIDs(t.t.ContactIDs).
		WithGroupIDs(t.t.IncludeGroupIDs).
		WithExcludeGroupIDs(t.t.ExcludeGroupIDs)
}

// loadTriggers loads all non-schedule triggers for the passed in org
func loadTriggers(ctx context.Context, db *sql.DB, orgID OrgID) ([]*Trigger, error) {
	rows, err := db.QueryContext(ctx, sqlSelectTriggersByOrg, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying triggers for org: %d", orgID)
	}
	defer rows.Close()

	triggers := make([]*Trigger, 0, 10)
	for rows.Next() {
		trigger := &Trigger{}
		err = dbutil.ScanJSON(rows, &trigger.t)
		if err != nil {
			return nil, errors.Wrap(err, "error scanning label row")
		}

		triggers = append(triggers, trigger)
	}

	return triggers, nil
}

// FindMatchingMsgTrigger finds the best match trigger for an incoming message from the given contact
func FindMatchingMsgTrigger(oa *OrgAssets, channel *Channel, contact *flows.Contact, text string) (*Trigger, string) {
	// determine our message keyword
	words := utils.TokenizeString(text)
	keyword := ""
	only := false
	if len(words) > 0 {
		// our keyword is our first word
		keyword = words[0]
		only = len(words) == 1
	}

	// for each candidate trigger, the keyword that matched
	candidateKeywords := make(map[*Trigger]string, 10)

	candidates := findTriggerCandidates(oa, KeywordTriggerType, func(t *Trigger) bool {
		for _, k := range t.Keywords() {
			m := envs.CollateEquals(oa.Env(), k, keyword) && (t.MatchType() == MatchFirst || (t.MatchType() == MatchOnly && only))
			if m {
				candidateKeywords[t] = k
				return true
			}
		}
		return false
	})

	// if we have a matching keyword trigger return that, otherwise we move on to catchall triggers..
	byKeyword := findBestTriggerMatch(candidates, channel, contact)
	if byKeyword != nil {
		return byKeyword, candidateKeywords[byKeyword]
	}

	candidates = findTriggerCandidates(oa, CatchallTriggerType, nil)

	return findBestTriggerMatch(candidates, channel, contact), ""
}

// FindMatchingIncomingCallTrigger finds the best match trigger for incoming calls
func FindMatchingIncomingCallTrigger(oa *OrgAssets, channel *Channel, contact *flows.Contact) *Trigger {
	candidates := findTriggerCandidates(oa, IncomingCallTriggerType, nil)

	return findBestTriggerMatch(candidates, channel, contact)
}

// FindMatchingMissedCallTrigger finds the best match trigger for missed incoming calls
func FindMatchingMissedCallTrigger(oa *OrgAssets, channel *Channel) *Trigger {
	candidates := findTriggerCandidates(oa, MissedCallTriggerType, nil)

	return findBestTriggerMatch(candidates, channel, nil)
}

// FindMatchingNewConversationTrigger finds the best match trigger for new conversation channel events
func FindMatchingNewConversationTrigger(oa *OrgAssets, channel *Channel) *Trigger {
	candidates := findTriggerCandidates(oa, NewConversationTriggerType, nil)

	return findBestTriggerMatch(candidates, channel, nil)
}

// FindMatchingOptInTrigger finds the best match trigger for optin channel events
func FindMatchingOptInTrigger(oa *OrgAssets, channel *Channel) *Trigger {
	candidates := findTriggerCandidates(oa, OptInTriggerType, nil)

	return findBestTriggerMatch(candidates, channel, nil)
}

// FindMatchingOptOutTrigger finds the best match trigger for optout channel events
func FindMatchingOptOutTrigger(oa *OrgAssets, channel *Channel) *Trigger {
	candidates := findTriggerCandidates(oa, OptOutTriggerType, nil)

	return findBestTriggerMatch(candidates, channel, nil)
}

// FindMatchingReferralTrigger finds the best match trigger for referral click channel events
func FindMatchingReferralTrigger(oa *OrgAssets, channel *Channel, referrerID string) *Trigger {
	// first try to find matching referrer ID
	candidates := findTriggerCandidates(oa, ReferralTriggerType, func(t *Trigger) bool {
		return strings.EqualFold(t.ReferrerID(), referrerID)
	})

	match := findBestTriggerMatch(candidates, channel, nil)
	if match != nil {
		return match
	}

	// if that didn't work look for an empty referrer ID
	candidates = findTriggerCandidates(oa, ReferralTriggerType, func(t *Trigger) bool {
		return t.ReferrerID() == ""
	})

	return findBestTriggerMatch(candidates, channel, nil)
}

// FindMatchingTicketClosedTrigger finds the best match trigger for ticket closed events
func FindMatchingTicketClosedTrigger(oa *OrgAssets, contact *flows.Contact) *Trigger {
	candidates := findTriggerCandidates(oa, TicketClosedTriggerType, nil)

	return findBestTriggerMatch(candidates, nil, contact)
}

// finds trigger candidates based on type and optional filter
func findTriggerCandidates(oa *OrgAssets, type_ TriggerType, filter func(*Trigger) bool) []*Trigger {
	candidates := make([]*Trigger, 0, 10)

	for _, t := range oa.Triggers() {
		if t.TriggerType() == type_ && (filter == nil || filter(t)) {
			candidates = append(candidates, t)
		}
	}

	return candidates
}

type triggerMatch struct {
	trigger *Trigger
	score   int
}

// matching triggers are given a score based on how they matched, and this score is used to select the most
// specific match:
//
// channel (4) + include (2) + exclude (1) = 7
// channel (4) + include (2) = 6
// channel (4) + exclude (1) = 5
// channel (4) = 4
// include (2) + exclude (1) = 3
// include (2) = 2
// exclude (1) = 1
const triggerScoreByChannel = 4
const triggerScoreByInclusion = 2
const triggerScoreByExclusion = 1

func findBestTriggerMatch(candidates []*Trigger, channel *Channel, contact *flows.Contact) *Trigger {
	matches := make([]*triggerMatch, 0, len(candidates))

	var groupIDs map[GroupID]bool

	if contact != nil {
		// build a set of the groups this contact is in
		groupIDs = make(map[GroupID]bool, 10)
		for _, g := range contact.Groups().All() {
			groupIDs[g.Asset().(*Group).ID()] = true
		}
	}

	for _, t := range candidates {
		matched, score := triggerMatchQualifiers(t, channel, groupIDs)
		if matched {
			matches = append(matches, &triggerMatch{t, score})
		}
	}

	if len(matches) == 0 {
		return nil
	}

	// sort the matches to get them in descending order of score
	sort.SliceStable(matches, func(i, j int) bool { return matches[i].score > matches[j].score })

	return matches[0].trigger
}

// matches against the qualifiers (inclusion groups, exclusion groups, channel) on this trigger and returns a score
func triggerMatchQualifiers(t *Trigger, channel *Channel, contactGroups map[GroupID]bool) (bool, int) {
	score := 0

	if channel != nil && t.ChannelID() != NilChannelID {
		if t.ChannelID() == channel.ID() {
			score += triggerScoreByChannel
		} else {
			return false, 0
		}
	}

	if len(t.IncludeGroupIDs()) > 0 {
		inGroup := false
		// if contact is in any of the groups to include that's a match by inclusion
		for _, g := range t.IncludeGroupIDs() {
			if contactGroups[g] {
				inGroup = true
				score += triggerScoreByInclusion
				break
			}
		}
		if !inGroup {
			return false, 0
		}
	}

	if len(t.ExcludeGroupIDs()) > 0 {
		// if contact is in none of the groups to exclude that's a match by exclusion
		for _, g := range t.ExcludeGroupIDs() {
			if contactGroups[g] {
				return false, 0
			}
		}
		score += triggerScoreByExclusion
	}

	return true, score
}

const sqlSelectTriggersByOrg = `
SELECT ROW_TO_JSON(r) FROM (
             SELECT
                    t.id,
                    t.org_id,
                    t.flow_id,
                    t.trigger_type,
                    t.keywords,
                    t.match_type,
                    t.channel_id,
                    COALESCE(t.referrer_id, '') AS referrer_id,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT ig.contactgroup_id), NULL) AS include_group_ids,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT eg.contactgroup_id), NULL) AS exclude_group_ids
               FROM triggers_trigger t
    LEFT OUTER JOIN triggers_trigger_groups ig ON t.id = ig.trigger_id
    LEFT OUTER JOIN triggers_trigger_exclude_groups eg ON t.id = eg.trigger_id
              WHERE t.org_id = $1 AND t.is_active = TRUE AND t.is_archived = FALSE AND t.trigger_type != 'S'
           GROUP BY t.id
) r;`

const sqlSelectTriggersByContactIDs = `
    SELECT t.id AS id
      FROM triggers_trigger t
INNER JOIN triggers_trigger_contacts tc ON tc.trigger_id = t.id
     WHERE tc.contact_id = ANY($1) AND is_archived = FALSE`

const sqlArchiveEmptyTriggers = `
UPDATE triggers_trigger
   SET is_archived = TRUE
 WHERE id = ANY($1) AND
	NOT EXISTS (SELECT * FROM triggers_trigger_contacts WHERE trigger_id = triggers_trigger.id) AND
	NOT EXISTS (SELECT * FROM triggers_trigger_groups WHERE trigger_id = triggers_trigger.id) AND
	NOT EXISTS (SELECT * FROM triggers_trigger_exclude_groups WHERE trigger_id = triggers_trigger.id)`

// ArchiveContactTriggers removes the given contacts from any triggers and archives any triggers
// which reference only those contacts
func ArchiveContactTriggers(ctx context.Context, tx DBorTx, contactIDs []ContactID) error {
	// start by getting all the active triggers that reference these contacts
	rows, err := tx.QueryxContext(ctx, sqlSelectTriggersByContactIDs, pq.Array(contactIDs))
	if err != nil {
		return errors.Wrapf(err, "error finding triggers for contacts")
	}
	defer rows.Close()

	triggerIDs := make([]TriggerID, 0)
	for rows.Next() {
		var triggerID TriggerID
		err := rows.Scan(&triggerID)
		if err != nil {
			return errors.Wrapf(err, "error reading trigger ID")
		}
		triggerIDs = append(triggerIDs, triggerID)
	}

	// remove any references to these contacts in triggers
	_, err = tx.ExecContext(ctx, `DELETE FROM triggers_trigger_contacts WHERE contact_id = ANY($1)`, pq.Array(contactIDs))
	if err != nil {
		return errors.Wrapf(err, "error removing contacts from triggers")
	}

	// archive any of the original triggers which are now not referencing any contact or group
	_, err = tx.ExecContext(ctx, sqlArchiveEmptyTriggers, pq.Array(triggerIDs))
	if err != nil {
		return errors.Wrapf(err, "error archiving empty triggers")
	}

	return nil
}
