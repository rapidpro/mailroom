package models

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
)

func insertTrigger(t *testing.T, db *sqlx.DB, active bool, flowID FlowID, triggerType TriggerType, keyword string, matchType MatchType, groupIDs []GroupID, referrerID string, channelID ChannelID) TriggerID {
	var triggerID TriggerID
	err := db.Get(&triggerID,
		`INSERT INTO triggers_trigger(is_active, created_on, modified_on, keyword, referrer_id, is_archived, 
									  flow_id, trigger_type, match_type, created_by_id, modified_by_id, org_id, channel_id)
		VALUES($1, now(), now(), $2, $6, false, $3, $4, $5, 1, 1, 1, $7) RETURNING id`, active, keyword, flowID, triggerType, matchType, referrerID, channelID)

	assert.NoError(t, err)

	// insert any group associations
	for _, g := range groupIDs {
		db.MustExec(`INSERT INTO triggers_trigger_groups(trigger_id, contactgroup_id) VALUES($1, $2)`, triggerID, g)
	}

	return triggerID
}

func TestChannelTriggers(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	fooID := insertTrigger(t, db, true, FavoritesFlowID, ReferralTriggerType, "", MatchFirst, nil, "foo", TwitterChannelID)
	barID := insertTrigger(t, db, true, FavoritesFlowID, ReferralTriggerType, "", MatchFirst, nil, "bar", NilChannelID)
	bazID := insertTrigger(t, db, true, FavoritesFlowID, ReferralTriggerType, "", MatchFirst, nil, "", TwitterChannelID)

	FlushCache()

	org, err := GetOrgAssets(ctx, db, Org1)
	assert.NoError(t, err)

	tcs := []struct {
		ReferrerID string
		Channel    ChannelID
		TriggerID  TriggerID
	}{
		{"", TwilioChannelID, NilTriggerID},
		{"foo", TwilioChannelID, NilTriggerID},
		{"foo", TwitterChannelID, fooID},
		{"bar", TwilioChannelID, barID},
		{"bar", TwitterChannelID, barID},
		{"zap", TwilioChannelID, NilTriggerID},
		{"zap", TwitterChannelID, bazID},
	}

	for i, tc := range tcs {
		channel := org.ChannelByID(tc.Channel)

		trigger := FindMatchingReferralTrigger(org, channel, tc.ReferrerID)
		if trigger == nil {
			assert.Equal(t, tc.TriggerID, NilTriggerID, "%d: did not get back expected trigger", i)
		} else {
			assert.Equal(t, tc.TriggerID, trigger.ID(), "%d: did not get back expected trigger", i)
		}
	}
}

func TestTriggers(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	joinID := insertTrigger(t, db, true, FavoritesFlowID, KeywordTriggerType, "join", MatchFirst, nil, "", NilChannelID)
	resistID := insertTrigger(t, db, true, SingleMessageFlowID, KeywordTriggerType, "resist", MatchOnly, nil, "", NilChannelID)
	farmersID := insertTrigger(t, db, true, SingleMessageFlowID, KeywordTriggerType, "resist", MatchOnly, []GroupID{DoctorsGroupID}, "", NilChannelID)
	farmersAllID := insertTrigger(t, db, true, SingleMessageFlowID, CatchallTriggerType, "", MatchOnly, []GroupID{DoctorsGroupID}, "", NilChannelID)
	othersAllID := insertTrigger(t, db, true, SingleMessageFlowID, CatchallTriggerType, "", MatchOnly, nil, "", NilChannelID)

	FlushCache()

	org, err := GetOrgAssets(ctx, db, Org1)
	assert.NoError(t, err)

	contactIDs := []ContactID{CathyID, GeorgeID}
	contacts, err := LoadContacts(ctx, db, org, contactIDs)
	assert.NoError(t, err)

	cathy, err := contacts[0].FlowContact(org)
	assert.NoError(t, err)

	greg, err := contacts[1].FlowContact(org)
	assert.NoError(t, err)

	tcs := []struct {
		Text      string
		Contact   *flows.Contact
		TriggerID TriggerID
	}{
		{"join", cathy, joinID},
		{"join this", cathy, joinID},
		{"resist", greg, resistID},
		{"resist", cathy, farmersID},
		{"resist this", cathy, farmersAllID},
		{"other", cathy, farmersAllID},
		{"other", greg, othersAllID},
		{"", greg, othersAllID},
	}

	for i, tc := range tcs {
		trigger := FindMatchingMsgTrigger(org, tc.Contact, tc.Text)
		if trigger == nil {
			assert.Equal(t, tc.TriggerID, TriggerID(0), "%d: did not get back expected trigger", i)
		} else {
			assert.Equal(t, tc.TriggerID, trigger.ID(), "%d: did not get back expected trigger", i)
		}
	}
}
