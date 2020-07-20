package models

import (
	"fmt"
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func insertTrigger(t *testing.T, db *sqlx.DB, active bool, flowID FlowID, triggerType TriggerType, keyword string, matchType MatchType, groupIDs []GroupID, contactIDs []ContactID, referrerID string, channelID ChannelID) TriggerID {
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

	// insert any contact associations
	for _, c := range contactIDs {
		db.MustExec(`INSERT INTO triggers_trigger_contacts(trigger_id, contact_id) VALUES($1, $2)`, triggerID, c)
	}

	return triggerID
}

func TestChannelTriggers(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	fooID := insertTrigger(t, db, true, FavoritesFlowID, ReferralTriggerType, "", MatchFirst, nil, nil, "foo", TwitterChannelID)
	barID := insertTrigger(t, db, true, FavoritesFlowID, ReferralTriggerType, "", MatchFirst, nil, nil, "bar", NilChannelID)
	bazID := insertTrigger(t, db, true, FavoritesFlowID, ReferralTriggerType, "", MatchFirst, nil, nil, "", TwitterChannelID)

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

	joinID := insertTrigger(t, db, true, FavoritesFlowID, KeywordTriggerType, "join", MatchFirst, nil, nil, "", NilChannelID)
	resistID := insertTrigger(t, db, true, SingleMessageFlowID, KeywordTriggerType, "resist", MatchOnly, nil, nil, "", NilChannelID)
	farmersID := insertTrigger(t, db, true, SingleMessageFlowID, KeywordTriggerType, "resist", MatchOnly, []GroupID{DoctorsGroupID}, nil, "", NilChannelID)
	farmersAllID := insertTrigger(t, db, true, SingleMessageFlowID, CatchallTriggerType, "", MatchOnly, []GroupID{DoctorsGroupID}, nil, "", NilChannelID)
	othersAllID := insertTrigger(t, db, true, SingleMessageFlowID, CatchallTriggerType, "", MatchOnly, nil, nil, "", NilChannelID)

	FlushCache()

	org, err := GetOrgAssets(ctx, db, Org1)
	assert.NoError(t, err)

	contactIDs := []ContactID{CathyID, GeorgeID}
	contacts, err := LoadContacts(ctx, db, org, contactIDs)
	assert.NoError(t, err)

	cathy, err := contacts[0].FlowContact(org)
	assert.NoError(t, err)

	george, err := contacts[1].FlowContact(org)
	assert.NoError(t, err)

	tcs := []struct {
		Text      string
		Contact   *flows.Contact
		TriggerID TriggerID
	}{
		{"join", cathy, joinID},
		{"join this", cathy, joinID},
		{"resist", george, resistID},
		{"resist", cathy, farmersID},
		{"resist this", cathy, farmersAllID},
		{"other", cathy, farmersAllID},
		{"other", george, othersAllID},
		{"", george, othersAllID},
	}

	for _, tc := range tcs {
		testID := fmt.Sprintf("'%s' sent by %s", tc.Text, tc.Contact.Name())

		actualTriggerID := NilTriggerID
		actualTrigger := FindMatchingMsgTrigger(org, tc.Contact, tc.Text)
		if actualTrigger != nil {
			actualTriggerID = actualTrigger.ID()
		}

		assert.Equal(t, tc.TriggerID, actualTriggerID, "did not get back expected trigger for %s", testID)
	}
}

func TestArchiveContactTriggers(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	everybodyID := insertTrigger(t, db, true, FavoritesFlowID, KeywordTriggerType, "join", MatchFirst, nil, nil, "", NilChannelID)
	cathyOnly1ID := insertTrigger(t, db, true, FavoritesFlowID, KeywordTriggerType, "join", MatchFirst, nil, []ContactID{CathyID}, "", NilChannelID)
	cathyOnly2ID := insertTrigger(t, db, true, FavoritesFlowID, KeywordTriggerType, "this", MatchOnly, nil, []ContactID{CathyID}, "", NilChannelID)
	cathyAndGeorgeID := insertTrigger(t, db, true, FavoritesFlowID, KeywordTriggerType, "join", MatchFirst, nil, []ContactID{CathyID, GeorgeID}, "", NilChannelID)
	cathyAndGroupID := insertTrigger(t, db, true, FavoritesFlowID, KeywordTriggerType, "join", MatchFirst, []GroupID{DoctorsGroupID}, []ContactID{CathyID}, "", NilChannelID)
	georgeOnlyID := insertTrigger(t, db, true, FavoritesFlowID, KeywordTriggerType, "join", MatchFirst, nil, []ContactID{GeorgeID}, "", NilChannelID)

	err := ArchiveContactTriggers(ctx, db, []ContactID{CathyID, BobID})
	require.NoError(t, err)

	assertTriggerArchived := func(id TriggerID, archived bool) {
		var isArchived bool
		db.Get(&isArchived, `SELECT is_archived FROM triggers_trigger WHERE id = $1`, id)
		assert.Equal(t, archived, isArchived, `is_archived mismatch for trigger %d`, id)
	}

	assertTriggerArchived(everybodyID, false)
	assertTriggerArchived(cathyOnly1ID, true)
	assertTriggerArchived(cathyOnly2ID, true)
	assertTriggerArchived(cathyAndGeorgeID, false)
	assertTriggerArchived(cathyAndGroupID, false)
	assertTriggerArchived(georgeOnlyID, false)
}
