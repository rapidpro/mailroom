package models_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func insertTrigger(t *testing.T, db *sqlx.DB, active bool, flowID models.FlowID, triggerType models.TriggerType, keyword string, matchType models.MatchType, groupIDs []models.GroupID, contactIDs []models.ContactID, referrerID string, channelID models.ChannelID) models.TriggerID {
	var triggerID models.TriggerID
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

	fooID := insertTrigger(t, db, true, testdata.Favorites.ID, models.ReferralTriggerType, "", models.MatchFirst, nil, nil, "foo", testdata.TwitterChannel.ID)
	barID := insertTrigger(t, db, true, testdata.Favorites.ID, models.ReferralTriggerType, "", models.MatchFirst, nil, nil, "bar", models.NilChannelID)
	bazID := insertTrigger(t, db, true, testdata.Favorites.ID, models.ReferralTriggerType, "", models.MatchFirst, nil, nil, "", testdata.TwitterChannel.ID)

	models.FlushCache()

	org, err := models.GetOrgAssets(ctx, db, testdata.Org1.ID)
	assert.NoError(t, err)

	tcs := []struct {
		ReferrerID string
		Channel    models.ChannelID
		TriggerID  models.TriggerID
	}{
		{"", testdata.TwilioChannel.ID, models.NilTriggerID},
		{"foo", testdata.TwilioChannel.ID, models.NilTriggerID},
		{"foo", testdata.TwitterChannel.ID, fooID},
		{"FOO", testdata.TwitterChannel.ID, fooID},
		{"bar", testdata.TwilioChannel.ID, barID},
		{"bar", testdata.TwitterChannel.ID, barID},
		{"zap", testdata.TwilioChannel.ID, models.NilTriggerID},
		{"zap", testdata.TwitterChannel.ID, bazID},
	}

	for i, tc := range tcs {
		channel := org.ChannelByID(tc.Channel)

		trigger := models.FindMatchingReferralTrigger(org, channel, tc.ReferrerID)
		if trigger == nil {
			assert.Equal(t, tc.TriggerID, models.NilTriggerID, "%d: did not get back expected trigger", i)
		} else {
			assert.Equal(t, tc.TriggerID, trigger.ID(), "%d: did not get back expected trigger", i)
		}
	}
}

func TestTriggers(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	joinID := insertTrigger(t, db, true, testdata.Favorites.ID, models.KeywordTriggerType, "join", models.MatchFirst, nil, nil, "", models.NilChannelID)
	resistID := insertTrigger(t, db, true, testdata.SingleMessage.ID, models.KeywordTriggerType, "resist", models.MatchOnly, nil, nil, "", models.NilChannelID)
	farmersID := insertTrigger(t, db, true, testdata.SingleMessage.ID, models.KeywordTriggerType, "resist", models.MatchOnly, []models.GroupID{testdata.DoctorsGroup.ID}, nil, "", models.NilChannelID)
	farmersAllID := insertTrigger(t, db, true, testdata.SingleMessage.ID, models.CatchallTriggerType, "", models.MatchOnly, []models.GroupID{testdata.DoctorsGroup.ID}, nil, "", models.NilChannelID)
	othersAllID := insertTrigger(t, db, true, testdata.SingleMessage.ID, models.CatchallTriggerType, "", models.MatchOnly, nil, nil, "", models.NilChannelID)

	models.FlushCache()

	org, err := models.GetOrgAssets(ctx, db, testdata.Org1.ID)
	assert.NoError(t, err)

	contactIDs := []models.ContactID{testdata.Cathy.ID, testdata.George.ID}
	contacts, err := models.LoadContacts(ctx, db, org, contactIDs)
	assert.NoError(t, err)

	cathy, err := contacts[0].FlowContact(org)
	assert.NoError(t, err)

	george, err := contacts[1].FlowContact(org)
	assert.NoError(t, err)

	tcs := []struct {
		Text      string
		Contact   *flows.Contact
		TriggerID models.TriggerID
	}{
		{"join", cathy, joinID},
		{"JOIN", cathy, joinID},
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

		actualTriggerID := models.NilTriggerID
		actualTrigger := models.FindMatchingMsgTrigger(org, tc.Contact, tc.Text)
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

	everybodyID := insertTrigger(t, db, true, testdata.Favorites.ID, models.KeywordTriggerType, "join", models.MatchFirst, nil, nil, "", models.NilChannelID)
	cathyOnly1ID := insertTrigger(t, db, true, testdata.Favorites.ID, models.KeywordTriggerType, "join", models.MatchFirst, nil, []models.ContactID{testdata.Cathy.ID}, "", models.NilChannelID)
	cathyOnly2ID := insertTrigger(t, db, true, testdata.Favorites.ID, models.KeywordTriggerType, "this", models.MatchOnly, nil, []models.ContactID{testdata.Cathy.ID}, "", models.NilChannelID)
	cathyAndGeorgeID := insertTrigger(t, db, true, testdata.Favorites.ID, models.KeywordTriggerType, "join", models.MatchFirst, nil, []models.ContactID{testdata.Cathy.ID, testdata.George.ID}, "", models.NilChannelID)
	cathyAndGroupID := insertTrigger(t, db, true, testdata.Favorites.ID, models.KeywordTriggerType, "join", models.MatchFirst, []models.GroupID{testdata.DoctorsGroup.ID}, []models.ContactID{testdata.Cathy.ID}, "", models.NilChannelID)
	georgeOnlyID := insertTrigger(t, db, true, testdata.Favorites.ID, models.KeywordTriggerType, "join", models.MatchFirst, nil, []models.ContactID{testdata.George.ID}, "", models.NilChannelID)

	err := models.ArchiveContactTriggers(ctx, db, []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID})
	require.NoError(t, err)

	assertTriggerArchived := func(id models.TriggerID, archived bool) {
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
