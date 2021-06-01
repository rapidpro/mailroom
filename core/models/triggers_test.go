package models_test

import (
	"fmt"
	"testing"

	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadTriggers(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	db.MustExec(`DELETE FROM triggers_trigger`)
	farmersGroupID := testdata.InsertContactGroup(t, db, testdata.Org1.ID, assets.GroupUUID(uuids.New()), "Farmers", "")

	tcs := []struct {
		id               models.TriggerID
		type_            models.TriggerType
		flowID           models.FlowID
		keyword          string
		keywordMatchType models.MatchType
		referrerID       string
		includeGroups    []models.GroupID
		excludeGroups    []models.GroupID
		includeContacts  []models.ContactID
		channelID        models.ChannelID
	}{
		{
			id:               testdata.InsertKeywordTrigger(t, db, testdata.Favorites.ID, "join", models.MatchFirst, nil, nil),
			type_:            models.KeywordTriggerType,
			flowID:           testdata.Favorites.ID,
			keyword:          "join",
			keywordMatchType: models.MatchFirst,
		},
		{
			id: testdata.InsertKeywordTrigger(
				t, db, testdata.PickANumber.ID, "start", models.MatchOnly,
				[]models.GroupID{testdata.DoctorsGroup.ID, testdata.TestersGroup.ID},
				[]models.GroupID{farmersGroupID},
			),
			type_:            models.KeywordTriggerType,
			flowID:           testdata.PickANumber.ID,
			keyword:          "start",
			keywordMatchType: models.MatchOnly,
			includeGroups:    []models.GroupID{testdata.DoctorsGroup.ID, testdata.TestersGroup.ID},
			excludeGroups:    []models.GroupID{farmersGroupID},
		},
		{
			id:            testdata.InsertIncomingCallTrigger(t, db, testdata.Favorites.ID, []models.GroupID{testdata.DoctorsGroup.ID, testdata.TestersGroup.ID}, []models.GroupID{farmersGroupID}),
			type_:         models.IncomingCallTriggerType,
			flowID:        testdata.Favorites.ID,
			includeGroups: []models.GroupID{testdata.DoctorsGroup.ID, testdata.TestersGroup.ID},
			excludeGroups: []models.GroupID{farmersGroupID},
		},
		{
			id:     testdata.InsertMissedCallTrigger(t, db, testdata.Favorites.ID),
			type_:  models.MissedCallTriggerType,
			flowID: testdata.Favorites.ID,
		},
		{
			id:        testdata.InsertNewConversationTrigger(t, db, testdata.Favorites.ID, testdata.TwilioChannel.ID),
			type_:     models.NewConversationTriggerType,
			flowID:    testdata.Favorites.ID,
			channelID: testdata.TwilioChannel.ID,
		},
		{
			id:     testdata.InsertReferralTrigger(t, db, testdata.Favorites.ID, "", models.NilChannelID),
			type_:  models.ReferralTriggerType,
			flowID: testdata.Favorites.ID,
		},
		{
			id:         testdata.InsertReferralTrigger(t, db, testdata.Favorites.ID, "3256437635", testdata.TwilioChannel.ID),
			type_:      models.ReferralTriggerType,
			flowID:     testdata.Favorites.ID,
			referrerID: "3256437635",
			channelID:  testdata.TwilioChannel.ID,
		},
		{
			id:     testdata.InsertCatchallTrigger(t, db, testdata.Favorites.ID, nil, nil),
			type_:  models.CatchallTriggerType,
			flowID: testdata.Favorites.ID,
		},
	}

	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, testdata.Org1.ID, models.RefreshTriggers)
	require.NoError(t, err)

	require.Equal(t, len(tcs), len(oa.Triggers()))

	for i, tc := range tcs {
		actual := oa.Triggers()[i]

		assert.Equal(t, tc.id, actual.ID(), "id mismatch in trigger #%d", i)
		assert.Equal(t, tc.type_, actual.TriggerType(), "type mismatch in trigger #%d", i)
		assert.Equal(t, tc.flowID, actual.FlowID(), "flow id mismatch in trigger #%d", i)
		assert.Equal(t, tc.keyword, actual.Keyword(), "keyword mismatch in trigger #%d", i)
		assert.Equal(t, tc.keywordMatchType, actual.MatchType(), "match type mismatch in trigger #%d", i)
		assert.Equal(t, tc.referrerID, actual.ReferrerID(), "referrer id mismatch in trigger #%d", i)
		assert.ElementsMatch(t, tc.includeGroups, actual.IncludeGroupIDs(), "include groups mismatch in trigger #%d", i)
		assert.ElementsMatch(t, tc.excludeGroups, actual.ExcludeGroupIDs(), "exclude groups mismatch in trigger #%d", i)
		assert.ElementsMatch(t, tc.includeContacts, actual.ContactIDs(), "include contacts mismatch in trigger #%d", i)
		assert.Equal(t, tc.channelID, actual.ChannelID(), "channel id mismatch in trigger #%d", i)
	}
}

func TestFindMatchingMsgTrigger(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	joinID := testdata.InsertKeywordTrigger(t, db, testdata.Favorites.ID, "join", models.MatchFirst, nil, nil)
	resistID := testdata.InsertKeywordTrigger(t, db, testdata.SingleMessage.ID, "resist", models.MatchOnly, nil, nil)
	farmersID := testdata.InsertKeywordTrigger(t, db, testdata.SingleMessage.ID, "resist", models.MatchOnly, []models.GroupID{testdata.DoctorsGroup.ID}, nil)
	farmersAllID := testdata.InsertCatchallTrigger(t, db, testdata.SingleMessage.ID, []models.GroupID{testdata.DoctorsGroup.ID}, nil)
	othersAllID := testdata.InsertCatchallTrigger(t, db, testdata.SingleMessage.ID, nil, nil)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, testdata.Org1.ID, models.RefreshTriggers)
	require.NoError(t, err)

	_, cathy := testdata.Cathy.Load(t, db, oa)
	_, george := testdata.George.Load(t, db, oa)

	tcs := []struct {
		text              string
		contact           *flows.Contact
		expectedTriggerID models.TriggerID
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
		testID := fmt.Sprintf("'%s' sent by %s", tc.text, tc.contact.Name())
		trigger := models.FindMatchingMsgTrigger(oa, tc.contact, tc.text)

		assertTrigger(t, tc.expectedTriggerID, trigger, testID)
	}
}

func TestFindMatchingIncomingCallTrigger(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	doctorsAndNotTestersTriggerID := testdata.InsertIncomingCallTrigger(t, db, testdata.Favorites.ID, []models.GroupID{testdata.DoctorsGroup.ID}, []models.GroupID{testdata.TestersGroup.ID})
	doctorsTriggerID := testdata.InsertIncomingCallTrigger(t, db, testdata.Favorites.ID, []models.GroupID{testdata.DoctorsGroup.ID}, nil)
	notTestersTriggerID := testdata.InsertIncomingCallTrigger(t, db, testdata.Favorites.ID, nil, []models.GroupID{testdata.TestersGroup.ID})
	everyoneTriggerID := testdata.InsertIncomingCallTrigger(t, db, testdata.Favorites.ID, nil, nil)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, testdata.Org1.ID, models.RefreshTriggers)
	require.NoError(t, err)

	testdata.DoctorsGroup.Add(db, testdata.Bob.ID)
	testdata.TestersGroup.Add(db, testdata.Bob.ID, testdata.Alexandria.ID)

	cathy, _ := testdata.Cathy.Load(t, db, oa)
	bob, _ := testdata.Bob.Load(t, db, oa)
	george, _ := testdata.George.Load(t, db, oa)
	alexa, _ := testdata.Alexandria.Load(t, db, oa)

	tcs := []struct {
		contact           *models.Contact
		expectedTriggerID models.TriggerID
	}{
		{cathy, doctorsAndNotTestersTriggerID}, // they're in doctors and not in testers
		{bob, doctorsTriggerID},                // they're in doctors and testers
		{george, notTestersTriggerID},          // they're not in doctors and not in testers
		{alexa, everyoneTriggerID},             // they're not in doctors but are in testers
	}

	for _, tc := range tcs {
		trigger := models.FindMatchingIncomingCallTrigger(oa, tc.contact)

		assertTrigger(t, tc.expectedTriggerID, trigger, "trigger mismatch for %s", tc.contact.Name())
	}
}

func TestFindMatchingNewConversationTrigger(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	twilioTriggerID := testdata.InsertNewConversationTrigger(t, db, testdata.Favorites.ID, testdata.TwilioChannel.ID)
	noChTriggerID := testdata.InsertNewConversationTrigger(t, db, testdata.Favorites.ID, models.NilChannelID)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, testdata.Org1.ID, models.RefreshTriggers)
	require.NoError(t, err)

	tcs := []struct {
		channelID         models.ChannelID
		expectedTriggerID models.TriggerID
	}{
		{testdata.TwilioChannel.ID, twilioTriggerID},
		{testdata.VonageChannel.ID, noChTriggerID},
	}

	for i, tc := range tcs {
		channel := oa.ChannelByID(tc.channelID)
		trigger := models.FindMatchingNewConversationTrigger(oa, channel)

		assertTrigger(t, tc.expectedTriggerID, trigger, "trigger mismatch in test case #%d", i)
	}
}

func TestFindMatchingReferralTrigger(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	fooID := testdata.InsertReferralTrigger(t, db, testdata.Favorites.ID, "foo", testdata.TwitterChannel.ID)
	barID := testdata.InsertReferralTrigger(t, db, testdata.Favorites.ID, "bar", models.NilChannelID)
	bazID := testdata.InsertReferralTrigger(t, db, testdata.Favorites.ID, "", testdata.TwitterChannel.ID)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, db, testdata.Org1.ID, models.RefreshTriggers)
	require.NoError(t, err)

	tcs := []struct {
		referrerID        string
		channelID         models.ChannelID
		expectedTriggerID models.TriggerID
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
		channel := oa.ChannelByID(tc.channelID)
		trigger := models.FindMatchingReferralTrigger(oa, channel, tc.referrerID)

		assertTrigger(t, tc.expectedTriggerID, trigger, "trigger mismatch in test case #%d", i)
	}
}

func TestArchiveContactTriggers(t *testing.T) {
	testsuite.Reset()
	db := testsuite.DB()
	ctx := testsuite.CTX()

	everybodyID := testdata.InsertKeywordTrigger(t, db, testdata.Favorites.ID, "join", models.MatchFirst, nil, nil)
	cathyOnly1ID := testdata.InsertScheduledTrigger(t, db, testdata.Favorites.ID, nil, nil, []models.ContactID{testdata.Cathy.ID})
	cathyOnly2ID := testdata.InsertScheduledTrigger(t, db, testdata.Favorites.ID, nil, nil, []models.ContactID{testdata.Cathy.ID})
	cathyAndGeorgeID := testdata.InsertScheduledTrigger(t, db, testdata.Favorites.ID, nil, nil, []models.ContactID{testdata.Cathy.ID, testdata.George.ID})
	cathyAndGroupID := testdata.InsertScheduledTrigger(t, db, testdata.Favorites.ID, []models.GroupID{testdata.DoctorsGroup.ID}, nil, []models.ContactID{testdata.Cathy.ID})
	georgeOnlyID := testdata.InsertScheduledTrigger(t, db, testdata.Favorites.ID, nil, nil, []models.ContactID{testdata.George.ID})

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

func assertTrigger(t *testing.T, expected models.TriggerID, actual *models.Trigger, msgAndArgs ...interface{}) {
	if actual == nil {
		assert.Equal(t, expected, models.NilTriggerID, msgAndArgs...)
	} else {
		assert.Equal(t, expected, actual.ID(), msgAndArgs...)
	}
}
