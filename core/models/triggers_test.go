package models_test

import (
	"testing"
	"time"

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
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	rt.DB.MustExec(`DELETE FROM triggers_trigger`)
	farmersGroup := testdata.InsertContactGroup(rt, testdata.Org1, assets.GroupUUID(uuids.New()), "Farmers", "")

	// create trigger for other org to ensure it isn't loaded
	testdata.InsertCatchallTrigger(rt, testdata.Org2, testdata.Org2Favorites, nil, nil, nil)

	tcs := []struct {
		id               models.TriggerID
		type_            models.TriggerType
		flowID           models.FlowID
		keywords         []string
		keywordMatchType models.MatchType
		referrerID       string
		includeGroups    []models.GroupID
		excludeGroups    []models.GroupID
		includeContacts  []models.ContactID
		channelID        models.ChannelID
	}{
		{
			id:               testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.Favorites, []string{"join"}, models.MatchFirst, nil, nil, nil),
			type_:            models.KeywordTriggerType,
			flowID:           testdata.Favorites.ID,
			keywords:         []string{"join"},
			keywordMatchType: models.MatchFirst,
		},
		{
			id:               testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.Favorites, []string{"join"}, models.MatchFirst, nil, nil, testdata.TwilioChannel),
			type_:            models.KeywordTriggerType,
			flowID:           testdata.Favorites.ID,
			keywords:         []string{"join"},
			keywordMatchType: models.MatchFirst,
			channelID:        testdata.TwilioChannel.ID,
		},
		{
			id:               testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.PickANumber, []string{"start"}, models.MatchOnly, []*testdata.Group{testdata.DoctorsGroup, testdata.TestersGroup}, []*testdata.Group{farmersGroup}, nil),
			type_:            models.KeywordTriggerType,
			flowID:           testdata.PickANumber.ID,
			keywords:         []string{"start"},
			keywordMatchType: models.MatchOnly,
			includeGroups:    []models.GroupID{testdata.DoctorsGroup.ID, testdata.TestersGroup.ID},
			excludeGroups:    []models.GroupID{farmersGroup.ID},
		},
		{
			id:            testdata.InsertIncomingCallTrigger(rt, testdata.Org1, testdata.Favorites, []*testdata.Group{testdata.DoctorsGroup, testdata.TestersGroup}, []*testdata.Group{farmersGroup}, nil),
			type_:         models.IncomingCallTriggerType,
			flowID:        testdata.Favorites.ID,
			includeGroups: []models.GroupID{testdata.DoctorsGroup.ID, testdata.TestersGroup.ID},
			excludeGroups: []models.GroupID{farmersGroup.ID},
		},
		{
			id:     testdata.InsertIncomingCallTrigger(rt, testdata.Org1, testdata.Favorites, []*testdata.Group{testdata.DoctorsGroup, testdata.TestersGroup}, []*testdata.Group{farmersGroup}, testdata.TwilioChannel),
			type_:  models.IncomingCallTriggerType,
			flowID: testdata.Favorites.ID,

			includeGroups: []models.GroupID{testdata.DoctorsGroup.ID, testdata.TestersGroup.ID},
			excludeGroups: []models.GroupID{farmersGroup.ID},
			channelID:     testdata.TwilioChannel.ID,
		},
		{
			id:     testdata.InsertMissedCallTrigger(rt, testdata.Org1, testdata.Favorites, nil),
			type_:  models.MissedCallTriggerType,
			flowID: testdata.Favorites.ID,
		},
		{
			id:        testdata.InsertNewConversationTrigger(rt, testdata.Org1, testdata.Favorites, testdata.TwilioChannel),
			type_:     models.NewConversationTriggerType,
			flowID:    testdata.Favorites.ID,
			channelID: testdata.TwilioChannel.ID,
		},
		{
			id:     testdata.InsertReferralTrigger(rt, testdata.Org1, testdata.Favorites, "", nil),
			type_:  models.ReferralTriggerType,
			flowID: testdata.Favorites.ID,
		},
		{
			id:         testdata.InsertReferralTrigger(rt, testdata.Org1, testdata.Favorites, "3256437635", testdata.TwilioChannel),
			type_:      models.ReferralTriggerType,
			flowID:     testdata.Favorites.ID,
			referrerID: "3256437635",
			channelID:  testdata.TwilioChannel.ID,
		},
		{
			id:     testdata.InsertCatchallTrigger(rt, testdata.Org1, testdata.Favorites, nil, nil, nil),
			type_:  models.CatchallTriggerType,
			flowID: testdata.Favorites.ID,
		},
		{
			id:        testdata.InsertCatchallTrigger(rt, testdata.Org1, testdata.Favorites, nil, nil, testdata.TwilioChannel),
			type_:     models.CatchallTriggerType,
			flowID:    testdata.Favorites.ID,
			channelID: testdata.TwilioChannel.ID,
		},
	}

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTriggers)
	require.NoError(t, err)

	require.Equal(t, len(tcs), len(oa.Triggers()))

	for i, tc := range tcs {
		actual := oa.Triggers()[i]

		assert.Equal(t, tc.id, actual.ID(), "id mismatch in trigger #%d", i)
		assert.Equal(t, tc.type_, actual.TriggerType(), "type mismatch in trigger #%d", i)
		assert.Equal(t, tc.flowID, actual.FlowID(), "flow id mismatch in trigger #%d", i)
		assert.Equal(t, tc.keywords, actual.Keywords(), "keywords mismatch in trigger #%d", i)
		assert.Equal(t, tc.keywordMatchType, actual.MatchType(), "match type mismatch in trigger #%d", i)
		assert.Equal(t, tc.referrerID, actual.ReferrerID(), "referrer id mismatch in trigger #%d", i)
		assert.ElementsMatch(t, tc.includeGroups, actual.IncludeGroupIDs(), "include groups mismatch in trigger #%d", i)
		assert.ElementsMatch(t, tc.excludeGroups, actual.ExcludeGroupIDs(), "exclude groups mismatch in trigger #%d", i)
		assert.ElementsMatch(t, tc.includeContacts, actual.ContactIDs(), "include contacts mismatch in trigger #%d", i)
		assert.Equal(t, tc.channelID, actual.ChannelID(), "channel id mismatch in trigger #%d", i)
	}
}

func TestFindMatchingMsgTrigger(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	rt.DB.MustExec(`DELETE FROM triggers_trigger`)

	joinID := testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.Favorites, []string{"join"}, models.MatchFirst, nil, nil, nil)
	joinTwilioOnlyID := testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.Favorites, []string{"join"}, models.MatchFirst, nil, nil, testdata.TwilioChannel)
	startTwilioOnlyID := testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.Favorites, []string{"start"}, models.MatchFirst, nil, nil, testdata.TwilioChannel)
	resistID := testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.SingleMessage, []string{"resist"}, models.MatchOnly, nil, nil, nil)
	resistTwilioOnlyID := testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.SingleMessage, []string{"resist"}, models.MatchOnly, nil, nil, testdata.TwilioChannel)
	emojiID := testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.PickANumber, []string{"👍"}, models.MatchFirst, nil, nil, nil)
	doctorsID := testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.SingleMessage, []string{"resist"}, models.MatchOnly, []*testdata.Group{testdata.DoctorsGroup}, nil, nil)
	doctorsAndNotTestersID := testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.SingleMessage, []string{"resist"}, models.MatchOnly, []*testdata.Group{testdata.DoctorsGroup}, []*testdata.Group{testdata.TestersGroup}, nil)
	doctorsCatchallID := testdata.InsertCatchallTrigger(rt, testdata.Org1, testdata.SingleMessage, []*testdata.Group{testdata.DoctorsGroup}, nil, nil)
	othersAllID := testdata.InsertCatchallTrigger(rt, testdata.Org1, testdata.SingleMessage, nil, nil, nil)

	// trigger for other org
	testdata.InsertCatchallTrigger(rt, testdata.Org2, testdata.Org2Favorites, nil, nil, nil)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTriggers)
	require.NoError(t, err)

	testdata.DoctorsGroup.Add(rt, testdata.Bob)
	testdata.TestersGroup.Add(rt, testdata.Bob)

	_, cathy, _ := testdata.Cathy.Load(rt, oa)
	_, george, _ := testdata.George.Load(rt, oa)
	_, bob, _ := testdata.Bob.Load(rt, oa)

	twilioChannels, _ := models.GetChannelsByID(ctx, rt.DB.DB, []models.ChannelID{testdata.TwilioChannel.ID})
	facebookChannels, _ := models.GetChannelsByID(ctx, rt.DB.DB, []models.ChannelID{testdata.FacebookChannel.ID})

	tcs := []struct {
		text              string
		channel           *models.Channel
		contact           *flows.Contact
		expectedTriggerID models.TriggerID
		expectedKeyword   string
	}{
		{" join ", nil, cathy, joinID, "join"},
		{"JOIN", nil, cathy, joinID, "join"},
		{"JOIN", twilioChannels[0], cathy, joinTwilioOnlyID, "join"},
		{"JOIN", facebookChannels[0], cathy, joinID, "join"},
		{"join this", nil, cathy, joinID, "join"},
		{"resist", nil, george, resistID, "resist"},
		{"resist", twilioChannels[0], george, resistTwilioOnlyID, "resist"},
		{"resist", nil, bob, doctorsID, "resist"},
		{"resist", twilioChannels[0], cathy, resistTwilioOnlyID, "resist"},
		{"resist", nil, cathy, doctorsAndNotTestersID, "resist"},
		{"resist this", nil, cathy, doctorsCatchallID, ""},
		{" 👍 ", nil, george, emojiID, "👍"},
		{"👍🏾", nil, george, emojiID, "👍"}, // is 👍 + 🏾
		{"😀👍", nil, george, othersAllID, ""},
		{"other", nil, cathy, doctorsCatchallID, ""},
		{"other", nil, george, othersAllID, ""},
		{"", nil, george, othersAllID, ""},
		{"start", twilioChannels[0], cathy, startTwilioOnlyID, "start"},
		{"start", facebookChannels[0], cathy, doctorsCatchallID, ""},
		{"start", twilioChannels[0], george, startTwilioOnlyID, "start"},
		{"start", facebookChannels[0], george, othersAllID, ""},
	}

	for _, tc := range tcs {
		trigger, keyword := models.FindMatchingMsgTrigger(oa, tc.channel, tc.contact, tc.text)

		assertTrigger(t, tc.expectedTriggerID, trigger, "trigger mismatch for %s sending '%s'", tc.contact.Name(), tc.text)
		assert.Equal(t, tc.expectedKeyword, keyword, "keyword mismatch for %s sending '%s'", tc.contact.Name(), tc.text)
	}
}

func TestFindMatchingIncomingCallTrigger(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	doctorsAndNotTestersTriggerID := testdata.InsertIncomingCallTrigger(rt, testdata.Org1, testdata.Favorites, []*testdata.Group{testdata.DoctorsGroup}, []*testdata.Group{testdata.TestersGroup}, nil)
	doctorsTriggerID := testdata.InsertIncomingCallTrigger(rt, testdata.Org1, testdata.Favorites, []*testdata.Group{testdata.DoctorsGroup}, nil, nil)
	notTestersTriggerID := testdata.InsertIncomingCallTrigger(rt, testdata.Org1, testdata.Favorites, nil, []*testdata.Group{testdata.TestersGroup}, nil)
	everyoneTriggerID := testdata.InsertIncomingCallTrigger(rt, testdata.Org1, testdata.Favorites, nil, nil, nil)
	specificChannelTriggerID := testdata.InsertIncomingCallTrigger(rt, testdata.Org1, testdata.Favorites, nil, nil, testdata.TwilioChannel)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTriggers)
	require.NoError(t, err)

	testdata.DoctorsGroup.Add(rt, testdata.Bob)
	testdata.TestersGroup.Add(rt, testdata.Bob, testdata.Alexandria)

	_, cathy, _ := testdata.Cathy.Load(rt, oa)
	_, bob, _ := testdata.Bob.Load(rt, oa)
	_, george, _ := testdata.George.Load(rt, oa)
	_, alexa, _ := testdata.Alexandria.Load(rt, oa)

	twilioChannels, _ := models.GetChannelsByID(ctx, rt.DB.DB, []models.ChannelID{testdata.TwilioChannel.ID})
	facebookChannels, _ := models.GetChannelsByID(ctx, rt.DB.DB, []models.ChannelID{testdata.FacebookChannel.ID})

	tcs := []struct {
		contact           *flows.Contact
		channel           *models.Channel
		expectedTriggerID models.TriggerID
	}{
		{cathy, twilioChannels[0], specificChannelTriggerID},        // specific channel
		{cathy, facebookChannels[0], doctorsAndNotTestersTriggerID}, // not matching channel, get the next best scored channel
		{cathy, nil, doctorsAndNotTestersTriggerID},                 // they're in doctors and not in testers
		{bob, nil, doctorsTriggerID},                                // they're in doctors and testers
		{george, nil, notTestersTriggerID},                          // they're not in doctors and not in testers
		{alexa, nil, everyoneTriggerID},                             // they're not in doctors but are in testers
	}

	for _, tc := range tcs {
		trigger := models.FindMatchingIncomingCallTrigger(oa, tc.channel, tc.contact)

		assertTrigger(t, tc.expectedTriggerID, trigger, "trigger mismatch for %s", tc.contact.Name())
	}
}

func TestFindMatchingMissedCallTrigger(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	testdata.InsertCatchallTrigger(rt, testdata.Org1, testdata.SingleMessage, nil, nil, nil)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTriggers)
	require.NoError(t, err)

	// no missed call trigger yet
	trigger := models.FindMatchingMissedCallTrigger(oa, nil)
	assert.Nil(t, trigger)

	triggerID := testdata.InsertMissedCallTrigger(rt, testdata.Org1, testdata.Favorites, nil)

	oa, err = models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTriggers)
	require.NoError(t, err)

	trigger = models.FindMatchingMissedCallTrigger(oa, nil)
	assertTrigger(t, triggerID, trigger)

	triggerIDwithChannel := testdata.InsertMissedCallTrigger(rt, testdata.Org1, testdata.Favorites, testdata.TwilioChannel)

	oa, err = models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTriggers)
	require.NoError(t, err)

	channels, _ := models.GetChannelsByID(ctx, rt.DB.DB, []models.ChannelID{testdata.TwilioChannel.ID})

	trigger = models.FindMatchingMissedCallTrigger(oa, channels[0])
	assertTrigger(t, triggerIDwithChannel, trigger)

}

func TestFindMatchingNewConversationTrigger(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	twilioTriggerID := testdata.InsertNewConversationTrigger(rt, testdata.Org1, testdata.Favorites, testdata.TwilioChannel)
	noChTriggerID := testdata.InsertNewConversationTrigger(rt, testdata.Org1, testdata.Favorites, nil)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTriggers)
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
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	fooID := testdata.InsertReferralTrigger(rt, testdata.Org1, testdata.Favorites, "foo", testdata.FacebookChannel)
	barID := testdata.InsertReferralTrigger(rt, testdata.Org1, testdata.Favorites, "bar", nil)
	bazID := testdata.InsertReferralTrigger(rt, testdata.Org1, testdata.Favorites, "", testdata.FacebookChannel)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTriggers)
	require.NoError(t, err)

	tcs := []struct {
		referrerID        string
		channelID         models.ChannelID
		expectedTriggerID models.TriggerID
	}{
		{"", testdata.TwilioChannel.ID, models.NilTriggerID},
		{"foo", testdata.TwilioChannel.ID, models.NilTriggerID},
		{"foo", testdata.FacebookChannel.ID, fooID},
		{"FOO", testdata.FacebookChannel.ID, fooID},
		{"bar", testdata.TwilioChannel.ID, barID},
		{"bar", testdata.FacebookChannel.ID, barID},
		{"zap", testdata.TwilioChannel.ID, models.NilTriggerID},
		{"zap", testdata.FacebookChannel.ID, bazID},
	}

	for i, tc := range tcs {
		channel := oa.ChannelByID(tc.channelID)
		trigger := models.FindMatchingReferralTrigger(oa, channel, tc.referrerID)

		assertTrigger(t, tc.expectedTriggerID, trigger, "trigger mismatch in test case #%d", i)
	}
}

func TestFindMatchingOptInTrigger(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	twilioTriggerID := testdata.InsertOptInTrigger(rt, testdata.Org1, testdata.Favorites, testdata.TwilioChannel)
	noChTriggerID := testdata.InsertOptInTrigger(rt, testdata.Org1, testdata.Favorites, nil)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTriggers)
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
		trigger := models.FindMatchingOptInTrigger(oa, channel)

		assertTrigger(t, tc.expectedTriggerID, trigger, "trigger mismatch in test case #%d", i)
	}
}

func TestFindMatchingOptOutTrigger(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	twilioTriggerID := testdata.InsertOptOutTrigger(rt, testdata.Org1, testdata.Favorites, testdata.TwilioChannel)
	noChTriggerID := testdata.InsertOptOutTrigger(rt, testdata.Org1, testdata.Favorites, nil)

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshTriggers)
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
		trigger := models.FindMatchingOptOutTrigger(oa, channel)

		assertTrigger(t, tc.expectedTriggerID, trigger, "trigger mismatch in test case #%d", i)
	}
}

func TestArchiveContactTriggers(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	everybodyID := testdata.InsertKeywordTrigger(rt, testdata.Org1, testdata.Favorites, []string{"join"}, models.MatchFirst, nil, nil, nil)
	cathyOnly1ID := testdata.InsertScheduledTrigger(rt, testdata.Org1, testdata.Favorites, testdata.InsertSchedule(rt, testdata.Org1, models.RepeatPeriodMonthly, time.Now()), nil, nil, []*testdata.Contact{testdata.Cathy})
	cathyOnly2ID := testdata.InsertScheduledTrigger(rt, testdata.Org1, testdata.Favorites, testdata.InsertSchedule(rt, testdata.Org1, models.RepeatPeriodMonthly, time.Now()), nil, nil, []*testdata.Contact{testdata.Cathy})
	cathyAndGeorgeID := testdata.InsertScheduledTrigger(rt, testdata.Org1, testdata.Favorites, testdata.InsertSchedule(rt, testdata.Org1, models.RepeatPeriodMonthly, time.Now()), nil, nil, []*testdata.Contact{testdata.Cathy, testdata.George})
	cathyAndGroupID := testdata.InsertScheduledTrigger(rt, testdata.Org1, testdata.Favorites, testdata.InsertSchedule(rt, testdata.Org1, models.RepeatPeriodMonthly, time.Now()), []*testdata.Group{testdata.DoctorsGroup}, nil, []*testdata.Contact{testdata.Cathy})
	georgeOnlyID := testdata.InsertScheduledTrigger(rt, testdata.Org1, testdata.Favorites, testdata.InsertSchedule(rt, testdata.Org1, models.RepeatPeriodMonthly, time.Now()), nil, nil, []*testdata.Contact{testdata.George})

	err := models.ArchiveContactTriggers(ctx, rt.DB, []models.ContactID{testdata.Cathy.ID, testdata.Bob.ID})
	require.NoError(t, err)

	assertTriggerArchived := func(id models.TriggerID, archived bool) {
		var isArchived bool
		rt.DB.Get(&isArchived, `SELECT is_archived FROM triggers_trigger WHERE id = $1`, id)
		assert.Equal(t, archived, isArchived, `is_archived mismatch for trigger %d`, id)
	}

	assertTriggerArchived(everybodyID, false)
	assertTriggerArchived(cathyOnly1ID, true)
	assertTriggerArchived(cathyOnly2ID, true)
	assertTriggerArchived(cathyAndGeorgeID, false)
	assertTriggerArchived(cathyAndGroupID, false)
	assertTriggerArchived(georgeOnlyID, false)
}

func assertTrigger(t *testing.T, expected models.TriggerID, actual *models.Trigger, msgAndArgs ...any) {
	if actual == nil {
		assert.Equal(t, expected, models.NilTriggerID, msgAndArgs...)
	} else {
		assert.Equal(t, expected, actual.ID(), msgAndArgs...)
	}
}
