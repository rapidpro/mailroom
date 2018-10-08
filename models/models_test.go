package models

import (
	"os"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/definition"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/stretchr/testify/assert"
	null "gopkg.in/guregu/null.v3"
)

// Custom entry point so we can reset our database
func TestMain(m *testing.M) {
	testsuite.Reset()
	os.Exit(m.Run())
}

func TestChannels(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	// add some prefixes to channel 2
	db.MustExec(`UPDATE channels_channel SET config = '{"matching_prefixes": ["250", "251"]}' WHERE id = 2`)

	// make channel 3 have a parent of channel 1
	db.MustExec(`UPDATE channels_channel SET parent_id = 1 WHERE id = 3`)

	channels, err := loadChannels(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID       ChannelID
		UUID     assets.ChannelUUID
		Name     string
		Address  string
		Schemes  []string
		Roles    []assets.ChannelRole
		Prefixes []string
		Parent   *assets.ChannelReference
	}{
		{ChannelID(1), assets.ChannelUUID("ac4c718a-db3f-4d8a-ae43-321f1a5bd44a"), "Android", "1234",
			[]string{"tel"}, []assets.ChannelRole{"send", "receive"}, nil, nil},
		{ChannelID(2), assets.ChannelUUID("c534272e-817d-4a78-a70c-f21df34407f8"), "Nexmo", "2345",
			[]string{"tel"}, []assets.ChannelRole{"send", "receive"}, []string{"250", "251"}, nil},
		{ChannelID(3), assets.ChannelUUID("0b10b271-a4ec-480f-abed-b4a197490590"), "Twitter", "my_handle", []string{"twitter"}, []assets.ChannelRole{"send", "receive"}, nil,
			assets.NewChannelReference(assets.ChannelUUID("ac4c718a-db3f-4d8a-ae43-321f1a5bd44a"), "Android")},
	}

	assert.Equal(t, len(tcs), len(channels))
	for i, tc := range tcs {
		channel := channels[i].(*Channel)
		assert.Equal(t, tc.UUID, channel.UUID())
		assert.Equal(t, tc.ID, channel.ID())
		assert.Equal(t, tc.Name, channel.Name())
		assert.Equal(t, tc.Address, channel.Address())
		assert.Equal(t, tc.Roles, channel.Roles())
		assert.Equal(t, tc.Schemes, channel.Schemes())
		assert.Equal(t, tc.Prefixes, channel.MatchPrefixes())
		assert.Equal(t, tc.Parent, channel.Parent())
	}

}

func TestFields(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	fields, err := loadFields(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		Key       string
		Name      string
		ValueType assets.FieldType
	}{
		{"age", "Age", assets.FieldTypeNumber},
		{"district", "District", assets.FieldTypeDistrict},
		{"gender", "Gender", assets.FieldTypeText},
		{"joined", "Joined On", assets.FieldTypeDatetime},
		{"state", "State", assets.FieldTypeState},
		{"ward", "Ward", assets.FieldTypeWard},
	}

	assert.Equal(t, 6, len(fields))
	for i, tc := range tcs {
		assert.Equal(t, tc.Key, fields[i].Key())
		assert.Equal(t, tc.Name, fields[i].Name())
		assert.Equal(t, tc.ValueType, fields[i].Type())
	}
}

func TestFlows(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	tcs := []struct {
		FlowID   FlowID
		FlowUUID assets.FlowUUID
		Name     string
		Found    bool
	}{
		{FlowID(1), assets.FlowUUID("51e3c67d-8483-449c-abf7-25e50686f0db"), "Favorites", true},
	}

	for _, tc := range tcs {
		flow, err := loadFlowByUUID(ctx, db, tc.FlowUUID)
		assert.NoError(t, err)
		if tc.Found {
			assert.Equal(t, tc.Name, flow.Name())
			assert.Equal(t, tc.FlowID, flow.ID())
			assert.Equal(t, tc.FlowUUID, flow.UUID())

			_, err := definition.ReadFlow(flow.Definition())
			assert.NoError(t, err)
		}
	}
}

func TestMsgs(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rp := testsuite.RP()

	orgID := OrgID(1)
	channels, err := loadChannels(ctx, db, orgID)
	assert.NoError(t, err)

	channel := channels[1].(*Channel)
	chanUUID := assets.ChannelUUID(utils.UUID("c534272e-817d-4a78-a70c-f21df34407f8"))

	tcs := []struct {
		ChannelUUID  assets.ChannelUUID
		Channel      *Channel
		Text         string
		ContactID    flows.ContactID
		URN          urns.URN
		ContactURNID ContactURNID
		Attachments  flows.AttachmentList
		QuickReplies []string
		Metadata     null.String
		HasErr       bool
	}{
		{chanUUID, channel, "missing urn id", flows.ContactID(42), urns.URN("tel:+250700000001"), ContactURNID(0),
			nil, nil, null.NewString("", false), true},
		{chanUUID, channel, "test outgoing", flows.ContactID(42), urns.URN("tel:+250700000001?id=42"), ContactURNID(42),
			nil, []string{"yes", "no"}, null.NewString(`{"quick_replies":["yes","no"]}`, true), false},
		{chanUUID, channel, "test outgoing", flows.ContactID(42), urns.URN("tel:+250700000001?id=42"), ContactURNID(42),
			flows.AttachmentList([]flows.Attachment{flows.Attachment("image/jpeg:https://dl-foo.com/image.jpg")}), nil, null.NewString("", false), false},
	}

	now := time.Now()
	time.Sleep(time.Millisecond * 10)

	for _, tc := range tcs {
		tx, err := db.BeginTxx(ctx, nil)
		assert.NoError(t, err)

		flowMsg := flows.NewMsgOut(tc.URN, assets.NewChannelReference(tc.ChannelUUID, "Test Channel"), tc.Text, tc.Attachments, tc.QuickReplies)
		msg, err := NewOutgoingMsg(ctx, tx, rp, orgID, tc.Channel, tc.ContactID, flowMsg, now)

		if err == nil {
			err = BulkSQL(ctx, "insert msgs sq", tx, InsertMsgSQL, []interface{}{msg})
			assert.NoError(t, err)
			assert.Equal(t, orgID, msg.OrgID())
			assert.Equal(t, tc.Text, msg.Text())
			assert.Equal(t, tc.ContactID, msg.ContactID())
			assert.Equal(t, tc.Channel, msg.Channel())
			assert.Equal(t, tc.ChannelUUID, msg.ChannelUUID())
			assert.Equal(t, tc.URN, msg.URN())
			assert.Equal(t, tc.ContactURNID, msg.ContactURNID())
			assert.Equal(t, tc.Metadata, msg.Metadata)
			assert.Equal(t, now, msg.CreatedOn())
			assert.True(t, msg.ID() > 0)
			assert.True(t, msg.QueuedOn().After(now))
			assert.True(t, msg.ModifiedOn().After(now))
		} else {
			if !tc.HasErr {
				assert.Fail(t, "unexpected error: %s", err.Error())
			}
		}
		tx.Rollback()
	}
}

func TestOrgs(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	tx, err := db.BeginTxx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	tx.MustExec("UPDATE orgs_org SET language = 'eng' WHERE id = 2;")
	tx.MustExec(`INSERT INTO orgs_language(is_active, created_on, modified_on, name, iso_code, created_by_id, modified_by_id, org_id) 
				                    VALUES(TRUE, NOW(), NOW(), 'French', 'fra', 1, 1, 2);`)

	org, err := loadOrg(ctx, tx, 1)
	assert.NoError(t, err)

	assert.Equal(t, OrgID(1), org.ID())
	assert.Equal(t, utils.DateFormatDayMonthYear, org.DateFormat())
	assert.Equal(t, utils.TimeFormatHourMinute, org.TimeFormat())
	assert.Equal(t, utils.RedactionPolicyNone, org.RedactionPolicy())
	tz, _ := time.LoadLocation("Europe/Copenhagen")
	assert.Equal(t, tz, org.Timezone())
	assert.Equal(t, 0, len(org.AllowedLanguages()))

	org, err = loadOrg(ctx, tx, 2)
	assert.NoError(t, err)
	assert.Equal(t, []utils.Language{"eng", "fra"}, org.AllowedLanguages())

	_, err = loadOrg(ctx, tx, 99)
	assert.Error(t, err)
}

func TestResthooks(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	tx, err := db.BeginTxx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	tx.MustExec(`INSERT INTO api_resthook(is_active, created_on, modified_on, slug, created_by_id, modified_by_id, org_id)
								   VALUES(TRUE, NOW(), NOW(), 'registration', 1, 1, 1);`)
	tx.MustExec(`INSERT INTO api_resthook(is_active, created_on, modified_on, slug, created_by_id, modified_by_id, org_id)
								   VALUES(TRUE, NOW(), NOW(), 'block', 1, 1, 1);`)
	tx.MustExec(`INSERT INTO api_resthooksubscriber(is_active, created_on, modified_on, target_url, created_by_id, modified_by_id, resthook_id)
											 VALUES(TRUE, NOW(), NOW(), 'https://foo.bar', 1, 1, 2);`)
	tx.MustExec(`INSERT INTO api_resthooksubscriber(is_active, created_on, modified_on, target_url, created_by_id, modified_by_id, resthook_id)
	                                         VALUES(TRUE, NOW(), NOW(), 'https://bar.foo', 1, 1, 2);`)

	resthooks, err := loadResthooks(ctx, tx, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID          ResthookID
		Slug        string
		Subscribers []string
	}{
		{ResthookID(2), "block", []string{"https://bar.foo", "https://foo.bar"}},
		{ResthookID(1), "registration", nil},
	}

	assert.Equal(t, 2, len(resthooks))
	for i, tc := range tcs {
		resthook := resthooks[i].(*Resthook)
		assert.Equal(t, tc.ID, resthook.ID())
		assert.Equal(t, tc.Slug, resthook.Slug())
		assert.Equal(t, tc.Subscribers, resthook.Subscribers())
	}
}

func TestTopups(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()
	rc := testsuite.RC()
	defer rc.Close()

	tx, err := db.BeginTxx(ctx, nil)
	assert.NoError(t, err)
	defer tx.Rollback()

	tx.MustExec(`INSERT INTO orgs_topupcredits(is_squashed, used, topup_id)
	                                    VALUES(TRUE, 1000000, 3),(TRUE, 998000, 2),(TRUE, 998, 2)`)

	tcs := []struct {
		OrgID     OrgID
		TopupID   TopupID
		Remaining int
	}{
		{OrgID(1), TopupID(null.NewInt(1, true)), 1000000},
		{OrgID(2), TopupID(null.NewInt(2, true)), 2},
		{OrgID(3), NilTopupID, 0},
	}

	for _, tc := range tcs {
		topup, err := calculateActiveTopup(ctx, tx, tc.OrgID)
		assert.NoError(t, err)

		if tc.TopupID == NilTopupID {
			assert.Nil(t, topup)
		} else {
			assert.NotNil(t, topup)
			assert.Equal(t, tc.TopupID, topup.ID)
			assert.Equal(t, tc.Remaining, topup.Remaining)
		}
	}

	tc2s := []struct {
		OrgID   OrgID
		TopupID TopupID
	}{
		{OrgID(1), TopupID(null.NewInt(1, true))},
		{OrgID(2), TopupID(null.NewInt(2, true))},
		{OrgID(2), TopupID(null.NewInt(2, true))},
		{OrgID(2), NilTopupID},
		{OrgID(3), NilTopupID},
	}

	for _, tc := range tc2s {
		topup, err := DecrementOrgCredits(ctx, tx, rc, tc.OrgID, 1)
		assert.NoError(t, err)
		assert.Equal(t, tc.TopupID, topup)
		tx.MustExec(`INSERT INTO orgs_topupcredits(is_squashed, used, topup_id) VALUES(TRUE, 1, $1)`, tc.OrgID)
	}
}

func TestLocations(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	db.MustExec(`INSERT INTO locations_boundaryalias(is_active, created_on, modified_on, name, boundary_id, created_by_id, modified_by_id, org_id)
											  VALUES(TRUE, NOW(), NOW(), 'Soko', 2, 1, 1, 1);`)
	db.MustExec(`INSERT INTO locations_boundaryalias(is_active, created_on, modified_on, name, boundary_id, created_by_id, modified_by_id, org_id)
	                                          VALUES(TRUE, NOW(), NOW(), 'Sokoz', 2, 1, 1, 2);`)

	root, err := loadLocations(ctx, db, 1)
	assert.NoError(t, err)

	locations := root[0].FindByName("Nigeria", 0, nil)

	assert.Equal(t, 1, len(locations))
	assert.Equal(t, "Nigeria", locations[0].Name())
	assert.Equal(t, []string(nil), locations[0].Aliases())
	assert.Equal(t, 37, len(locations[0].Children()))
	nigeria := locations[0]

	tcs := []struct {
		Name        string
		Level       utils.LocationLevel
		Aliases     []string
		NumChildren int
	}{
		{"Sokoto", 1, []string{"Soko"}, 23},
		{"Zamfara", 1, nil, 14},
	}

	for _, tc := range tcs {
		locations = root[0].FindByName(tc.Name, tc.Level, nigeria)
		assert.Equal(t, 1, len(locations))
		state := locations[0]

		assert.Equal(t, tc.Name, state.Name())
		assert.Equal(t, tc.Aliases, state.Aliases())
		assert.Equal(t, tc.NumChildren, len(state.Children()))
	}
}

func TestLabels(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	labels, err := loadLabels(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID   LabelID
		Name string
	}{
		{LabelID(9), "Building"},
		{LabelID(8), "Driving"},
	}

	assert.Equal(t, 10, len(labels))
	for i, tc := range tcs {
		label := labels[i].(*Label)
		assert.Equal(t, tc.ID, label.ID())
		assert.Equal(t, tc.Name, label.Name())
	}
}

func TestGroups(t *testing.T) {
	ctx := testsuite.CTX()
	db := testsuite.DB()

	groups, err := loadGroups(ctx, db, 1)
	assert.NoError(t, err)

	tcs := []struct {
		ID    GroupID
		UUID  assets.GroupUUID
		Name  string
		Query string
	}{
		{GroupID(40), assets.GroupUUID("5fc427e8-c307-49d7-91f7-8baf0db8a55e"), "Districts (Dynamic)", `district = "Faskari" OR district = "Zuru" OR district = "Anka"`},
		{GroupID(33), assets.GroupUUID("85a5a793-4741-4896-b55e-05af65f3c0fa"), "Doctors", ""},
	}

	assert.Equal(t, 10, len(groups))
	for i, tc := range tcs {
		group := groups[i].(*Group)
		assert.Equal(t, tc.UUID, group.UUID())
		assert.Equal(t, tc.ID, group.ID())
		assert.Equal(t, tc.Name, group.Name())
		assert.Equal(t, tc.Query, group.Query())
	}
}
