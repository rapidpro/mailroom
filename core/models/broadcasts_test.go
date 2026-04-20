package models_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNonPersistentBroadcasts(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	translations := flows.BroadcastTranslations{"eng": {Text: "Hi there"}}
	optIn := testdata.InsertOptIn(rt, testdata.Org1, "Polls")

	// create a broadcast which doesn't actually exist in the DB
	bcast := models.NewBroadcast(
		testdata.Org1.ID,
		translations,
		models.TemplateStateUnevaluated,
		"eng",
		optIn.ID,
		[]urns.URN{"tel:+593979012345"},
		[]models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID, testdata.Cathy.ID},
		[]models.GroupID{testdata.DoctorsGroup.ID},
		"",
		models.NilUserID,
	)

	assert.Equal(t, models.NilBroadcastID, bcast.ID)
	assert.Equal(t, testdata.Org1.ID, bcast.OrgID)
	assert.Equal(t, i18n.Language("eng"), bcast.BaseLanguage)
	assert.Equal(t, translations, bcast.Translations)
	assert.Equal(t, models.TemplateStateUnevaluated, bcast.TemplateState)
	assert.Equal(t, optIn.ID, bcast.OptInID)
	assert.Equal(t, []urns.URN{"tel:+593979012345"}, bcast.URNs)
	assert.Equal(t, []models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID, testdata.Cathy.ID}, bcast.ContactIDs)
	assert.Equal(t, []models.GroupID{testdata.DoctorsGroup.ID}, bcast.GroupIDs)

	batch := bcast.CreateBatch([]models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID}, false)

	assert.Equal(t, models.NilBroadcastID, batch.BroadcastID)
	assert.Equal(t, testdata.Org1.ID, batch.OrgID)
	assert.Equal(t, i18n.Language("eng"), batch.BaseLanguage)
	assert.Equal(t, translations, batch.Translations)
	assert.Equal(t, models.TemplateStateUnevaluated, batch.TemplateState)
	assert.Equal(t, optIn.ID, batch.OptInID)
	assert.Equal(t, []models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID}, batch.ContactIDs)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	msgs, err := batch.CreateMessages(ctx, rt, oa)
	require.NoError(t, err)

	assert.Equal(t, 2, len(msgs))

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_msg WHERE direction = 'O' AND broadcast_id IS NULL AND text = 'Hi there'`).Returns(2)
}

func TestBroadcastTranslations(t *testing.T) {
	_, rt := testsuite.Runtime()

	defer func() {
		rt.DB.MustExec(`DELETE FROM msgs_broadcast_contacts`)
		rt.DB.MustExec(`DELETE FROM msgs_broadcast`)
	}()

	bcastID := testdata.InsertBroadcast(rt, testdata.Org1, `eng`, map[i18n.Language]string{`eng`: "Hello", `spa`: "Hola"}, nil, models.NilScheduleID, []*testdata.Contact{testdata.Cathy}, nil)

	type TestStruct struct {
		Translations flows.BroadcastTranslations `json:"translations"`
	}

	s := &TestStruct{}
	err := rt.DB.Get(s, `SELECT translations FROM msgs_broadcast WHERE id = $1`, bcastID)
	require.NoError(t, err)

	assert.Equal(t, flows.BroadcastTranslations{"eng": {Text: "Hello"}, "spa": {Text: "Hola"}}, s.Translations)

	s.Translations = flows.BroadcastTranslations{"fra": {Text: "Bonjour"}}

	rt.DB.MustExec(`UPDATE msgs_broadcast SET translations = $1 WHERE id = $2`, s.Translations, bcastID)

	assertdb.Query(t, rt.DB, `SELECT count(*) FROM msgs_broadcast WHERE translations -> 'fra' ->> 'text' = 'Bonjour'`, 1)
}

func TestBroadcastBatchCreateMessage(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer func() {
		rt.DB.MustExec(`UPDATE contacts_contact SET language = NULL WHERE id = $1`, testdata.Cathy.ID)
		testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)
	}()

	polls := testdata.InsertOptIn(rt, testdata.Org1, "Polls")

	oa, err := models.GetOrgAssetsWithRefresh(ctx, rt, testdata.Org1.ID, models.RefreshOptIns)
	require.NoError(t, err)

	// we need a broadcast id to insert messages but the content here is ignored
	bcastID := testdata.InsertBroadcast(rt, testdata.Org1, "eng", map[i18n.Language]string{"eng": "Test"}, nil, models.NilScheduleID, nil, nil)

	tcs := []struct {
		contactLanguage      i18n.Language
		translations         flows.BroadcastTranslations
		baseLanguage         i18n.Language
		templateState        models.TemplateState
		optInID              models.OptInID
		expectedText         string
		expectedAttachments  []utils.Attachment
		expectedQuickReplies []string
		expectedLocale       i18n.Locale
		expectedError        string
	}{
		{ // 0
			contactLanguage:      i18n.NilLanguage,
			translations:         flows.BroadcastTranslations{"eng": {Text: "Hi @Cathy"}},
			baseLanguage:         "eng",
			templateState:        models.TemplateStateEvaluated,
			expectedText:         "Hi @Cathy",
			expectedAttachments:  []utils.Attachment{},
			expectedQuickReplies: nil,
			expectedLocale:       "eng",
		},
		{ // 1: contact language not set, uses base language
			contactLanguage:      i18n.NilLanguage,
			translations:         flows.BroadcastTranslations{"eng": {Text: "Hello @contact.name"}, "spa": {Text: "Hola @contact.name"}},
			baseLanguage:         "eng",
			templateState:        models.TemplateStateUnevaluated,
			expectedText:         "Hello Cathy",
			expectedAttachments:  []utils.Attachment{},
			expectedQuickReplies: nil,
			expectedLocale:       "eng",
		},
		{ // 2: contact language iggnored if it isn't a valid org language, even if translation exists
			contactLanguage:      "spa",
			translations:         flows.BroadcastTranslations{"eng": {Text: "Hello @contact.name"}, "spa": {Text: "Hola @contact.name"}},
			baseLanguage:         "eng",
			templateState:        models.TemplateStateUnevaluated,
			expectedText:         "Hello Cathy",
			expectedAttachments:  []utils.Attachment{},
			expectedQuickReplies: nil,
			expectedLocale:       "eng",
		},
		{ // 3: contact language used
			contactLanguage: "fra",
			translations: flows.BroadcastTranslations{
				"eng": {Text: "Hello @contact.name", Attachments: []utils.Attachment{"audio/mp3:http://test.en.mp3"}, QuickReplies: []string{"yes", "no"}},
				"fra": {Text: "Bonjour @contact.name", Attachments: []utils.Attachment{"audio/mp3:http://test.fr.mp3"}, QuickReplies: []string{"oui", "no"}},
			},
			baseLanguage:         "eng",
			templateState:        models.TemplateStateUnevaluated,
			expectedText:         "Bonjour Cathy",
			expectedAttachments:  []utils.Attachment{"audio/mp3:http://test.fr.mp3"},
			expectedQuickReplies: []string{"oui", "no"},
			expectedLocale:       "fra",
		},
		{ // 4: broken broadcast with no translation in base language
			contactLanguage: i18n.NilLanguage,
			translations:    flows.BroadcastTranslations{"fra": {Text: "Bonjour @contact.name"}},
			baseLanguage:    "eng",
			templateState:   models.TemplateStateUnevaluated,
			expectedError:   "error creating broadcast message: broadcast has no translation in base language",
		},
		{ // 5: broadcast with optin
			contactLanguage:      i18n.NilLanguage,
			translations:         flows.BroadcastTranslations{"eng": {Text: "Hi @Cathy"}},
			baseLanguage:         "eng",
			templateState:        models.TemplateStateEvaluated,
			optInID:              polls.ID,
			expectedText:         "Hi @Cathy",
			expectedAttachments:  []utils.Attachment{},
			expectedQuickReplies: nil,
			expectedLocale:       "eng",
		},
	}

	for i, tc := range tcs {
		batch := &models.BroadcastBatch{
			BroadcastID:   bcastID,
			OrgID:         testdata.Org1.ID,
			Translations:  tc.translations,
			BaseLanguage:  tc.baseLanguage,
			TemplateState: tc.templateState,
			OptInID:       tc.optInID,
			ContactIDs:    []models.ContactID{testdata.Cathy.ID},
		}

		rt.DB.MustExec(`UPDATE contacts_contact SET language = $2 WHERE id = $1`, testdata.Cathy.ID, tc.contactLanguage)

		msgs, err := batch.CreateMessages(ctx, rt, oa)
		if tc.expectedError != "" {
			assert.EqualError(t, err, tc.expectedError, "error mismatch in test case %d", i)
		} else {
			assert.NoError(t, err, "unexpected error in test case %d", i)
			if assert.Len(t, msgs, 1, "msg count mismatch in test case %d", i) {
				assert.Equal(t, tc.expectedText, msgs[0].Text(), "msg text mismatch in test case %d", i)
				assert.Equal(t, tc.expectedAttachments, msgs[0].Attachments(), "attachments mismatch in test case %d", i)
				assert.Equal(t, tc.expectedQuickReplies, msgs[0].QuickReplies(), "quick replies mismatch in test case %d", i)
				assert.Equal(t, tc.expectedLocale, msgs[0].Locale(), "msg locale mismatch in test case %d", i)
				assert.Equal(t, tc.optInID, msgs[0].OptInID(), "optin id mismatch in test case %d", i)
			}
		}
	}
}

func TestInsertChildBroadcast(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	optIn := testdata.InsertOptIn(rt, testdata.Org1, "Polls")
	schedID := testdata.InsertSchedule(rt, testdata.Org1, models.RepeatPeriodDaily, time.Now())
	bcastID := testdata.InsertBroadcast(rt, testdata.Org1, `eng`, map[i18n.Language]string{`eng`: "Hello"}, optIn, schedID, []*testdata.Contact{testdata.Bob, testdata.Cathy}, nil)

	var bj json.RawMessage
	err := rt.DB.GetContext(ctx, &bj, `SELECT ROW_TO_JSON(r) FROM (
		SELECT id, org_id, translations, base_language, optin_id, query, created_by_id, parent_id FROM msgs_broadcast WHERE id = $1
	) r`, bcastID)
	require.NoError(t, err)

	parent := &models.Broadcast{}
	jsonx.MustUnmarshal(bj, parent)

	child, err := models.InsertChildBroadcast(ctx, rt.DB, parent)
	assert.NoError(t, err)
	assert.Equal(t, parent.ID, child.ParentID)
	assert.Equal(t, parent.OrgID, child.OrgID)
	assert.Equal(t, parent.BaseLanguage, child.BaseLanguage)
	assert.Equal(t, parent.OptInID, child.OptInID)
}
