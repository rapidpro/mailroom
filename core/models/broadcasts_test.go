package models_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNonPersistentBroadcasts(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetData)

	ticket := testdata.InsertOpenTicket(db, testdata.Org1, testdata.Bob, testdata.Mailgun, testdata.DefaultTopic, "", "", time.Now(), nil)
	modelTicket := ticket.Load(db)

	translations := flows.BroadcastTranslations{"eng": {Text: "Hi there"}}

	// create a broadcast which doesn't actually exist in the DB
	bcast := models.NewBroadcast(
		testdata.Org1.ID,
		translations,
		models.TemplateStateUnevaluated,
		envs.Language("eng"),
		[]urns.URN{"tel:+593979012345"},
		[]models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID, testdata.Cathy.ID},
		[]models.GroupID{testdata.DoctorsGroup.ID},
		"",
		ticket.ID,
		models.NilUserID,
	)

	assert.Equal(t, models.NilBroadcastID, bcast.ID)
	assert.Equal(t, testdata.Org1.ID, bcast.OrgID)
	assert.Equal(t, envs.Language("eng"), bcast.BaseLanguage)
	assert.Equal(t, translations, bcast.Translations)
	assert.Equal(t, models.TemplateStateUnevaluated, bcast.TemplateState)
	assert.Equal(t, ticket.ID, bcast.TicketID)
	assert.Equal(t, []urns.URN{"tel:+593979012345"}, bcast.URNs)
	assert.Equal(t, []models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID, testdata.Cathy.ID}, bcast.ContactIDs)
	assert.Equal(t, []models.GroupID{testdata.DoctorsGroup.ID}, bcast.GroupIDs)

	batch := bcast.CreateBatch([]models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID}, false)

	assert.Equal(t, models.NilBroadcastID, batch.BroadcastID)
	assert.Equal(t, testdata.Org1.ID, batch.OrgID)
	assert.Equal(t, envs.Language("eng"), batch.BaseLanguage)
	assert.Equal(t, translations, batch.Translations)
	assert.Equal(t, models.TemplateStateUnevaluated, batch.TemplateState)
	assert.Equal(t, ticket.ID, batch.TicketID)
	assert.Equal(t, []models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID}, batch.ContactIDs)

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	msgs, err := batch.CreateMessages(ctx, rt, oa)
	require.NoError(t, err)

	assert.Equal(t, 2, len(msgs))

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_msg WHERE direction = 'O' AND broadcast_id IS NULL AND text = 'Hi there'`).Returns(2)

	// test ticket was updated
	assertdb.Query(t, db, `SELECT count(*) FROM tickets_ticket WHERE id = $1 AND last_activity_on > $2`, ticket.ID, modelTicket.LastActivityOn()).Returns(1)
}

func TestBroadcastTranslations(t *testing.T) {
	_, _, db, _ := testsuite.Get()

	defer func() {
		db.MustExec(`DELETE FROM msgs_broadcast_contacts`)
		db.MustExec(`DELETE FROM msgs_broadcast`)
	}()

	bcastID := testdata.InsertBroadcast(db, testdata.Org1, `eng`, map[envs.Language]string{`eng`: "Hello", `spa`: "Hola"}, models.NilScheduleID, []*testdata.Contact{testdata.Cathy}, nil)

	type TestStruct struct {
		Translations flows.BroadcastTranslations `json:"translations"`
	}

	s := &TestStruct{}
	err := db.Get(s, `SELECT translations FROM msgs_broadcast WHERE id = $1`, bcastID)
	require.NoError(t, err)

	assert.Equal(t, flows.BroadcastTranslations{"eng": {Text: "Hello"}, "spa": {Text: "Hola"}}, s.Translations)

	s.Translations = flows.BroadcastTranslations{"fra": {Text: "Bonjour"}}

	db.MustExec(`UPDATE msgs_broadcast SET translations = $1 WHERE id = $2`, s.Translations, bcastID)

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_broadcast WHERE translations -> 'fra' ->> 'text' = 'Bonjour'`, 1)
}

func TestBroadcastBatchCreateMessage(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer func() {
		db.MustExec(`UPDATE contacts_contact SET language = NULL WHERE id = $1`, testdata.Cathy.ID)
		testsuite.Reset(testsuite.ResetData | testsuite.ResetRedis)
	}()

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	// we need a broadcast id to insert messages but the content here is ignored
	bcastID := testdata.InsertBroadcast(db, testdata.Org1, "eng", map[envs.Language]string{"eng": "Test"}, models.NilScheduleID, nil, nil)

	tcs := []struct {
		contactLanguage      envs.Language
		translations         flows.BroadcastTranslations
		baseLanguage         envs.Language
		templateState        models.TemplateState
		expectedText         string
		expectedAttachments  []utils.Attachment
		expectedQuickReplies []string
		expectedLocale       envs.Locale
		expectedError        string
	}{
		{
			contactLanguage:      envs.NilLanguage,
			translations:         flows.BroadcastTranslations{"eng": {Text: "Hi @Cathy"}},
			baseLanguage:         "eng",
			templateState:        models.TemplateStateEvaluated,
			expectedText:         "Hi @Cathy",
			expectedAttachments:  []utils.Attachment{},
			expectedQuickReplies: nil,
			expectedLocale:       "eng",
		},
		{
			// contact language not set, uses base language
			contactLanguage:      envs.NilLanguage,
			translations:         flows.BroadcastTranslations{"eng": {Text: "Hello @contact.name"}, "spa": {Text: "Hola @contact.name"}},
			baseLanguage:         "eng",
			templateState:        models.TemplateStateUnevaluated,
			expectedText:         "Hello Cathy",
			expectedAttachments:  []utils.Attachment{},
			expectedQuickReplies: nil,
			expectedLocale:       "eng",
		},
		{
			// contact language iggnored if it isn't a valid org language, even if translation exists
			contactLanguage:      envs.Language("spa"),
			translations:         flows.BroadcastTranslations{"eng": {Text: "Hello @contact.name"}, "spa": {Text: "Hola @contact.name"}},
			baseLanguage:         "eng",
			templateState:        models.TemplateStateUnevaluated,
			expectedText:         "Hello Cathy",
			expectedAttachments:  []utils.Attachment{},
			expectedQuickReplies: nil,
			expectedLocale:       "eng",
		},
		{
			// contact language used
			contactLanguage: envs.Language("fra"),
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
		{
			// broken broadcast with no translation in base language
			contactLanguage: envs.NilLanguage,
			translations:    flows.BroadcastTranslations{"fra": {Text: "Bonjour @contact.name"}},
			baseLanguage:    "eng",
			templateState:   models.TemplateStateUnevaluated,
			expectedError:   "error creating broadcast message: broadcast has no translation in base language",
		},
	}

	for i, tc := range tcs {
		batch := &models.BroadcastBatch{
			BroadcastID:   bcastID,
			OrgID:         testdata.Org1.ID,
			Translations:  tc.translations,
			BaseLanguage:  tc.baseLanguage,
			TemplateState: tc.templateState,
			ContactIDs:    []models.ContactID{testdata.Cathy.ID},
		}

		db.MustExec(`UPDATE contacts_contact SET language = $2 WHERE id = $1`, testdata.Cathy.ID, tc.contactLanguage)

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
			}
		}
	}
}
