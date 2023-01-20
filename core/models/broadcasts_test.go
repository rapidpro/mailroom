package models_test

import (
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/envs"
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

	translations := map[envs.Language]*models.BroadcastTranslation{envs.Language("eng"): {Text: "Hi there"}}

	// create a broadcast which doesn't actually exist in the DB
	bcast := models.NewBroadcast(
		testdata.Org1.ID,
		models.NilBroadcastID,
		translations,
		models.TemplateStateUnevaluated,
		envs.Language("eng"),
		[]urns.URN{"tel:+593979012345"},
		[]models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID, testdata.Cathy.ID},
		[]models.GroupID{testdata.DoctorsGroup.ID},
		ticket.ID,
		models.NilUserID,
	)

	assert.Equal(t, models.NilBroadcastID, bcast.ID())
	assert.Equal(t, testdata.Org1.ID, bcast.OrgID())
	assert.Equal(t, envs.Language("eng"), bcast.BaseLanguage())
	assert.Equal(t, translations, bcast.Translations())
	assert.Equal(t, models.TemplateStateUnevaluated, bcast.TemplateState())
	assert.Equal(t, ticket.ID, bcast.TicketID())
	assert.Equal(t, []urns.URN{"tel:+593979012345"}, bcast.URNs())
	assert.Equal(t, []models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID, testdata.Cathy.ID}, bcast.ContactIDs())
	assert.Equal(t, []models.GroupID{testdata.DoctorsGroup.ID}, bcast.GroupIDs())

	batch := bcast.CreateBatch([]models.ContactID{testdata.Alexandria.ID, testdata.Bob.ID})

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
		Translations models.BroadcastTranslations `json:"translations"`
	}

	s := &TestStruct{}
	err := db.Get(s, `SELECT translations FROM msgs_broadcast WHERE id = $1`, bcastID)
	require.NoError(t, err)

	assert.Equal(t, models.BroadcastTranslations{"eng": &models.BroadcastTranslation{Text: "Hello"}, "spa": &models.BroadcastTranslation{Text: "Hola"}}, s.Translations)

	s.Translations = models.BroadcastTranslations{"fra": &models.BroadcastTranslation{Text: "Bonjour"}}

	db.MustExec(`UPDATE msgs_broadcast SET translations = $1 WHERE id = $2`, s.Translations, bcastID)

	assertdb.Query(t, db, `SELECT count(*) FROM msgs_broadcast WHERE translations -> 'fra' ->> 'text' = 'Bonjour'`, 1)
}
