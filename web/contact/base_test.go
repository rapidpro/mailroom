package contact_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
	"github.com/nyaruka/mailroom/web/contact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreate(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	// detach Cathy's tel URN
	rt.DB.MustExec(`UPDATE contacts_contacturn SET contact_id = NULL WHERE contact_id = $1`, testdata.Cathy.ID)

	rt.DB.MustExec(`ALTER SEQUENCE contacts_contact_id_seq RESTART WITH 30000`)

	testsuite.RunWebTests(t, ctx, rt, "testdata/create.json", nil)
}

func TestBulkCreate(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	rt.DB.MustExec(`ALTER SEQUENCE contacts_contact_id_seq RESTART WITH 30000`)

	testsuite.RunWebTests(t, ctx, rt, "testdata/bulk_create.json", nil)
}

func TestInspect(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	// give cathy an unsendable twitterid URN with a display value
	testdata.InsertContactURN(rt, testdata.Org1, testdata.Cathy, urns.URN("twitterid:23145325#cathy"), 20000, nil)

	testsuite.RunWebTests(t, ctx, rt, "testdata/inspect.json", nil)
}

func TestModify(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	// to be deterministic, update the creation date on cathy
	rt.DB.MustExec(`UPDATE contacts_contact SET created_on = $1 WHERE id = $2`, time.Date(2018, 7, 6, 12, 30, 0, 123456789, time.UTC), testdata.Cathy.ID)

	// make our campaign group dynamic
	rt.DB.MustExec(`UPDATE contacts_contactgroup SET query = 'age > 18' WHERE id = $1`, testdata.DoctorsGroup.ID)

	// insert an event on our campaign that is based on created on
	testdata.InsertCampaignFlowEvent(rt, testdata.RemindersCampaign, testdata.Favorites, testdata.CreatedOnField, 1000, "W")

	// for simpler tests we clear out cathy's fields and groups to start
	rt.DB.MustExec(`UPDATE contacts_contact SET fields = NULL WHERE id = $1`, testdata.Cathy.ID)
	rt.DB.MustExec(`DELETE FROM contacts_contactgroup_contacts WHERE contact_id = $1`, testdata.Cathy.ID)
	rt.DB.MustExec(`UPDATE contacts_contacturn SET contact_id = NULL WHERE contact_id = $1`, testdata.Cathy.ID)

	// because we made changes to a group above, need to make sure we don't use stale org assets
	models.FlushCache()

	// lock a contact to test skipping them
	models.LockContacts(ctx, rt, testdata.Org1.ID, []models.ContactID{testdata.Alexandria.ID}, time.Second)

	testsuite.RunWebTests(t, ctx, rt, "testdata/modify.json", nil)
}

func TestResolve(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	// detach Cathy's tel URN
	rt.DB.MustExec(`UPDATE contacts_contacturn SET contact_id = NULL WHERE contact_id = $1`, testdata.Cathy.ID)

	rt.DB.MustExec(`ALTER SEQUENCE contacts_contact_id_seq RESTART WITH 30000`)

	testsuite.RunWebTests(t, ctx, rt, "testdata/resolve.json", nil)
}

func TestInterrupt(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetData)

	// give Cathy an completed and a waiting session
	testdata.InsertFlowSession(rt, testdata.Org1, testdata.Cathy, models.FlowTypeMessaging, models.SessionStatusCompleted, testdata.Favorites, models.NilCallID)
	testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Cathy, models.FlowTypeMessaging, testdata.Favorites, models.NilCallID, time.Now(), time.Now().Add(time.Hour), true, nil)

	// give Bob a waiting session
	testdata.InsertWaitingSession(rt, testdata.Org1, testdata.Bob, models.FlowTypeMessaging, testdata.PickANumber, models.NilCallID, time.Now(), time.Now().Add(time.Hour), true, nil)

	testsuite.RunWebTests(t, ctx, rt, "testdata/interrupt.json", nil)
}

func TestSearch(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetElastic)

	wg := &sync.WaitGroup{}

	server := web.NewServer(ctx, rt, wg)
	server.Start()

	// give our server time to start
	time.Sleep(time.Second)

	defer server.Stop()

	tcs := []struct {
		method               string
		url                  string
		body                 string
		expectedStatus       int
		expectedError        string
		expectedHits         []models.ContactID
		expectedQuery        string
		expectedAttributes   []string
		expectedFields       []*assets.FieldReference
		expectedSchemes      []string
		expectedAllowAsGroup bool
	}{
		{ // 0
			method:         "GET",
			url:            "/mr/contact/search",
			expectedStatus: 405,
			expectedError:  "illegal method: GET",
		},
		{ // 1
			method:         "POST",
			url:            "/mr/contact/search",
			body:           fmt.Sprintf(`{"org_id": 1, "query": "birthday = tomorrow", "group_id": %d}`, testdata.ActiveGroup.ID),
			expectedStatus: 400,
			expectedError:  "can't resolve 'birthday' to attribute, scheme or field",
		},
		{ // 2
			method:         "POST",
			url:            "/mr/contact/search",
			body:           fmt.Sprintf(`{"org_id": 1, "query": "age > tomorrow", "group_id": %d}`, testdata.ActiveGroup.ID),
			expectedStatus: 400,
			expectedError:  "can't convert 'tomorrow' to a number",
		},
		{ // 3
			method:               "POST",
			url:                  "/mr/contact/search",
			body:                 fmt.Sprintf(`{"org_id": 1, "query": "Cathy", "group_id": %d}`, testdata.ActiveGroup.ID),
			expectedStatus:       200,
			expectedHits:         []models.ContactID{testdata.Cathy.ID},
			expectedQuery:        `name ~ "Cathy"`,
			expectedAttributes:   []string{"name"},
			expectedFields:       []*assets.FieldReference{},
			expectedSchemes:      []string{},
			expectedAllowAsGroup: true,
		},
		{ // 4
			method:               "POST",
			url:                  "/mr/contact/search",
			body:                 fmt.Sprintf(`{"org_id": 1, "query": "Cathy OR George", "group_id": %d, "exclude_ids": [%d, %d]}`, testdata.ActiveGroup.ID, testdata.Bob.ID, testdata.George.ID),
			expectedStatus:       200,
			expectedHits:         []models.ContactID{testdata.Cathy.ID},
			expectedQuery:        `name ~ "Cathy" OR name ~ "George"`,
			expectedAttributes:   []string{"name"},
			expectedFields:       []*assets.FieldReference{},
			expectedSchemes:      []string{},
			expectedAllowAsGroup: true,
		},
		{ // 5
			method:             "POST",
			url:                "/mr/contact/search",
			body:               fmt.Sprintf(`{"org_id": 1, "query": "AGE = 10 and gender = M", "group_id": %d}`, testdata.ActiveGroup.ID),
			expectedStatus:     200,
			expectedHits:       []models.ContactID{},
			expectedQuery:      `age = 10 AND gender = "M"`,
			expectedAttributes: []string{},
			expectedFields: []*assets.FieldReference{
				assets.NewFieldReference("age", "Age"),
				assets.NewFieldReference("gender", "Gender"),
			},
			expectedSchemes:      []string{},
			expectedAllowAsGroup: true,
		},
		{ // 6
			method:               "POST",
			url:                  "/mr/contact/search",
			body:                 fmt.Sprintf(`{"org_id": 1, "query": "", "group_id": %d}`, testdata.TestersGroup.ID),
			expectedStatus:       200,
			expectedHits:         []models.ContactID{10013, 10012, 10011, 10010, 10009, 10008, 10007, 10006, 10005, 10004},
			expectedQuery:        ``,
			expectedAttributes:   []string{},
			expectedFields:       []*assets.FieldReference{},
			expectedSchemes:      []string{},
			expectedAllowAsGroup: true,
		},
	}

	for i, tc := range tcs {
		var body io.Reader
		if tc.body != "" {
			body = bytes.NewReader([]byte(tc.body))
		}

		req, err := http.NewRequest(tc.method, "http://localhost:8090"+tc.url, body)
		assert.NoError(t, err, "%d: error creating request", i)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "%d: error making request", i)

		assert.Equal(t, tc.expectedStatus, resp.StatusCode, "%d: unexpected status", i)

		content, err := io.ReadAll(resp.Body)
		assert.NoError(t, err, "%d: error reading body", i)

		// on 200 responses parse them
		if resp.StatusCode == 200 {
			r := &contact.SearchResponse{}
			err = json.Unmarshal(content, r)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedHits, r.ContactIDs, "%d: hits mismatch", i)
			assert.Equal(t, tc.expectedQuery, r.Query, "%d: query mismatch", i)

			if len(tc.expectedAttributes) > 0 || len(tc.expectedFields) > 0 || len(tc.expectedSchemes) > 0 {
				assert.Equal(t, tc.expectedAttributes, r.Metadata.Attributes)
				assert.Equal(t, tc.expectedFields, r.Metadata.Fields)
				assert.Equal(t, tc.expectedSchemes, r.Metadata.Schemes)
				assert.Equal(t, tc.expectedAllowAsGroup, r.Metadata.AllowAsGroup)
			}
		} else {
			r := &web.ErrorResponse{}
			err = json.Unmarshal(content, r)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedError, r.Error)
		}
	}
}

func TestParseQuery(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	testsuite.RunWebTests(t, ctx, rt, "testdata/parse_query.json", nil)
}

func TestSpecToCreation(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	oa, err := models.GetOrgAssets(ctx, rt, testdata.Org1.ID)
	require.NoError(t, err)

	sa := oa.SessionAssets()
	env := envs.NewBuilder().Build()

	// empty spec is valid
	s := &models.ContactSpec{}
	c, err := contact.SpecToCreation(s, env, sa)
	assert.NoError(t, err)
	assert.Equal(t, "", c.Name)
	assert.Equal(t, i18n.NilLanguage, c.Language)
	assert.Equal(t, 0, len(c.URNs))
	assert.Equal(t, 0, len(c.Mods))

	// try to set invalid language
	lang := "xyzd"
	s = &models.ContactSpec{Language: &lang}
	_, err = contact.SpecToCreation(s, env, sa)
	assert.EqualError(t, err, "invalid language: iso-639-3 codes must be 3 characters, got: xyzd")

	// try to set non-existent contact field
	s = &models.ContactSpec{Fields: map[string]string{"goats": "7"}}
	_, err = contact.SpecToCreation(s, env, sa)
	assert.EqualError(t, err, "unknown contact field 'goats'")

	// try to add to non-existent group
	s = &models.ContactSpec{Groups: []assets.GroupUUID{"52f6c50e-f9a8-4f24-bb80-5c9f144ed27f"}}
	_, err = contact.SpecToCreation(s, env, sa)
	assert.EqualError(t, err, "unknown contact group '52f6c50e-f9a8-4f24-bb80-5c9f144ed27f'")

	// try to add to dynamic group
	s = &models.ContactSpec{Groups: []assets.GroupUUID{"52f6c50e-f9a8-4f24-bb80-5c9f144ed27f"}}
	_, err = contact.SpecToCreation(s, env, sa)
	assert.EqualError(t, err, "unknown contact group '52f6c50e-f9a8-4f24-bb80-5c9f144ed27f'")
}
