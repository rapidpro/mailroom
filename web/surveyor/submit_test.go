package surveyor

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/buger/jsonparser"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
	"github.com/stretchr/testify/assert"
)

func TestSurveyor(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	wg := &sync.WaitGroup{}
	server := web.NewServer(ctx, rt, wg)
	server.Start()
	defer server.Stop()

	// insert an auth token for user 1 for org 1
	rt.DB.MustExec(`INSERT INTO api_apitoken(is_active, key, created, org_id, role_id, user_id) VALUES(TRUE, 'sesame', NOW(), $1, $2, 1)`, testdata.Org1.ID, testdata.AuthGroupIDs["Surveyors"])

	type Assertion struct {
		Query string
		Count int
	}

	tcs := []struct {
		file             string
		token            string
		expectedStatus   int
		expectedContains string
		assertions       []Assertion
	}{
		{
			file:             "valid_submission1.json",
			token:            "",
			expectedStatus:   401,
			expectedContains: "missing authorization",
		},
		{
			file:             "valid_submission1.json",
			token:            "invalid",
			expectedStatus:   401,
			expectedContains: "invalid authorization",
			assertions: []Assertion{
				{`SELECT count(*) FROM flows_flowrun WHERE flow_id = :flow_id`, 0},
			},
		},
		// new contact is created (our test db already has a bob, he should be unaffected)
		{
			file:             "valid_submission1.json",
			token:            "sesame",
			expectedStatus:   201,
			expectedContains: `"status": "C"`,
			assertions: []Assertion{
				{`SELECT count(*) FROM flows_flowrun WHERE flow_id = :flow_id AND contact_id = :contact_id AND status = 'C'`, 1},
				{`SELECT count(*) FROM contacts_contact WHERE name = 'Bob' AND org_id = 1`, 2},
				{`SELECT count(*) FROM contacts_contact WHERE uuid = 'bdfe862c-84f8-422e-8fdc-ebfaaae0697a'`, 0},
				{`SELECT count(*) FROM contacts_contact WHERE name = 'Bob' AND fields -> :age_field_uuid = jsonb_build_object('text', '37', 'number', 37)`, 1},
				{`SELECT count(*) FROM contacts_contacturn WHERE identity = 'tel::+593979123456' AND contact_id = :contact_id`, 1},
				{`SELECT count(*) FROM contacts_contactgroup_contacts WHERE contact_id = :contact_id and contactgroup_id = :testers_group_id`, 1},
				{`SELECT count(*) FROM msgs_msg WHERE contact_id = :contact_id AND contact_urn_id IS NULL AND direction = 'O' AND org_id = :org_id`, 4},
				{`SELECT count(*) FROM msgs_msg WHERE contact_id = :contact_id AND contact_urn_id IS NULL AND direction = 'I' AND org_id = :org_id`, 3},
			},
		},
		// dupe submission should fail due to run UUIDs being duplicated
		{
			file:             "valid_submission1.json",
			token:            "sesame",
			expectedStatus:   500,
			expectedContains: `error writing runs`,
			assertions: []Assertion{
				{`SELECT count(*) FROM flows_flowrun WHERE flow_id = :flow_id`, 1},
			},
		},
		// but submission with new UUIDs should succeed, new run is created but not contact
		{
			file:             "valid_submission2.json",
			token:            "sesame",
			expectedStatus:   201,
			expectedContains: `"status": "C"`,
			assertions: []Assertion{
				{`SELECT count(*) FROM flows_flowrun WHERE flow_id = :flow_id AND contact_id = :contact_id`, 2},
				{`SELECT count(*) FROM contacts_contact WHERE uuid = 'bdfe862c-84f8-422e-8fdc-ebfaaae0697a'`, 0},
				{`SELECT count(*) FROM contacts_contact WHERE name = 'Bob' AND fields -> :age_field_uuid = jsonb_build_object('text', '37', 'number', 37)`, 1},
				{`SELECT count(*) FROM contacts_contacturn WHERE identity = 'tel::+593979123456' AND contact_id = :contact_id`, 1},
				{`SELECT count(*) FROM contacts_contactgroup_contacts WHERE contact_id = :contact_id and contactgroup_id = :testers_group_id`, 1},
				{`SELECT count(*) FROM msgs_msg WHERE contact_id = :contact_id AND contact_urn_id IS NULL AND direction = 'O' AND org_id = :org_id`, 8},
				{`SELECT count(*) FROM msgs_msg WHERE contact_id = :contact_id AND contact_urn_id IS NULL AND direction = 'I' AND org_id = :org_id`, 6},
			}},
		// group removal is ONLY in the modifier
		{
			file:             "remove_group.json",
			token:            "sesame",
			expectedStatus:   201,
			expectedContains: `"status": "C"`,
			assertions: []Assertion{
				{`SELECT count(*) FROM flows_flowrun WHERE flow_id = :flow_id AND contact_id = :contact_id`, 3},
				{`SELECT count(*) FROM contacts_contact WHERE uuid = 'bdfe862c-84f8-422e-8fdc-ebfaaae0697a'`, 0},
				{`SELECT count(*) FROM contacts_contact WHERE name = 'Bob' AND fields -> :age_field_uuid = jsonb_build_object('text', '37', 'number', 37)`, 1},
				{`SELECT count(*) FROM contacts_contacturn WHERE identity = 'tel::+593979123456' AND contact_id = :contact_id`, 1},
				{`SELECT count(*) FROM contacts_contactgroup_contacts WHERE contact_id = :contact_id and contactgroup_id = :testers_group_id`, 0},
			},
		},
		// new contact, new session, group and field no longer exist
		{
			file:             "missing_group_field.json",
			token:            "sesame",
			expectedStatus:   201,
			expectedContains: `"status": "C"`,
			assertions: []Assertion{
				{`SELECT count(*) FROM flows_flowrun WHERE flow_id = :flow_id AND contact_id = :contact_id`, 1},
				{`SELECT count(*) FROM contacts_contact WHERE uuid = 'c7fa24ca-48f9-45bf-b923-f95aa49c3cd2'`, 0},
				{`SELECT count(*) FROM contacts_contact WHERE name = 'Fred' AND fields = jsonb_build_object()`, 1},
				{`SELECT count(*) FROM contacts_contacturn WHERE identity = 'tel::+593979123488' AND contact_id = :contact_id`, 1},
				{`SELECT count(*) FROM contacts_contactgroup_contacts WHERE contact_id = :contact_id and contactgroup_id = :testers_group_id`, 0},
			},
		},
		// submission that is too old should fail
		{
			file:             "too_old.json",
			token:            "sesame",
			expectedStatus:   500,
			expectedContains: `"error": "session too old to be submitted"`,
			assertions: []Assertion{
				{`SELECT count(*) FROM flows_flowrun WHERE flow_id = :flow_id AND contact_id = :contact_id`, 0},
			},
		},
	}

	type AssertionArgs struct {
		FlowID         models.FlowID    `db:"flow_id"`
		ContactID      flows.ContactID  `db:"contact_id"`
		OrgID          models.OrgID     `db:"org_id"`
		AgeFieldUUID   assets.FieldUUID `db:"age_field_uuid"`
		TestersGroupID models.GroupID   `db:"testers_group_id"`
	}

	args := &AssertionArgs{
		FlowID:         testdata.SurveyorFlow.ID,
		OrgID:          testdata.Org1.ID,
		AgeFieldUUID:   testdata.AgeField.UUID,
		TestersGroupID: testdata.TestersGroup.ID,
	}

	dates.SetNowSource(dates.NewSequentialNowSource(time.Date(2018, 12, 21, 12, 0, 0, 0, time.UTC)))
	defer dates.SetNowSource(dates.DefaultNowSource)

	for i, tc := range tcs {
		testID := fmt.Sprintf("%s[token=%s]", tc.file, tc.token)
		path := filepath.Join("testdata", tc.file)
		submission := testsuite.ReadFile(path)

		url := "http://localhost:8090/mr/surveyor/submit"
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(submission))
		assert.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		if tc.token != "" {
			req.Header.Set("Authorization", "Token "+tc.token)
		}

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err)
		assert.Equal(t, tc.expectedStatus, resp.StatusCode, "unexpected status code for %s", testID)

		body, _ := io.ReadAll(resp.Body)
		assert.Containsf(t, string(body), tc.expectedContains, "%s does not contain expected body", testID)

		id, _ := jsonparser.GetInt(body, "contact", "id")
		args.ContactID = flows.ContactID(id)

		// if we have assertions, check them
		for ii, assertion := range tc.assertions {
			rows, err := rt.DB.NamedQuery(assertion.Query, args)
			assert.NoError(t, err, "%d:%d error with named query", i, ii)

			count := 0
			assert.True(t, rows.Next())
			err = rows.Scan(&count)
			assert.NoError(t, err)

			assert.Equal(t, assertion.Count, count, "%d:%d mismatched counts", i, ii)
		}
	}
}
