package surveyor

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"sync"
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web"

	"github.com/buger/jsonparser"
	_ "github.com/nyaruka/mailroom/hooks"
	"github.com/stretchr/testify/assert"
)

func TestSurveyor(t *testing.T) {
	ctx, db, rp := testsuite.Reset()
	rc := rp.Get()
	defer rc.Close()

	wg := &sync.WaitGroup{}
	server := web.NewServer(ctx, config.Mailroom, db, rp, nil, nil, wg)
	server.Start()
	defer server.Stop()

	// insert an auth token for user 1 for org 1
	db.MustExec(`INSERT INTO api_apitoken(is_active, key, created, org_id, role_id, user_id) VALUES(TRUE, 'sesame', NOW(), 1, 5, 1)`)

	type Assertion struct {
		Query string
		Count int
	}

	tcs := []struct {
		File       string
		Token      string
		StatusCode int
		Contains   string
		Assertions []Assertion
	}{
		{"contact_surveyor_submission.json", "", 401, "missing authorization", nil},
		{"contact_surveyor_submission.json", "invalid", 401, "invalid authorization", []Assertion{
			Assertion{`SELECT count(*) FROM flows_flowrun WHERE flow_id = :flow_id`, 0},
		}},
		// new contact is created (our test db already has a bob, he should be unaffected)
		{"contact_surveyor_submission.json", "sesame", 201, `"status": "C"`, []Assertion{
			Assertion{`SELECT count(*) FROM flows_flowrun WHERE flow_id = :flow_id AND contact_id = :contact_id AND is_active = FALSE`, 1},
			Assertion{`SELECT count(*) FROM contacts_contact WHERE name = 'Bob' AND org_id = 1`, 2},
			Assertion{`SELECT count(*) FROM contacts_contact WHERE uuid = 'bdfe862c-84f8-422e-8fdc-ebfaaae0697a'`, 0},
			Assertion{`SELECT count(*) FROM contacts_contact WHERE name = 'Bob' AND fields -> :age_field_uuid = jsonb_build_object('text', '37', 'number', 37)`, 1},
			Assertion{`SELECT count(*) FROM contacts_contacturn WHERE identity = 'tel::+593979123456' AND contact_id = :contact_id`, 1},
			Assertion{`SELECT count(*) FROM contacts_contactgroup_contacts WHERE contact_id = :contact_id and contactgroup_id = :testers_group_id`, 1},
			Assertion{`SELECT count(*) FROM msgs_msg WHERE contact_id = :contact_id AND contact_urn_id IS NULL AND direction = 'O' AND org_id = :org_id`, 4},
			Assertion{`SELECT count(*) FROM msgs_msg WHERE contact_id = :contact_id AND contact_urn_id IS NULL AND direction = 'I' AND org_id = :org_id`, 3},
		}},
		// dupe submission should fail due to run UUIDs being duplicated
		{"contact_surveyor_submission.json", "sesame", 500, `error writing runs`, []Assertion{
			Assertion{`SELECT count(*) FROM flows_flowrun WHERE flow_id = :flow_id`, 1},
		}},
		// but submission with new UUIDs should succeed, new run is created but not contact
		{"contact_surveyor_submission2.json", "sesame", 201, `"status": "C"`, []Assertion{
			Assertion{`SELECT count(*) FROM flows_flowrun WHERE flow_id = :flow_id AND contact_id = :contact_id`, 2},
			Assertion{`SELECT count(*) FROM contacts_contact WHERE uuid = 'bdfe862c-84f8-422e-8fdc-ebfaaae0697a'`, 0},
			Assertion{`SELECT count(*) FROM contacts_contact WHERE name = 'Bob' AND fields -> :age_field_uuid = jsonb_build_object('text', '37', 'number', 37)`, 1},
			Assertion{`SELECT count(*) FROM contacts_contacturn WHERE identity = 'tel::+593979123456' AND contact_id = :contact_id`, 1},
			Assertion{`SELECT count(*) FROM contacts_contactgroup_contacts WHERE contact_id = :contact_id and contactgroup_id = :testers_group_id`, 1},
			Assertion{`SELECT count(*) FROM msgs_msg WHERE contact_id = :contact_id AND contact_urn_id IS NULL AND direction = 'O' AND org_id = :org_id`, 8},
			Assertion{`SELECT count(*) FROM msgs_msg WHERE contact_id = :contact_id AND contact_urn_id IS NULL AND direction = 'I' AND org_id = :org_id`, 6},
		}},
		// group removal is ONLY in the modifier
		{"remove_group.json", "sesame", 201, `"status": "C"`, []Assertion{
			Assertion{`SELECT count(*) FROM flows_flowrun WHERE flow_id = :flow_id AND contact_id = :contact_id`, 3},
			Assertion{`SELECT count(*) FROM contacts_contact WHERE uuid = 'bdfe862c-84f8-422e-8fdc-ebfaaae0697a'`, 0},
			Assertion{`SELECT count(*) FROM contacts_contact WHERE name = 'Bob' AND fields -> :age_field_uuid = jsonb_build_object('text', '37', 'number', 37)`, 1},
			Assertion{`SELECT count(*) FROM contacts_contacturn WHERE identity = 'tel::+593979123456' AND contact_id = :contact_id`, 1},
			Assertion{`SELECT count(*) FROM contacts_contactgroup_contacts WHERE contact_id = :contact_id and contactgroup_id = :testers_group_id`, 0},
		}},
		// new contact, new session, group and field no longer exist
		{"missing_group_field.json", "sesame", 201, `"status": "C"`, []Assertion{
			Assertion{`SELECT count(*) FROM flows_flowrun WHERE flow_id = :flow_id AND contact_id = :contact_id`, 1},
			Assertion{`SELECT count(*) FROM contacts_contact WHERE uuid = 'c7fa24ca-48f9-45bf-b923-f95aa49c3cd2'`, 0},
			Assertion{`SELECT count(*) FROM contacts_contact WHERE name = 'Fred' AND fields = jsonb_build_object()`, 1},
			Assertion{`SELECT count(*) FROM contacts_contacturn WHERE identity = 'tel::+593979123488' AND contact_id = :contact_id`, 1},
			Assertion{`SELECT count(*) FROM contacts_contactgroup_contacts WHERE contact_id = :contact_id and contactgroup_id = :testers_group_id`, 0},
		}},
	}

	type AssertionArgs struct {
		FlowID         models.FlowID    `db:"flow_id"`
		ContactID      flows.ContactID  `db:"contact_id"`
		OrgID          models.OrgID     `db:"org_id"`
		AgeFieldUUID   assets.FieldUUID `db:"age_field_uuid"`
		TestersGroupID models.GroupID   `db:"testers_group_id"`
	}

	args := &AssertionArgs{
		FlowID:         models.SurveyorFlowID,
		OrgID:          models.Org1,
		AgeFieldUUID:   models.AgeFieldUUID,
		TestersGroupID: models.TestersGroupID,
	}

	for i, tc := range tcs {
		testID := fmt.Sprintf("%s[token=%s]", tc.File, tc.Token)
		path := filepath.Join("testdata", tc.File)
		submission, err := ioutil.ReadFile(path)
		assert.NoError(t, err)

		url := fmt.Sprintf("http://localhost:8090/mr/surveyor/submit")
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(submission))
		assert.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		if tc.Token != "" {
			req.Header.Set("Authorization", "Token "+tc.Token)
		}

		resp, err := http.DefaultClient.Do(req)
		assert.Equal(t, tc.StatusCode, resp.StatusCode, "unexpected status code for %s", testID)

		body, _ := ioutil.ReadAll(resp.Body)
		assert.Containsf(t, string(body), tc.Contains, "%s does not contain expected body", testID)

		id, _ := jsonparser.GetInt(body, "contact", "id")
		args.ContactID = flows.ContactID(id)

		// if we have assertions, check them
		for ii, assertion := range tc.Assertions {
			rows, err := db.NamedQuery(assertion.Query, args)
			assert.NoError(t, err, "%d:%d error with named query", i, ii)

			count := 0
			assert.True(t, rows.Next())
			err = rows.Scan(&count)
			assert.NoError(t, err)

			assert.Equal(t, assertion.Count, count, "%d:%d mismatched counts", i, ii)
		}
	}
}
