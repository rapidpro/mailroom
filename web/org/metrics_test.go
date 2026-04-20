package org_test

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
	"github.com/stretchr/testify/assert"
)

func TestMetrics(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	promToken := "2d26a50841ff48237238bbdd021150f6a33a4196"
	rt.DB.MustExec(`INSERT INTO api_apitoken(is_active, org_id, created, key, role_id, user_id) VALUES(TRUE, $1, NOW(), $2, $3, 1);`, testdata.Org1.ID, promToken, testdata.AuthGroupIDs["Prometheus"])

	adminToken := "5c26a50841ff48237238bbdd021150f6a33a4199"
	rt.DB.MustExec(`INSERT INTO api_apitoken(is_active, org_id, created, key, role_id, user_id) VALUES(TRUE, $1, NOW(), $2, $3, 1);`, testdata.Org1.ID, adminToken, testdata.AuthGroupIDs["Administrators"])

	wg := &sync.WaitGroup{}
	server := web.NewServer(ctx, rt, wg)
	server.Start()

	// wait for the server to start
	time.Sleep(time.Second)
	defer server.Stop()

	tcs := []struct {
		Label    string
		URL      string
		Username string
		Password string
		Response string
		Contains []string
	}{
		{
			Label:    "no username",
			URL:      fmt.Sprintf("http://localhost:8090/mr/org/%s/metrics", testdata.Org1.UUID),
			Username: "",
			Password: "",
			Response: `{"error": "invalid authentication"}`,
		},
		{
			Label:    "invalid password",
			URL:      fmt.Sprintf("http://localhost:8090/mr/org/%s/metrics", testdata.Org1.UUID),
			Username: "metrics",
			Password: "invalid",
			Response: `{"error": "invalid authentication"}`,
		},
		{
			Label:    "invalid username",
			URL:      fmt.Sprintf("http://localhost:8090/mr/org/%s/metrics", testdata.Org1.UUID),
			Username: "invalid",
			Password: promToken,
			Response: `{"error": "invalid authentication"}`,
		},
		{
			Label:    "valid login, wrong org",
			URL:      fmt.Sprintf("http://localhost:8090/mr/org/%s/metrics", testdata.Org2.UUID),
			Username: "metrics",
			Password: promToken,
			Response: `{"error": "invalid authentication"}`,
		},
		{
			Label:    "valid login, invalid user",
			URL:      fmt.Sprintf("http://localhost:8090/mr/org/%s/metrics", testdata.Org1.UUID),
			Username: "metrics",
			Password: adminToken,
			Response: `{"error": "invalid authentication"}`,
		},
		{
			Label:    "valid",
			URL:      fmt.Sprintf("http://localhost:8090/mr/org/%s/metrics", testdata.Org1.UUID),
			Username: "metrics",
			Password: promToken,
			Contains: []string{
				`rapidpro_group_contact_count{group_name="Active",group_uuid="b97f69f7-5edf-45c7-9fda-d37066eae91d",group_type="system",org="UNICEF"} 124`,
				`rapidpro_group_contact_count{group_name="Doctors",group_uuid="c153e265-f7c9-4539-9dbc-9b358714b638",group_type="user",org="UNICEF"} 121`,
				`rapidpro_channel_msg_count{channel_name="Vonage",channel_uuid="19012bfd-3ce3-4cae-9bb9-76cf92c73d49",channel_type="NX",msg_direction="out",msg_type="message",org="UNICEF"} 0`,
			},
		},
	}

	for _, tc := range tcs {
		req, _ := http.NewRequest(http.MethodGet, tc.URL, nil)
		req.SetBasicAuth(tc.Username, tc.Password)
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "%s: received error", tc.Label)

		body, _ := io.ReadAll(resp.Body)

		if tc.Response != "" {
			assert.Equal(t, tc.Response, string(body), "%s: response mismatch", tc.Label)
		}
		for _, contains := range tc.Contains {
			assert.Contains(t, string(body), contains, "%s: does not contain: %s", tc.Label, contains)
		}
	}
}
