package org

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web"
	"github.com/stretchr/testify/assert"
)

func TestMetrics(t *testing.T) {
	ctx, db, rp := testsuite.Reset()

	promToken := "2d26a50841ff48237238bbdd021150f6a33a4196"
	db.MustExec(`INSERT INTO api_apitoken(is_active, org_id, created, key, role_id, user_id) VALUES(TRUE, $1, NOW(), $2, 11, 1);`, models.Org1, promToken)

	adminToken := "5c26a50841ff48237238bbdd021150f6a33a4199"
	db.MustExec(`INSERT INTO api_apitoken(is_active, org_id, created, key, role_id, user_id) VALUES(TRUE, $1, NOW(), $2, 8, 1);`, models.Org1, adminToken)

	wg := &sync.WaitGroup{}
	server := web.NewServer(ctx, config.Mailroom, db, rp, nil, nil, wg)
	server.Start()

	// wait for the server to start
	time.Sleep(time.Second)
	defer server.Stop()

	tcs := []struct {
		URL      string
		Username string
		Password string
		Response string
		Contains []string
	}{
		{
			URL:      fmt.Sprintf("http://localhost:8090/mr/org/%s/metrics", models.Org1UUID),
			Username: "",
			Password: "",
			Response: `{"error": "invalid authentication"}`,
		},
		{
			URL:      fmt.Sprintf("http://localhost:8090/mr/org/%s/metrics", models.Org1UUID),
			Username: "metrics",
			Password: "invalid",
			Response: `{"error": "invalid authentication"}`,
		},
		{
			URL:      fmt.Sprintf("http://localhost:8090/mr/org/%s/metrics", models.Org1UUID),
			Username: "invalid",
			Password: promToken,
			Response: `{"error": "invalid authentication"}`,
		},
		{
			URL:      fmt.Sprintf("http://localhost:8090/mr/org/%s/metrics", models.Org2UUID),
			Username: "metrics",
			Password: promToken,
			Response: `{"error": "invalid authentication"}`,
		},
		{
			URL:      fmt.Sprintf("http://localhost:8090/mr/org/%s/metrics", models.Org1UUID),
			Username: "metrics",
			Password: adminToken,
			Response: `{"error": "invalid authentication"}`,
		},
		{
			URL:      fmt.Sprintf("http://localhost:8090/mr/org/%s/metrics", models.Org1UUID),
			Username: "metrics",
			Password: promToken,
			Contains: []string{
				`rapidpro_group_contact_count{group_name="Active",group_uuid="d1ee73f0-bdb5-47ce-99dd-0c95d4ebf008",group_type="system",org="UNICEF"} 124`,
				`rapidpro_group_contact_count{group_name="Doctors",group_uuid="c153e265-f7c9-4539-9dbc-9b358714b638",group_type="user",org="UNICEF"} 121`,
				`rapidpro_channel_msg_count{channel_name="Nexmo",channel_uuid="19012bfd-3ce3-4cae-9bb9-76cf92c73d49",channel_type="NX",msg_direction="out",msg_type="message",org="UNICEF"} 0`,
			},
		},
	}

	for i, tc := range tcs {
		req, _ := http.NewRequest(http.MethodGet, tc.URL, nil)
		req.SetBasicAuth(tc.Username, tc.Password)
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err, "%d received error")

		body, _ := ioutil.ReadAll(resp.Body)

		if tc.Response != "" {
			assert.Equal(t, string(body), tc.Response, "%d: response mismatch", i)
		}
		for _, contains := range tc.Contains {
			assert.Contains(t, string(body), contains, "%d does not contain: %s", i, contains)
		}
	}
}
