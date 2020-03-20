package org

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"testing"

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/web"
	"github.com/stretchr/testify/assert"
)

func TestMetrics(t *testing.T) {
	ctx, db, rp := testsuite.Reset()

	token := "2d26a50841ff48237238bbdd021150f6a33a4196"
	db.MustExec(`INSERT INTO api_apitoken(is_active, org_id, created, key, role_id, user_id) VALUES(TRUE, $1, NOW(), $2, 1, 1);`, models.Org1, token)

	wg := &sync.WaitGroup{}
	server := web.NewServer(ctx, config.Mailroom, db, rp, nil, nil, wg)
	server.Start()
	defer server.Stop()

	tcs := []struct {
		URL      string
		Contains []string
	}{
		{
			URL:      "http://localhost:8090/mr/org/011/metrics",
			Contains: []string{`{"error": "invalid token"}`},
		},
		{
			URL: fmt.Sprintf("http://localhost:8090/mr/org/%s/metrics", token),
			Contains: []string{
				`rapidpro_group_contact_count{group_name="All Contacts",group_uuid="966e2bdb-078c-42bb-afd7-c5c1dbe31aa6",group_type="system",org="UNICEF"} 124`,
				`rapidpro_group_contact_count{group_name="Doctors",group_uuid="c153e265-f7c9-4539-9dbc-9b358714b638",group_type="user",org="UNICEF"} 121`,
				`rapidpro_channel_msg_count{channel_name="Nexmo",channel_uuid="19012bfd-3ce3-4cae-9bb9-76cf92c73d49",channel_type="NX",msg_direction="out",msg_type="message",org="UNICEF"} 0`,
			},
		},
	}

	for i, tc := range tcs {
		resp, err := http.Get(tc.URL)
		assert.NoError(t, err, "%d received error")

		body, _ := ioutil.ReadAll(resp.Body)

		for _, contains := range tc.Contains {
			assert.Contains(t, string(body), contains, "%d does not contain: %s", i, contains)
		}
	}
}
