package search_test

import (
	"testing"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/search"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchMessages(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)

	defer testsuite.Reset(t, rt, testsuite.ResetOpenSearch|testsuite.ResetDynamo)

	testsuite.IndexMessages(t, rt, []search.MessageDoc{
		{CreatedOn: time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC), OrgID: testdb.Org1.ID, UUID: "01968bb7-ca00-7000-8000-000000000001", ContactUUID: testdb.Ann.UUID, Text: "hello world"},
		{CreatedOn: time.Date(2026, 1, 15, 13, 0, 0, 0, time.UTC), OrgID: testdb.Org1.ID, UUID: "01968bee-b880-7000-8000-000000000002", ContactUUID: testdb.Bob.UUID, Text: "hello there friend", InTicket: true},
		{CreatedOn: time.Date(2026, 1, 15, 14, 0, 0, 0, time.UTC), OrgID: testdb.Org1.ID, UUID: "01968c25-a700-7000-8000-000000000003", ContactUUID: testdb.Cat.UUID, Text: "goodbye world"},
		{CreatedOn: time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC), OrgID: testdb.Org2.ID, UUID: "01968bb7-ca00-7000-9000-000000000001", ContactUUID: testdb.Org2Contact.UUID, Text: "hello world"},
	})

	tcs := []struct {
		label       string
		text        string
		contactUUID flows.ContactUUID
		inTicket    bool
		limit       int
		expected    []flows.ContactUUID
	}{
		{
			label:    "matching two messages",
			text:     "hello",
			limit:    50,
			expected: []flows.ContactUUID{testdb.Bob.UUID, testdb.Ann.UUID},
		},
		{
			label:    "matching one message",
			text:     "goodbye",
			limit:    50,
			expected: []flows.ContactUUID{testdb.Cat.UUID},
		},
		{
			label:    "matching no messages",
			text:     "xyznotfound",
			limit:    50,
			expected: []flows.ContactUUID{},
		},
		{
			label:       "filtered by contact",
			text:        "hello",
			contactUUID: testdb.Bob.UUID,
			limit:       50,
			expected:    []flows.ContactUUID{testdb.Bob.UUID},
		},
		{
			label:    "filtered by in_ticket",
			text:     "hello",
			inTicket: true,
			limit:    50,
			expected: []flows.ContactUUID{testdb.Bob.UUID},
		},
		{
			label:    "without in_ticket returns all",
			text:     "hello",
			inTicket: false,
			limit:    50,
			expected: []flows.ContactUUID{testdb.Bob.UUID, testdb.Ann.UUID},
		},
		{
			label:    "respects limit",
			text:     "hello",
			limit:    1,
			expected: []flows.ContactUUID{testdb.Bob.UUID},
		},
		{
			label:    "multi-word match requires all terms",
			text:     "hello world",
			limit:    50,
			expected: []flows.ContactUUID{testdb.Ann.UUID},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.label, func(t *testing.T) {
			results, err := search.SearchMessages(ctx, rt, testdb.Org1.ID, tc.text, tc.contactUUID, tc.inTicket, tc.limit)
			require.NoError(t, err)

			contactUUIDs := make([]flows.ContactUUID, len(results))
			for i, r := range results {
				contactUUIDs[i] = r.ContactUUID
			}
			assert.Equal(t, tc.expected, contactUUIDs, "unexpected results for: %s", tc.label)
		})
	}
}
