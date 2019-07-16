package search

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"testing"
	"time"

	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/utils"
	"github.com/stretchr/testify/assert"
)

type MockRegistry struct {
	Fields map[string]*Field
	IsAnon bool
}

func (r *MockRegistry) LookupSearchField(key string) *Field {
	field := r.Fields[key]
	if field == nil {
		return field
	}

	if field.Category == Scheme && r.IsAnon {
		return &Field{key, Unavailable, Text, ""}
	}

	if field.Category == Implicit && r.IsAnon {
		return &Field{"name_id", Implicit, Text, ""}
	}

	return field
}

func TestElasticQuery(t *testing.T) {
	registry := &MockRegistry{
		Fields: map[string]*Field{
			"name":       &Field{"name", ContactAttribute, Text, ""},
			"id":         &Field{"id", ContactAttribute, Text, ""},
			"language":   &Field{"language", ContactAttribute, Text, ""},
			"created_on": &Field{"created_on", ContactAttribute, DateTime, ""},

			"age":      &Field{"age", ContactField, Number, "6b6a43fa-a26d-4017-bede-328bcdd5c93b"},
			"color":    &Field{"color", ContactField, Text, "ecc7b13b-c698-4f46-8a90-24a8fab6fe34"},
			"dob":      &Field{"dob", ContactField, DateTime, "cbd3fc0e-9b74-4207-a8c7-248082bb4572"},
			"state":    &Field{"state", ContactField, State, "67663ad1-3abc-42dd-a162-09df2dea66ec"},
			"district": &Field{"district", ContactField, District, "54c72635-d747-4e45-883c-099d57dd998e"},
			"ward":     &Field{"ward", ContactField, Ward, "fde8f740-c337-421b-8abb-83b954897c80"},

			"tel":      &Field{"tel", Scheme, Text, ""},
			"whatsapp": &Field{"whatsapp", Scheme, Text, ""},

			"*": &Field{"name_tel", Implicit, Text, ""},
		},
	}

	type TestCase struct {
		Label  string          `json:"label"`
		Search string          `json:"search"`
		Query  json.RawMessage `json:"query"`
		Error  string          `json:"error"`
		IsAnon bool            `json:"is_anon"`
	}

	tcs := make([]TestCase, 0, 20)
	tcJSON, err := ioutil.ReadFile("testdata/elastic_test.json")
	assert.NoError(t, err)

	err = json.Unmarshal(tcJSON, &tcs)
	assert.NoError(t, err)

	ny, _ := time.LoadLocation("America/New_York")
	env := utils.NewEnvironmentBuilder().WithTimezone(ny).Build()

	for _, tc := range tcs {
		registry.IsAnon = tc.IsAnon

		parsed, err := contactql.ParseQuery(tc.Search)
		assert.NoError(t, err, "%s: error received parsing: ", tc.Label, tc.Search)

		query, err := ToElasticQuery(env, registry, parsed)

		if tc.Error != "" {
			assert.Error(t, err, "%s: error not received converting to elastic: %s", tc.Label, tc.Search)
			if err != nil {
				assert.Contains(t, err.Error(), tc.Error)
			}
			continue
		}

		assert.NoError(t, err, "%s: error received converting to elastic: %s", tc.Label, tc.Search)
		if err != nil {
			continue
		}

		assert.NotNil(t, query, tc.Label)
		if query == nil {
			continue
		}

		source, err := query.Source()
		assert.NoError(t, err, tc.Label)
		if err != nil {
			continue
		}

		asJSON, err := json.Marshal(source)
		assert.NoError(t, err, tc.Label)
		if err != nil {
			continue
		}

		compacted := &bytes.Buffer{}
		json.Compact(compacted, tc.Query)

		assert.Equal(t, compacted.String(), string(asJSON), "%s: generated query does not match for: %s", tc.Label, tc.Search)
	}
}
