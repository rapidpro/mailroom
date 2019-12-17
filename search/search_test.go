package search

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"testing"
	"time"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/stretchr/testify/assert"
)

type MockField struct {
	fieldKey  string
	fieldType assets.FieldType
	fieldUUID assets.FieldUUID
}

func (f *MockField) Key() string            { return f.fieldKey }
func (f *MockField) Name() string           { return f.fieldKey }
func (f *MockField) Type() assets.FieldType { return f.fieldType }
func (f *MockField) UUID() assets.FieldUUID { return f.fieldUUID }

func TestElasticQuery(t *testing.T) {
	registry := map[string]assets.Field{
		"age":      &MockField{"age", assets.FieldTypeNumber, "6b6a43fa-a26d-4017-bede-328bcdd5c93b"},
		"color":    &MockField{"color", assets.FieldTypeText, "ecc7b13b-c698-4f46-8a90-24a8fab6fe34"},
		"dob":      &MockField{"dob", assets.FieldTypeDatetime, "cbd3fc0e-9b74-4207-a8c7-248082bb4572"},
		"state":    &MockField{"state", assets.FieldTypeState, "67663ad1-3abc-42dd-a162-09df2dea66ec"},
		"district": &MockField{"district", assets.FieldTypeDistrict, "54c72635-d747-4e45-883c-099d57dd998e"},
		"ward":     &MockField{"ward", assets.FieldTypeWard, "fde8f740-c337-421b-8abb-83b954897c80"},
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

	resolver := func(key string) assets.Field {
		return registry[key]
	}

	for _, tc := range tcs {
		redactionPolicy := envs.RedactionPolicyNone
		if tc.IsAnon {
			redactionPolicy = envs.RedactionPolicyURNs
		}
		env := envs.NewBuilder().WithTimezone(ny).WithRedactionPolicy(redactionPolicy).Build()

		_, query, err := ToElasticQuery(env, resolver, tc.Search)

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
