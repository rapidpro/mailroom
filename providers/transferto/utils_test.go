package transferto_test

import (
	"encoding/json"
	"testing"

	"github.com/nyaruka/mailroom/providers/transferto"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestCSVStrings(t *testing.T) {
	s := &struct {
		List1 transferto.CSVStrings `json:"list1"`
		List2 transferto.CSVStrings `json:"list2"`
	}{}
	err := json.Unmarshal([]byte(`{"list1":"foo","list2":"foo,bar"}`), s)
	assert.NoError(t, err)
	assert.Equal(t, transferto.CSVStrings{"foo"}, s.List1)
	assert.Equal(t, transferto.CSVStrings{"foo", "bar"}, s.List2)

	// try with invalid JSON
	err = json.Unmarshal([]byte(`{"list1":true}`), s)
	assert.Error(t, err)
}

func TestCSVDecimals(t *testing.T) {
	s := &struct {
		List1 transferto.CSVDecimals `json:"list1"`
		List2 transferto.CSVDecimals `json:"list2"`
	}{}
	err := json.Unmarshal([]byte(`{"list1":"12.34","list2":"12.34,56.78"}`), s)
	assert.NoError(t, err)
	assert.Equal(t, transferto.CSVDecimals{decimal.RequireFromString("12.34")}, s.List1)
	assert.Equal(t, transferto.CSVDecimals{decimal.RequireFromString("12.34"), decimal.RequireFromString("56.78")}, s.List2)

	// try with invalid JSON
	err = json.Unmarshal([]byte(`{"list1":true}`), s)
	assert.Error(t, err)
}
