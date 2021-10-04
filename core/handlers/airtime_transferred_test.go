package handlers_test

import (
	"testing"

	"github.com/nyaruka/gocommon/httpx"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"

	"github.com/shopspring/decimal"
)

var lookupNumberResponse = `[
	{
		"country": {
		"iso_code": "ECU",
		"name": "Ecuador",
		"regions": null
		},
		"id": 1596,
		"identified": true,
		"name": "Claro Ecuador",
		"regions": null
	},
	{
		"country": {
		"iso_code": "ECU",
		"name": "Ecuador",
		"regions": null
		},
		"id": 1597,
		"identified": false,
		"name": "CNT Ecuador",
		"regions": null
	}
]`

var productsResponse = `[
	{
		"availability_zones": [
			"INTERNATIONAL"
		],
		"benefits": [
			{
			"additional_information": null,
			"amount": {
				"base": 3,
				"promotion_bonus": 0,
				"total_excluding_tax": 3
			},
			"type": "CREDITS",
			"unit": "USD",
			"unit_type": "CURRENCY"
			}
		],
		"description": "",
		"destination": {
			"amount": 3,
			"unit": "USD",
			"unit_type": "CURRENCY"
		},
		"id": 6035,
		"name": "3 USD",
		"operator": {
			"country": {
			"iso_code": "ECU",
			"name": "Ecuador",
			"regions": null
			},
			"id": 1596,
			"name": "Claro Ecuador",
			"regions": null
		},
		"prices": {
			"retail": {
			"amount": 4,
			"fee": 0,
			"unit": "USD",
			"unit_type": "CURRENCY"
			},
			"wholesale": {
			"amount": 3.6,
			"fee": 0,
			"unit": "USD",
			"unit_type": "CURRENCY"
			}
		},
		"promotions": null,
		"rates": {
			"base": 0.833333333333333,
			"retail": 0.75,
			"wholesale": 0.833333333333333
		},
		"regions": null,
		"required_beneficiary_fields": null,
		"required_credit_party_identifier_fields": [
			[
			"mobile_number"
			]
		],
		"required_debit_party_identifier_fields": null,
		"required_sender_fields": null,
		"service": {
			"id": 1,
			"name": "Mobile"
		},
		"source": {
			"amount": 3.6,
			"unit": "USD",
			"unit_type": "CURRENCY"
		},
		"type": "FIXED_VALUE_RECHARGE",
		"validity": null
		},
		{
		"availability_zones": [
			"INTERNATIONAL"
		],
		"benefits": [
			{
			"additional_information": null,
			"amount": {
				"base": 6,
				"promotion_bonus": 0,
				"total_excluding_tax": 6
			},
			"type": "CREDITS",
			"unit": "USD",
			"unit_type": "CURRENCY"
			}
		],
		"description": "",
		"destination": {
			"amount": 6,
			"unit": "USD",
			"unit_type": "CURRENCY"
		},
		"id": 6036,
		"name": "6 USD",
		"operator": {
			"country": {
			"iso_code": "ECU",
			"name": "Ecuador",
			"regions": null
			},
			"id": 1596,
			"name": "Claro Ecuador",
			"regions": null
		},
		"prices": {
			"retail": {
			"amount": 7,
			"fee": 0,
			"unit": "USD",
			"unit_type": "CURRENCY"
			},
			"wholesale": {
			"amount": 6.3,
			"fee": 0,
			"unit": "USD",
			"unit_type": "CURRENCY"
			}
		},
		"promotions": null,
		"rates": {
			"base": 0.952380952380952,
			"retail": 0.857142857142857,
			"wholesale": 0.952380952380952
		},
		"regions": null,
		"required_beneficiary_fields": null,
		"required_credit_party_identifier_fields": [
			[
				"mobile_number"
			]
		],
		"required_debit_party_identifier_fields": null,
		"required_sender_fields": null,
		"service": {
			"id": 1,
			"name": "Mobile"
		},
		"source": {
			"amount": 6.3,
			"unit": "USD",
			"unit_type": "CURRENCY"
		},
		"type": "FIXED_VALUE_RECHARGE",
		"validity": null
	}
]`

var transactionRejectedResponse = `{
	"benefits": [
		{
			"additional_information": null,
			"amount": {
				"base": 3,
				"promotion_bonus": 0,
				"total_excluding_tax": 3
			},
			"type": "CREDITS",
			"unit": "USD",
			"unit_type": "CURRENCY"
		}
	],
	"confirmation_date": "2021-03-24T20:05:06.111631000Z",
	"confirmation_expiration_date": "2021-03-24T21:05:05.883561000Z",
	"creation_date": "2021-03-24T20:05:05.883561000Z",
	"credit_party_identifier": {
		"mobile_number": "+593979123456"
	},
	"external_id": "EX12345",
	"id": 2237512891,
	"prices": {
		"retail": {
			"amount": 4,
			"fee": 0,
			"unit": "USD",
			"unit_type": "CURRENCY"
		},
		"wholesale": {
			"amount": 3.6,
			"fee": 0,
			"unit": "USD",
			"unit_type": "CURRENCY"
		}
	},
	"product": {
		"description": "",
		"id": 6035,
		"name": "3 USD",
		"operator": {
			"country": {
				"iso_code": "ECU",
				"name": "Ecuador",
				"regions": null
			},
			"id": 1596,
			"name": "Claro Ecuador",
			"regions": null
		},
		"regions": null,
		"service": {
			"id": 1,
			"name": "Mobile"
		},
		"type": "FIXED_VALUE_RECHARGE"
	},
	"promotions": null,
	"rates": {
		"base": 0.833333333333333,
		"retail": 0.75,
		"wholesale": 0.833333333333333
	},
	"status": {
		"class": {
			"id": 2,
			"message": "CONFIRMED"
		},
		"id": 20000,
		"message": "CONFIRMED"
	}
}`

func TestAirtimeTransferred(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()

	defer testsuite.Reset(testsuite.ResetAll)
	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://dvs-api.dtone.com/v1/lookup/mobile-number/+16055741111": {
			httpx.NewMockResponse(200, nil, lookupNumberResponse), // successful mobile number lookup
		},
		"https://dvs-api.dtone.com/v1/products?type=FIXED_VALUE_RECHARGE&operator_id=1596&per_page=100": {
			httpx.NewMockResponse(200, nil, productsResponse),
		},
		"https://dvs-api.dtone.com/v1/sync/transactions": {
			httpx.NewMockResponse(200, nil, transactionRejectedResponse),
		},
		"https://dvs-api.dtone.com/v1/lookup/mobile-number/+16055743333": {
			httpx.MockConnectionError, // timeout x 3 retries
			httpx.MockConnectionError,
			httpx.MockConnectionError,
		},
	}))

	db.MustExec(`UPDATE orgs_org SET config = '{"dtone_key": "key123", "dtone_secret": "sesame"}'::jsonb WHERE id = $1`, testdata.Org1.ID)

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				testdata.Cathy: []flows.Action{
					actions.NewTransferAirtime(handlers.NewActionUUID(), map[string]decimal.Decimal{"USD": decimal.RequireFromString(`3.50`)}, "Transfer"),
				},
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   `select count(*) from airtime_airtimetransfer where org_id = $1 AND contact_id = $2 AND status = 'S'`,
					Args:  []interface{}{testdata.Org1.ID, testdata.Cathy.ID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from request_logs_httplog where org_id = $1 AND airtime_transfer_id IS NOT NULL AND is_error = FALSE AND url LIKE 'https://dvs-api.dtone.com/v1/%'`,
					Args:  []interface{}{testdata.Org1.ID},
					Count: 3,
				},
			},
		},
		{
			Actions: handlers.ContactActionMap{
				testdata.George: []flows.Action{
					actions.NewTransferAirtime(handlers.NewActionUUID(), map[string]decimal.Decimal{"USD": decimal.RequireFromString(`3.50`)}, "Transfer"),
				},
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   `select count(*) from airtime_airtimetransfer where org_id = $1 AND contact_id = $2 AND status = 'F'`,
					Args:  []interface{}{testdata.Org1.ID, testdata.George.ID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from request_logs_httplog where org_id = $1 AND airtime_transfer_id IS NOT NULL AND is_error = TRUE AND url LIKE 'https://dvs-api.dtone.com/v1/%'`,
					Args:  []interface{}{testdata.Org1.ID},
					Count: 1,
				},
			},
		},
	}

	handlers.RunTestCases(t, ctx, rt, tcs)
}
