package hooks

import (
	"strings"
	"testing"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/goflow/utils/httpx"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/testsuite"

	"github.com/shopspring/decimal"
)

const msisdnResponse = `country=Ecuador
countryid=727
operator=Movistar Ecuador
operatorid=1472
connection_status=100
destination_msisdn=593999000001
destination_currency=USD
product_list=1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,33,40,44,50,55,60,70
retail_price_list=1.30,2.60,3.90,5.20,6.50,7.80,9.00,10.40,11.70,13.00,14.30,15.60,16.90,18.20,19.50,20.80,22.10,23.40,24.70,26.00,27.30,28.60,29.90,31.20,32.50,33.80,35.00,36.30,37.60,38.90,40.60,51.90,54.10,64.80,67.60,73.80,86.10
wholesale_price_list=0.99,1.98,2.97,3.96,4.95,5.82,6.79,7.76,8.73,9.70,10.67,11.64,12.61,13.58,14.55,15.52,16.49,17.46,18.43,19.40,20.37,21.34,22.31,23.28,24.25,25.22,26.19,27.16,28.13,29.10,32.45,38.80,43.26,48.50,54.07,58.98,68.81
local_info_value_list=1.00,2.00,3.00,4.00,5.00,6.00,7.00,8.00,9.00,10.00,11.00,12.00,13.00,14.00,15.00,16.00,17.00,18.00,19.00,20.00,21.00,22.00,23.00,24.00,25.00,26.00,27.00,28.00,29.00,30.00,33.00,40.00,44.00,50.00,55.00,60.00,70.00
local_info_amount_list=1.00,2.00,3.00,4.00,5.00,6.00,7.00,8.00,9.00,10.00,11.00,12.00,13.00,14.00,15.00,16.00,17.00,18.00,19.00,20.00,21.00,22.00,23.00,24.00,25.00,26.00,27.00,28.00,29.00,30.00,33.00,40.00,44.00,50.00,55.00,60.00,70.00
local_info_currency=USD
authentication_key=4433322221111
error_code=0
error_txt=Transaction successful
`

const reserveResponse = `reserved_id=123456789
authentication_key=4433322221111
error_code=0
error_txt=Transaction successful
`

const topupResponse = `transactionid=837765537
msisdn=a friend
destination_msisdn=593999000001
country=Ecuador
countryid=727
operator=Movistar Ecuador
operatorid=1472
reference_operator=
originating_currency=USD
destination_currency=USD
product_requested=1
actual_product_sent=1
wholesale_price=0.99
retail_price=1.30
balance=27.03
sms_sent=yes
sms=
cid1=
cid2=
cid3=
authentication_key=4433322221111
error_code=0
error_txt=Transaction successful
`

var withCRLF = func(s string) string { return strings.Replace(s, "\n", "\r\n", -1) }

func TestAirtimeTransferred(t *testing.T) {
	testsuite.Reset()

	defer httpx.SetRequestor(httpx.DefaultRequestor)

	httpx.SetRequestor(httpx.NewMockRequestor(map[string][]httpx.MockResponse{
		"https://airtime-api.dtone.com/cgi-bin/shop/topup": []httpx.MockResponse{
			httpx.NewMockResponse(200, nil, withCRLF(msisdnResponse), 1),
			httpx.NewMockResponse(200, nil, withCRLF(reserveResponse), 1),
			httpx.NewMockResponse(200, nil, withCRLF(topupResponse), 1),
			httpx.NewMockResponse(200, nil, "error_code=13\r\nerror_txt=Oops\r\n", 1),
		},
	}))

	testsuite.DB().MustExec(
		`UPDATE orgs_org SET config = '{"TRANSFERTO_ACCOUNT_LOGIN": "nyaruka", "TRANSFERTO_AIRTIME_API_TOKEN": "123456789"}'::jsonb
		WHERE id = $1`, models.Org1)

	tcs := []HookTestCase{
		HookTestCase{
			Actions: ContactActionMap{
				models.CathyID: []flows.Action{
					actions.NewTransferAirtime(newActionUUID(), map[string]decimal.Decimal{"USD": decimal.RequireFromString(`1.20`)}, "Transfer"),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) from airtime_airtimetransfer where org_id = $1 AND contact_id = $2 AND status = 'S'`,
					Args:  []interface{}{models.Org1, models.CathyID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from request_logs_httplog where org_id = $1 AND airtime_transfer_id IS NOT NULL AND is_error = FALSE AND url = 'https://airtime-api.dtone.com/cgi-bin/shop/topup'`,
					Args:  []interface{}{models.Org1},
					Count: 3,
				},
			},
		},
		HookTestCase{
			Actions: ContactActionMap{
				models.GeorgeID: []flows.Action{
					actions.NewTransferAirtime(newActionUUID(), map[string]decimal.Decimal{"USD": decimal.RequireFromString(`1.20`)}, "Transfer"),
				},
			},
			SQLAssertions: []SQLAssertion{
				SQLAssertion{
					SQL:   `select count(*) from airtime_airtimetransfer where org_id = $1 AND contact_id = $2 AND status = 'F'`,
					Args:  []interface{}{models.Org1, models.GeorgeID},
					Count: 1,
				},
				SQLAssertion{
					SQL:   `select count(*) from request_logs_httplog where org_id = $1 AND airtime_transfer_id IS NOT NULL AND is_error = TRUE AND url = 'https://airtime-api.dtone.com/cgi-bin/shop/topup'`,
					Args:  []interface{}{models.Org1},
					Count: 1,
				},
			},
		},
	}

	RunActionTestCases(t, tcs)
}
