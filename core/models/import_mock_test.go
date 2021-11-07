package models

import (
	"fmt"
	"github.com/twilio/twilio-go"
	openapi "github.com/twilio/twilio-go/rest/lookups/v1"
)

var MockFnInitLookup = func () {
	getValidationFn = func (*twilio.RestClient) FetchPhoneNumber {
		fnOverride := func (phone string, params *openapi.FetchPhoneNumberParams) (*openapi.LookupsV1PhoneNumber, error) {
			responseSample := make(map[string]interface{})
			if phone == "tel:+16055740001" {
				responseSample["type"] = "mobile"
				responseSample["name"] = "AT&T Wireless"
			}

			if phone == "tel:+16055740002" {
				responseSample["type"] = "mobile"
				responseSample["name"] = "Cricket Wireless - ATT - SVR"
			}

			if phone == "tel:+23412703232" {
				responseSample["type"] = "landline"
				responseSample["name"] = "21st Century Technologies Limited"
			}

			var returnValue = &openapi.LookupsV1PhoneNumber{
				Carrier: &responseSample,
				PhoneNumber: &phone,
			}

			if phone == "tel:+23480395295011" {
				return returnValue, fmt.Errorf("the requested resource /PhoneNumbers/%s was not found - 404", phone)
			}
			return returnValue, nil
		}

		return fnOverride
	}
}
