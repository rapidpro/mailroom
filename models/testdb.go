package models

import (
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows"
)

const (
	Org1     = OrgID(1)
	Channel1 = ChannelID(1)

	Cathy      = flows.ContactID(43)
	CathyURN   = urns.URN("tel:+250700000002")
	CathyURNID = URNID(43)

	Bob      = flows.ContactID(58)
	BobURN   = urns.URN("tel:+250700000017")
	BobURNID = URNID(59)

	Evan = flows.ContactID(47)
)
