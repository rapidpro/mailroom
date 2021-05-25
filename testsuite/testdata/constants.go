package testdata

import (
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
)

// Constants used in tests, these are tied to the DB created by the RapidPro `mailroom_db` management command.

type Org struct {
	ID   models.OrgID
	UUID uuids.UUID
}

type Channel struct {
	ID   models.ChannelID
	UUID assets.ChannelUUID
}

type Contact struct {
	ID    models.ContactID
	UUID  flows.ContactUUID
	URN   urns.URN
	URNID models.URNID
}

type Flow struct {
	ID   models.FlowID
	UUID assets.FlowUUID
}

type Ticketer struct {
	ID   models.TicketerID
	UUID assets.TicketerUUID
}

type Classifier struct {
	ID   models.ClassifierID
	UUID assets.ClassifierUUID
}

var Org1 = Org{1, "bf0514a5-9407-44c9-b0f9-3f36f9c18414"}

var Cathy = Contact{10000, "6393abc0-283d-4c9b-a1b3-641a035c34bf", "tel:+16055741111", 10000}
var Bob = Contact{10001, "b699a406-7e44-49be-9f01-1a82893e8a10", "tel:+16055742222", 10001}
var George = Contact{10002, "8d024bcd-f473-4719-a00a-bd0bb1190135", "tel:+16055743333", 10002}
var Alexandria = Contact{10003, "9709c157-4606-4d41-9df3-9e9c9b4ae2d4", "tel:+16055744444", 10003}

var Favorites = Flow{10000, "9de3663f-c5c5-4c92-9f45-ecbc09abcc85"}
var PickANumber = Flow{10001, "5890fe3a-f204-4661-b74d-025be4ee019c"}
var SingleMessage = Flow{10004, "a7c11d68-f008-496f-b56d-2d5cf4cf16a5"}
var IVRFlow = Flow{10003, "2f81d0ea-4d75-4843-9371-3f7465311cce"}
var SurveyorFlow = Flow{10005, "ed8cf8d4-a42c-4ce1-a7e3-44a2918e3cec"}
var IncomingExtraFlow = Flow{10006, "376d3de6-7f0e-408c-80d6-b1919738bc80"}
var ParentTimeoutFlow = Flow{10007, "81c0f323-7e06-4e0c-a960-19c20f17117c"}
var CampaignFlow = Flow{10009, "3a92a964-3a8d-420b-9206-2cd9d884ac30"}

var Mailgun = Ticketer{1, "f9c9447f-a291-4f3c-8c79-c089bbd4e713"}
var Zendesk = Ticketer{2, "4ee6d4f3-f92b-439b-9718-8da90c05490b"}
var RocketChat = Ticketer{3, "6c50665f-b4ff-4e37-9625-bc464fe6a999"}
var Internal = Ticketer{4, "8bd48029-6ca1-46a8-aa14-68f7213b82b3"}

var Luis = Classifier{1, "097e026c-ae79-4740-af67-656dbedf0263"}
var Wit = Classifier{2, "ff2a817c-040a-4eb2-8404-7d92e8b79dd0"}
var Bothub = Classifier{3, "859b436d-3005-4e43-9ad5-3de5f26ede4c"}

// secondary org.. only a few things
var Org2 = Org{2, "3ae7cdeb-fd96-46e5-abc4-a4622f349921"}
var Org2Channel = Channel{20000, "a89bc872-3763-4b95-91d9-31d4e56c6651"}
var Org2Contact = Contact{20000, "26d20b72-f7d8-44dc-87f2-aae046dbff95", "tel:+250700000005", 20000}
var Org2Favorites = Flow{20000, "f161bd16-3c60-40bd-8c92-228ce815b9cd"}
var Org2SingleMessage = Flow{20001, "5277916d-6011-41ac-a4a4-f6ac6a4f1dd9"}
