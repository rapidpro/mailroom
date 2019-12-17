package models

import (
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
)

// Constants used in tests, these are tied to the DB created by the
// RapidPro `mailroom_db` management command. These need to live in the master /models
// dir because we are using typed values and otherwire we'd have a circular dependency.
//
// Solution there would be to create a new package for ID types which would let us put
// these in testsuite or the like.
//
// Note that integer ids MAY be fragile depending on how clumsy people are adding things
// to the mailroom_db command (hint, add things to the end). If this turns into an issue
// we could start deriving these instead from the UUIDs.

var Org1 = OrgID(1)
var TwilioChannelID = ChannelID(10000)
var TwilioChannelUUID = assets.ChannelUUID("74729f45-7f29-4868-9dc4-90e491e3c7d8")

var NexmoChannelID = ChannelID(10001)
var NexmoChannelUUID = assets.ChannelUUID("19012bfd-3ce3-4cae-9bb9-76cf92c73d49")

var TwitterChannelID = ChannelID(10002)
var TwitterChannelUUID = assets.ChannelUUID("0f661e8b-ea9d-4bd3-9953-d368340acf91")

var CathyID = ContactID(10000)
var CathyUUID = flows.ContactUUID("6393abc0-283d-4c9b-a1b3-641a035c34bf")
var CathyURN = urns.URN("tel:+250700000001")
var CathyURNID = URNID(10000)

var BobID = ContactID(10001)
var BobUUID = flows.ContactUUID("b699a406-7e44-49be-9f01-1a82893e8a10")
var BobURN = urns.URN("tel:+250700000002")
var BobURNID = URNID(10001)

var GeorgeID = ContactID(10002)
var GeorgeUUID = flows.ContactUUID("8d024bcd-f473-4719-a00a-bd0bb1190135")
var GeorgeURN = urns.URN("tel:+250700000003")
var GeorgeURNID = URNID(10002)

var AlexandriaID = ContactID(10003)
var AlexandriaUUID = flows.ContactUUID("9709c157-4606-4d41-9df3-9e9c9b4ae2d4")
var AlexandriaURN = urns.URN("tel:+250700000004")
var AlexandriaURNID = URNID(10003)

var FavoritesFlowID = FlowID(10000)
var FavoritesFlowUUID = assets.FlowUUID("9de3663f-c5c5-4c92-9f45-ecbc09abcc85")

var PickNumberFlowID = FlowID(10001)
var PickNumberFlowUUID = assets.FlowUUID("5890fe3a-f204-4661-b74d-025be4ee019c")

var SingleMessageFlowID = FlowID(10004)
var SingleMessageFlowUUID = assets.FlowUUID("a7c11d68-f008-496f-b56d-2d5cf4cf16a5")

var IVRFlowID = FlowID(10003)
var IVRFlowUUID = assets.FlowUUID("2f81d0ea-4d75-4843-9371-3f7465311cce")

var SurveyorFlowID = FlowID(10005)
var SurveyorFlowUUID = assets.FlowUUID("ed8cf8d4-a42c-4ce1-a7e3-44a2918e3cec")

var IncomingExtraFlowID = FlowID(10006)
var IncomingExtraFlowUUID = assets.FlowUUID("376d3de6-7f0e-408c-80d6-b1919738bc80")

var ParentTimeoutID = FlowID(10007)
var ParentTimeoutUUID = assets.FlowUUID("81c0f323-7e06-4e0c-a960-19c20f17117c")

var CampaignFlowID = FlowID(10009)
var CampaignFlowUUID = assets.FlowUUID("3a92a964-3a8d-420b-9206-2cd9d884ac30")

var DoctorRemindersCampaignUUID = CampaignUUID("72aa12c5-cc11-4bc7-9406-044047845c70")
var DoctorRemindersCampaignID = CampaignID(10000)

var RemindersEvent1ID = CampaignEventID(10000)
var RemindersEvent2ID = CampaignEventID(10001)

var DoctorsGroupID = GroupID(10000)
var DoctorsGroupUUID = assets.GroupUUID("c153e265-f7c9-4539-9dbc-9b358714b638")

var AllContactsGroupID = GroupID(1)
var AllContactsGroupUUID = assets.GroupUUID("bc268217-9ffa-49e0-883e-e4e09c252a5a")

var TestersGroupID = GroupID(10001)
var TestersGroupUUID = assets.GroupUUID("5e9d8fab-5e7e-4f51-b533-261af5dea70d")

var AgeFieldUUID = assets.FieldUUID("903f51da-2717-47c7-a0d3-f2f32877013d")
var GenderFieldUUID = assets.FieldUUID("3a5891e4-756e-4dc9-8e12-b7a766168824")

var CreatedOnFieldID = FieldID(2)

var ReportingLabelID = LabelID(10000)
var ReportingLabelUUID = assets.LabelUUID("ebc4dedc-91c4-4ed4-9dd6-daa05ea82698")

var TestingLabelID = LabelID(10001)
var TestingLabelUUID = assets.LabelUUID("a6338cdc-7938-4437-8b05-2d5d785e3a08")

var LuisID = ClassifierID(1)
var LuisUUID = assets.ClassifierUUID("097e026c-ae79-4740-af67-656dbedf0263")

var WitID = ClassifierID(2)
var WitUUID = assets.ClassifierUUID("ff2a817c-040a-4eb2-8404-7d92e8b79dd0")

var BothubID = ClassifierID(3)
var BothubUUID = assets.ClassifierUUID("859b436d-3005-4e43-9ad5-3de5f26ede4c")

// constants for org 2, just a few here

var Org2 = OrgID(2)
var Org2ChannelID = ChannelID(20000)
var Org2ChannelUUID = assets.ChannelUUID("a89bc872-3763-4b95-91d9-31d4e56c6651")

var Org2FredID = ContactID(20000)
var Org2FredUUID = flows.ContactUUID("26d20b72-f7d8-44dc-87f2-aae046dbff95")
var Org2FredURN = urns.URN("tel:+250700000005")
var Org2FredURNID = URNID(20000)

var Org2FavoritesFlowID = FlowID(20000)
var Org2FavoritesFlowUUID = assets.FlowUUID("f161bd16-3c60-40bd-8c92-228ce815b9cd")

var Org2SingleMessageFlowID = FlowID(20001)
var Org2SingleMessageFlowUUID = assets.FlowUUID("5277916d-6011-41ac-a4a4-f6ac6a4f1dd9")
