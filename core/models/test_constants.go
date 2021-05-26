package models

import (
	"github.com/nyaruka/goflow/assets"
)

// TODO move these to testsuite/testdata/constants.go

var TwilioChannelID = ChannelID(10000)
var TwilioChannelUUID = assets.ChannelUUID("74729f45-7f29-4868-9dc4-90e491e3c7d8")

var VonageChannelID = ChannelID(10001)
var VonageChannelUUID = assets.ChannelUUID("19012bfd-3ce3-4cae-9bb9-76cf92c73d49")

var TwitterChannelID = ChannelID(10002)
var TwitterChannelUUID = assets.ChannelUUID("0f661e8b-ea9d-4bd3-9953-d368340acf91")

var DoctorRemindersCampaignUUID = CampaignUUID("72aa12c5-cc11-4bc7-9406-044047845c70")
var DoctorRemindersCampaignID = CampaignID(10000)

var RemindersEvent1ID = CampaignEventID(10000)
var RemindersEvent2ID = CampaignEventID(10001)

var DoctorsGroupID = GroupID(10000)
var DoctorsGroupUUID = assets.GroupUUID("c153e265-f7c9-4539-9dbc-9b358714b638")

var AllContactsGroupID = GroupID(1)
var AllContactsGroupUUID = assets.GroupUUID("d1ee73f0-bdb5-47ce-99dd-0c95d4ebf008")

var BlockedContactsGroupID = GroupID(2)
var BlockedContactsGroupUUID = assets.GroupUUID("9295ebab-5c2d-4eb1-86f9-7c15ed2f3219")

var TestersGroupID = GroupID(10001)
var TestersGroupUUID = assets.GroupUUID("5e9d8fab-5e7e-4f51-b533-261af5dea70d")

var CreatedOnFieldID = FieldID(3)
var LastSeenOnFieldID = FieldID(5)

var AgeFieldUUID = assets.FieldUUID("903f51da-2717-47c7-a0d3-f2f32877013d")
var GenderFieldUUID = assets.FieldUUID("3a5891e4-756e-4dc9-8e12-b7a766168824")

var ReportingLabelID = LabelID(10000)
var ReportingLabelUUID = assets.LabelUUID("ebc4dedc-91c4-4ed4-9dd6-daa05ea82698")

var TestingLabelID = LabelID(10001)
var TestingLabelUUID = assets.LabelUUID("a6338cdc-7938-4437-8b05-2d5d785e3a08")
