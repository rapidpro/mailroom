package testdata

// Constants used in tests, these are tied to the DB created by the RapidPro `mailroom_db` management command.

var AuthGroupIDs = map[string]int{
	"Alpha":            1,
	"Beta":             2,
	"Dashboard":        3,
	"Surveyors":        4,
	"Customer Support": 5,
	"Granters":         6,
	"Administrators":   7,
	"Editors":          8,
	"Viewers":          9,
	"Agents":           10,
	"Prometheus":       11,
}

var Org1 = &Org{1, "bf0514a5-9407-44c9-b0f9-3f36f9c18414"}
var Admin = &User{3, "admin1@nyaruka.com"}
var Editor = &User{4, "editor1@nyaruka.com"}
var Viewer = &User{5, "viewer1@nyaruka.com"}
var Agent = &User{6, "agent1@nyaruka.com"}
var Surveyor = &User{7, "surveyor1@nyaruka.com"}

var TwilioChannel = &Channel{10000, "74729f45-7f29-4868-9dc4-90e491e3c7d8", "T"}
var VonageChannel = &Channel{10001, "19012bfd-3ce3-4cae-9bb9-76cf92c73d49", "NX"}
var FacebookChannel = &Channel{10002, "0f661e8b-ea9d-4bd3-9953-d368340acf91", "FBA"}

var Cathy = &Contact{10000, "6393abc0-283d-4c9b-a1b3-641a035c34bf", "tel:+16055741111", 10000}
var Bob = &Contact{10001, "b699a406-7e44-49be-9f01-1a82893e8a10", "tel:+16055742222", 10001}
var George = &Contact{10002, "8d024bcd-f473-4719-a00a-bd0bb1190135", "tel:+16055743333", 10002}
var Alexandria = &Contact{10003, "9709c157-4606-4d41-9df3-9e9c9b4ae2d4", "tel:+16055744444", 10003}

var Favorites = &Flow{10000, "9de3663f-c5c5-4c92-9f45-ecbc09abcc85"}
var PickANumber = &Flow{10001, "5890fe3a-f204-4661-b74d-025be4ee019c"}
var SingleMessage = &Flow{10004, "a7c11d68-f008-496f-b56d-2d5cf4cf16a5"}
var IVRFlow = &Flow{10003, "2f81d0ea-4d75-4843-9371-3f7465311cce"}
var SurveyorFlow = &Flow{10005, "ed8cf8d4-a42c-4ce1-a7e3-44a2918e3cec"}
var IncomingExtraFlow = &Flow{10006, "376d3de6-7f0e-408c-80d6-b1919738bc80"}
var ParentTimeoutFlow = &Flow{10007, "81c0f323-7e06-4e0c-a960-19c20f17117c"}
var CampaignFlow = &Flow{10009, "3a92a964-3a8d-420b-9206-2cd9d884ac30"}

var CreatedOnField = &Field{3, "fd18a69d-7514-4b76-9fad-072641995e17"}
var LanguageField = &Field{4, "4307df2e-b00b-42b6-922b-4a1dcfc268d8"}
var LastSeenOnField = &Field{5, "660ebe03-b717-4a80-aebf-9b7c718266e1"}
var GenderField = &Field{6, "3a5891e4-756e-4dc9-8e12-b7a766168824"}
var AgeField = &Field{7, "903f51da-2717-47c7-a0d3-f2f32877013d"}
var JoinedField = &Field{8, "d83aae24-4bbf-49d0-ab85-6bfd201eac6d"}

var ActiveGroup = &Group{1, "b97f69f7-5edf-45c7-9fda-d37066eae91d"}
var BlockedGroup = &Group{2, "14f6ea01-456b-4417-b0b8-35e942f549f1"}
var StoppedGroup = &Group{3, "d1ee73f0-bdb5-47ce-99dd-0c95d4ebf008"}
var ArchivedGroup = &Group{4, "9295ebab-5c2d-4eb1-86f9-7c15ed2f3219"}
var OpenTicketsGroup = &Group{5, "361838c4-2866-495a-8990-9f3c222a7604"}
var DoctorsGroup = &Group{10000, "c153e265-f7c9-4539-9dbc-9b358714b638"}
var TestersGroup = &Group{10001, "5e9d8fab-5e7e-4f51-b533-261af5dea70d"}

var ReportingLabel = &Label{10000, "ebc4dedc-91c4-4ed4-9dd6-daa05ea82698"}
var TestingLabel = &Label{10001, "a6338cdc-7938-4437-8b05-2d5d785e3a08"}

var DefaultTopic = &Topic{1, "ffc903f7-8cbb-443f-9627-87106842d1aa"}
var SalesTopic = &Topic{2, "9ef2ff21-064a-41f1-8560-ccc990b4f937"}
var SupportTopic = &Topic{3, "0a8f2e00-fef6-402c-bd79-d789446ec0e0"}

var Partners = &Team{1, "4321c30b-b596-46fa-adb4-4a46d37923f6"}
var Office = &Team{2, "f14c1762-d38b-4072-ae63-2705332a3719"}

var Luis = &Classifier{1, "097e026c-ae79-4740-af67-656dbedf0263"}
var Wit = &Classifier{2, "ff2a817c-040a-4eb2-8404-7d92e8b79dd0"}
var Bothub = &Classifier{3, "859b436d-3005-4e43-9ad5-3de5f26ede4c"}

var RemindersCampaign = &Campaign{10000, "72aa12c5-cc11-4bc7-9406-044047845c70"}
var RemindersEvent1 = &CampaignEvent{10000, "f2a3f8c5-e831-4df3-b046-8d8cdb90f178"} // Favorites flow, interrupts
var RemindersEvent2 = &CampaignEvent{10001, "aff4b8ac-2534-420f-a353-66a3e74b6e16"} // Message event, passive
var RemindersEvent3 = &CampaignEvent{10002, "3e4f06c2-e04f-47ca-a047-f5252b3160ea"} // Pick A Number flow, skips

// secondary org.. only a few things
var Org2 = &Org{2, "3ae7cdeb-fd96-46e5-abc4-a4622f349921"}
var Org2Admin = &User{8, "admin2@nyaruka.com"}
var Org2Channel = &Channel{20000, "a89bc872-3763-4b95-91d9-31d4e56c6651", "T"}
var Org2Contact = &Contact{20000, "26d20b72-f7d8-44dc-87f2-aae046dbff95", "tel:+250700000005", 20000}
var Org2Favorites = &Flow{20000, "f161bd16-3c60-40bd-8c92-228ce815b9cd"}
var Org2SingleMessage = &Flow{20001, "5277916d-6011-41ac-a4a4-f6ac6a4f1dd9"}

func must(err error, checks ...bool) {
	if err != nil {
		panic(err)
	}
	for _, check := range checks {
		if !check {
			panic("check failed")
		}
	}
}
