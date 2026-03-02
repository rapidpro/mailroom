package search

import (
	"time"

	"github.com/nyaruka/gocommon/i18n"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/shopspring/decimal"
)

// ContactFieldDoc represents a single field value in a contact document for OpenSearch.
type ContactFieldDoc struct {
	Field           assets.FieldUUID `json:"field"`
	Text            string           `json:"text,omitempty"`
	Number          *decimal.Decimal `json:"number,omitempty"`
	Datetime        *time.Time       `json:"datetime,omitempty"`
	State           string           `json:"state,omitempty"`
	StateKeyword    string           `json:"state_keyword,omitempty"`
	District        string           `json:"district,omitempty"`
	DistrictKeyword string           `json:"district_keyword,omitempty"`
	Ward            string           `json:"ward,omitempty"`
	WardKeyword     string           `json:"ward_keyword,omitempty"`
}

// ContactURNDoc represents a single URN in a contact document for OpenSearch.
type ContactURNDoc struct {
	Scheme string `json:"scheme"`
	Path   string `json:"path"`
}

// ContactDoc represents a contact document in the OpenSearch contacts index. UUID is used as the document _id.
type ContactDoc struct {
	OrgID          models.OrgID         `json:"org_id"`
	UUID           flows.ContactUUID    `json:"-"` // used as _id
	Name           string               `json:"name,omitempty"`
	Status         models.ContactStatus `json:"status"`
	Language       i18n.Language        `json:"language,omitempty"`
	Fields         []*ContactFieldDoc   `json:"fields,omitempty"`
	URNs           []*ContactURNDoc     `json:"urns,omitempty"`
	GroupIDs       []models.GroupID     `json:"group_ids,omitempty"`
	FlowID         models.FlowID        `json:"flow_id,omitempty"`
	FlowHistoryIDs []models.FlowID      `json:"flow_history_ids,omitempty"` // TODO
	Tickets        int                  `json:"tickets"`
	CreatedOn      time.Time            `json:"created_on"`
	LastSeenOn     *time.Time           `json:"last_seen_on,omitempty"`
	LegacyID       models.ContactID     `json:"legacy_id"`
}

// NewContactDoc builds a ContactDoc from a flow contact and its org assets. We use the flow contact
// rather than the DB contact because it is kept up-to-date in memory as events are applied.
func NewContactDoc(oa *models.OrgAssets, c *flows.Contact) *ContactDoc {
	doc := &ContactDoc{
		OrgID:      oa.OrgID(),
		UUID:       c.UUID(),
		Name:       c.Name(),
		Status:     models.ContactToModelStatus[c.Status()],
		Language:   c.Language(),
		CreatedOn:  c.CreatedOn(),
		LastSeenOn: c.LastSeenOn(),
		Tickets:    c.Tickets().Open().Count(),
		LegacyID:   models.ContactID(c.ID()),
	}

	// build field docs from the flow contact's field values
	for key, fv := range c.Fields() {
		if fv == nil {
			continue
		}

		value := fv.Value
		if value == nil {
			continue
		}

		field := oa.FieldByKey(key)
		if field == nil {
			continue
		}

		fd := &ContactFieldDoc{Field: field.UUID()}

		if value.Text != nil && !value.Text.Empty() {
			fd.Text = value.Text.Native()
		}
		if value.Number != nil {
			n := value.Number.Native()
			fd.Number = &n
		}
		if value.Datetime != nil {
			t := value.Datetime.Native()
			fd.Datetime = &t
		}
		if value.State != "" {
			fd.State = string(value.State)
			fd.StateKeyword = value.State.Name()
		}
		if value.District != "" {
			fd.District = string(value.District)
			fd.DistrictKeyword = value.District.Name()
		}
		if value.Ward != "" {
			fd.Ward = string(value.Ward)
			fd.WardKeyword = value.Ward.Name()
		}

		doc.Fields = append(doc.Fields, fd)
	}

	// build URN docs
	for _, urn := range c.URNs() {
		doc.URNs = append(doc.URNs, &ContactURNDoc{Scheme: urn.Scheme, Path: urn.Path})
	}

	// build group IDs by looking up the flow group UUIDs in the org assets
	for _, group := range c.Groups().All() {
		dbGroup := oa.GroupByUUID(group.UUID())
		if dbGroup != nil {
			doc.GroupIDs = append(doc.GroupIDs, dbGroup.ID())
		}
	}

	return doc
}
