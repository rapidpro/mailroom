package contact

import (
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions/modifiers"

	"github.com/pkg/errors"
)

// a validated contact creation task
type creation struct {
	name     string
	language envs.Language
	urns     []urns.URN
	mods     []flows.Modifier
}

// describes a contact to be created
type contactSpec struct {
	Name     string             `json:"name"`
	Language string             `json:"language"`
	URNs     []urns.URN         `json:"urns"`
	Fields   map[string]string  `json:"fields"`
	Groups   []assets.GroupUUID `json:"groups"`
}

func (c *contactSpec) validate(env envs.Environment, sa flows.SessionAssets) (*creation, error) {
	country := string(env.DefaultCountry())
	var err error
	validated := &creation{name: c.Name}

	if c.Language != "" {
		validated.language, err = envs.ParseLanguage(c.Language)
		if err != nil {
			return nil, errors.Wrap(err, "invalid language")
		}
	}

	validated.urns = make([]urns.URN, len(c.URNs))
	for i, urn := range c.URNs {
		validated.urns[i] = urn.Normalize(country)
	}

	validated.mods = make([]flows.Modifier, 0, len(c.Fields))

	for key, value := range c.Fields {
		field := sa.Fields().Get(key)
		if field == nil {
			return nil, errors.Errorf("unknown contact field '%s'", key)
		}
		if value != "" {
			validated.mods = append(validated.mods, modifiers.NewField(field, value))
		}
	}

	if len(c.Groups) > 0 {
		groups := make([]*flows.Group, len(c.Groups))
		for i, uuid := range c.Groups {
			group := sa.Groups().Get(uuid)
			if group == nil {
				return nil, errors.Errorf("unknown contact group '%s'", uuid)
			}
			if group.IsDynamic() {
				return nil, errors.Errorf("can't add contact to dynamic group '%s'", uuid)
			}
			groups[i] = group
		}

		validated.mods = append(validated.mods, modifiers.NewGroups(groups, modifiers.GroupsAdd))
	}

	return validated, nil
}
