package contact

import (
	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/modifiers"
	"github.com/nyaruka/mailroom/core/models"

	"github.com/pkg/errors"
)

// Creation a validated contact creation task
type Creation struct {
	Name     string
	Language envs.Language
	URNs     []urns.URN
	Mods     []flows.Modifier
}

// SpecToCreation validates that the spec is valid for the given assets
func SpecToCreation(s *models.ContactSpec, env envs.Environment, sa flows.SessionAssets) (*Creation, error) {
	country := string(env.DefaultCountry())
	var err error
	validated := &Creation{}

	if s.Name != nil {
		validated.Name = *s.Name
	}

	if s.Language != nil && *s.Language != "" {
		validated.Language, err = envs.ParseLanguage(*s.Language)
		if err != nil {
			return nil, errors.Wrap(err, "invalid language")
		}
	}

	validated.URNs = make([]urns.URN, len(s.URNs))
	for i, urn := range s.URNs {
		validated.URNs[i] = urn.Normalize(country)
	}

	validated.Mods = make([]flows.Modifier, 0, len(s.Fields))

	for key, value := range s.Fields {
		field := sa.Fields().Get(key)
		if field == nil {
			return nil, errors.Errorf("unknown contact field '%s'", key)
		}
		if value != "" {
			validated.Mods = append(validated.Mods, modifiers.NewField(field, value))
		}
	}

	if len(s.Groups) > 0 {
		groups := make([]*flows.Group, len(s.Groups))
		for i, uuid := range s.Groups {
			group := sa.Groups().Get(uuid)
			if group == nil {
				return nil, errors.Errorf("unknown contact group '%s'", uuid)
			}
			if group.UsesQuery() {
				return nil, errors.Errorf("can't add contact to query based group '%s'", uuid)
			}
			groups[i] = group
		}

		validated.Mods = append(validated.Mods, modifiers.NewGroups(groups, modifiers.GroupsAdd))
	}

	return validated, nil
}
