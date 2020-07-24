package contact

import (
	"context"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions/modifiers"
	"github.com/nyaruka/mailroom/models"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// Creation a validated contact creation task
type Creation struct {
	Name     string
	Language envs.Language
	URNs     []urns.URN
	Mods     []flows.Modifier
}

// Spec describes a contact to be created
type Spec struct {
	Name     string             `json:"name"`
	Language string             `json:"language"`
	URNs     []urns.URN         `json:"urns"`
	Fields   map[string]string  `json:"fields"`
	Groups   []assets.GroupUUID `json:"groups"`
}

// Validate validates that the spec is valid for the given assets
func (s *Spec) Validate(env envs.Environment, sa flows.SessionAssets) (*Creation, error) {
	country := string(env.DefaultCountry())
	var err error
	validated := &Creation{Name: s.Name}

	if s.Language != "" {
		validated.Language, err = envs.ParseLanguage(s.Language)
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
			if group.IsDynamic() {
				return nil, errors.Errorf("can't add contact to dynamic group '%s'", uuid)
			}
			groups[i] = group
		}

		validated.Mods = append(validated.Mods, modifiers.NewGroups(groups, modifiers.GroupsAdd))
	}

	return validated, nil
}

// ModifyContacts modifies contacts by applying modifiers and handling the resultant events
func ModifyContacts(ctx context.Context, db *sqlx.DB, rp *redis.Pool, oa *models.OrgAssets, modifiersByContact map[*flows.Contact][]flows.Modifier) (map[*flows.Contact][]flows.Event, error) {
	// create an environment instance with location support
	env := flows.NewEnvironment(oa.Env(), oa.SessionAssets().Locations())

	eventsByContact := make(map[*flows.Contact][]flows.Event)

	// apply the modifiers to get the events for each contact
	for contact, mods := range modifiersByContact {
		events := make([]flows.Event, 0)
		for _, mod := range mods {
			mod.Apply(env, oa.SessionAssets(), contact, func(e flows.Event) { events = append(events, e) })
		}
		eventsByContact[contact] = events
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting transaction")
	}

	scenes := make([]*models.Scene, 0, len(modifiersByContact))

	for contact := range modifiersByContact {
		scene := models.NewSceneForContact(contact)
		scenes = append(scenes, scene)

		err := models.HandleEvents(ctx, tx, rp, oa, scene, eventsByContact[contact])
		if err != nil {
			return nil, errors.Wrapf(err, "error handling events")
		}
	}

	// gather all our pre commit events, group them by hook and apply them
	err = models.ApplyEventPreCommitHooks(ctx, tx, rp, oa, scenes)
	if err != nil {
		return nil, errors.Wrapf(err, "error applying pre commit hooks")
	}

	// commit our transaction
	if err := tx.Commit(); err != nil {
		return nil, errors.Wrapf(err, "error committing transaction")
	}

	// start new transaction for post commit hooks
	tx, err = db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "error starting transaction for post commit")
	}

	// then apply our post commit hooks
	err = models.ApplyEventPostCommitHooks(ctx, tx, rp, oa, scenes)
	if err != nil {
		return nil, errors.Wrapf(err, "error applying post commit hooks")
	}

	if err := tx.Commit(); err != nil {
		return nil, errors.Wrapf(err, "error committing post commit hooks")
	}

	return eventsByContact, nil
}
