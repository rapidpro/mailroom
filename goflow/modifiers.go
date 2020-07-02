package goflow

import (
	"encoding/json"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions/modifiers"

	"github.com/pkg/errors"
)

// ReadModifiers reads modifiers from the given JSON
func ReadModifiers(sa flows.SessionAssets, data []json.RawMessage, allowMissing bool) ([]flows.Modifier, error) {
	mods := make([]flows.Modifier, 0, len(data))
	for _, m := range data {
		mod, err := modifiers.ReadModifier(sa, m, assets.IgnoreMissing)

		// if this modifier turned into a no-op, ignore
		if err == modifiers.ErrNoModifier && allowMissing {
			continue
		}
		if err != nil {
			return nil, errors.Wrapf(err, "error reading modifier: %s", string(m))
		}
		mods = append(mods, mod)
	}
	return mods, nil
}
