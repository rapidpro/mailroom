package goflow

import (
	"encoding/json"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/modifiers"

	"github.com/pkg/errors"
)

// MissingAssets is the type for defining missing assets behavior
type MissingAssets int

// missing assets constants
const (
	IgnoreMissing  MissingAssets = 0
	ErrorOnMissing MissingAssets = 1
)

// ReadModifiers reads modifiers from the given JSON
func ReadModifiers(sa flows.SessionAssets, data []json.RawMessage, missing MissingAssets) ([]flows.Modifier, error) {
	mods := make([]flows.Modifier, 0, len(data))
	for _, m := range data {
		mod, err := modifiers.ReadModifier(sa, m, assets.IgnoreMissing)

		// if this modifier turned into a no-op, ignore
		if err == modifiers.ErrNoModifier && missing == IgnoreMissing {
			continue
		}
		if err != nil {
			return nil, errors.Wrapf(err, "error reading modifier: %s", string(m))
		}
		mods = append(mods, mod)
	}
	return mods, nil
}
