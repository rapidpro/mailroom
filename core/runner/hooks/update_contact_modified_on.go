package hooks

import (
	"context"
	"fmt"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/vinovest/sqlx"
)

// UpdateContactModifiedOn is our hook for contact changes that require an update to modified_on
var UpdateContactModifiedOn runner.PreCommitHook = &updateContactModifiedOn{}

type updateContactModifiedOn struct{}

func (h *updateContactModifiedOn) Order() int { return 100 } // run after all other hooks

func (h *updateContactModifiedOn) Execute(ctx context.Context, rt *runtime.Runtime, tx *sqlx.Tx, oa *models.OrgAssets, scenes map[*runner.Scene][]any) error {
	contactIDs := make([]models.ContactID, 0, len(scenes))

	for scene := range scenes {
		contactIDs = append(contactIDs, scene.ContactID())
	}

	if err := models.UpdateContactModifiedOn(ctx, tx, contactIDs); err != nil {
		return fmt.Errorf("error updating modified_on on contacts: %w", err)
	}

	return nil
}
