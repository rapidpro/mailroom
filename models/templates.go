package models

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Template struct {
	t struct {
		Name         string                 `json:"name"          validate:"required"`
		UUID         utils.UUID             `json:"uuid"          validate:"required"`
		Translations []*TemplateTranslation `json:"translations"`
	}
}

func (t *Template) Name() string     { return t.t.Name }
func (t *Template) UUID() utils.UUID { return t.t.UUID }
func (t *Template) Translation() []*TemplateTranslation {
	trs := make([]*TemplateTranslation, len(t.t.Translations))
	for i := range trs {
		trs[i] = t.t.Translations[i]
	}
	return t.t.Translations
}

// UnmarshalJSON is our unmarshaller for json data
func (t *Template) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, &t.t) }

// MarshalJSON is our marshaller for json data
func (t *Template) MarshalJSON() ([]byte, error) { return json.Marshal(t.t) }

type TemplateTranslation struct {
	t struct {
		Channel  assets.ChannelReference `json:"channel"  validate:"required"`
		Language utils.Language          `json:"language" validate:"required"`
		Content  string                  `json:"content"  validate"required"`
	}
}

// UnmarshalJSON is our unmarshaller for json data
func (t *TemplateTranslation) UnmarshalJSON(data []byte) error { return json.Unmarshal(data, &t.t) }

// MarshalJSON is our marshaller for json data
func (t *TemplateTranslation) MarshalJSON() ([]byte, error) { return json.Marshal(t.t) }

func (t *TemplateTranslation) Channel() assets.ChannelReference { return t.t.Channel }
func (t *TemplateTranslation) Language() utils.Language         { return t.t.Language }
func (t *TemplateTranslation) Content() string                  { return t.t.Content }

// loads the templates for the passed in org
func loadTemplates(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]*Template, error) {
	start := time.Now()

	rows, err := db.Queryx(selectTemplatesSQL, orgID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying templates for org: %d", orgID)
	}
	defer rows.Close()

	templates := make([]*Template, 0)
	for rows.Next() {
		template := &Template{}
		err = readJSONRow(rows, &template.t)
		if err != nil {
			return nil, errors.Wrap(err, "error reading group row")
		}

		templates = append(templates, template)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", orgID).WithField("count", len(templates)).Debug("loaded templates")

	return templates, nil
}

const selectTemplatesSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
	t.name as name, 
	t.uuid as uuid, 
	(SELECT ROW_TO_JSON(tr) FROM (
		SELECT
			tr.language as language,
			tr.content as content,
			JSON_BUILD_OBJECT('uuid', c.uuid, 'name', c.name) as channel
		FROM
			templates_templatetranslation tr
			JOIN channels_channel c ON tr.channel_id = c.id
		WHERE 
			tr.is_active = TRUE AND
			tr.template_id = t.id
	)) as translations
FROM 
	templates_template t
WHERE 
	org_id = $1 AND 
	is_active = TRUE
ORDER BY 
	name ASC
) r;
`
