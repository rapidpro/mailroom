package search

import (
	"fmt"
	"strings"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/utils/dates"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/nyaruka/goflow/utils/uuids"
)

// Field represents a field that elastic can search against
type Field interface {
	assets.Field
	UUID() uuids.UUID
}

// FieldRegistry provides an interface for looking up queryable fields
type FieldRegistry interface {
	LookupSearchField(key string) Field
}

// ToElasticQuery converts a contactql query to an Elastic query
func ToElasticQuery(env envs.Environment, registry FieldRegistry, node contactql.QueryNode) (elastic.Query, error) {
	switch n := node.(type) {
	case *contactql.ContactQuery:
		return ToElasticQuery(env, registry, n.Root())
	case *contactql.BoolCombination:
		return boolCombinationToElasticQuery(env, registry, n)
	case *contactql.Condition:
		return conditionToElasticQuery(env, registry, n)
	default:
		return nil, errors.Errorf("unknown type converting to elastic query: %v", n)
	}
}

func boolCombinationToElasticQuery(env envs.Environment, registry FieldRegistry, combination *contactql.BoolCombination) (elastic.Query, error) {
	queries := make([]elastic.Query, len(combination.Children()))
	for i, child := range combination.Children() {
		childQuery, err := ToElasticQuery(env, registry, child)
		if err != nil {
			return nil, errors.Wrapf(err, "error evaluating child query")
		}
		queries[i] = childQuery
	}

	if combination.Operator() == contactql.BoolOperatorAnd {
		return elastic.NewBoolQuery().Must(queries...), nil
	}

	return elastic.NewBoolQuery().Should(queries...), nil
}

func conditionToElasticQuery(env envs.Environment, registry FieldRegistry, c *contactql.Condition) (elastic.Query, error) {
	var query elastic.Query
	key := c.PropertyKey()

	if c.PropertyType() == contactql.PropertyTypeField {
		field := registry.LookupSearchField(key)
		if field == nil {
			return nil, errors.Errorf("unable to find field: %s", key)
		}

		fieldQuery := elastic.NewTermQuery("fields.field", field.UUID())
		fieldType := field.Type()

		// special cases for set/unset
		if (c.Comparator() == "=" || c.Comparator() == "!=") && c.Value() == "" {
			query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
				fieldQuery,
				elastic.NewExistsQuery("fields."+string(field.Type())),
			))

			// if we are looking for unset, inverse our query
			if c.Comparator() == "=" {
				query = elastic.NewBoolQuery().MustNot(query)
			}
			return query, nil
		}

		if fieldType == assets.FieldTypeText {
			value := strings.ToLower(c.Value())
			if c.Comparator() == "=" {
				query = elastic.NewTermQuery("fields.text", value)
			} else if c.Comparator() == "!=" {
				query = elastic.NewBoolQuery().Must(
					fieldQuery,
					elastic.NewTermQuery("fields.text", value),
					elastic.NewExistsQuery("fields.text"),
				)
				return elastic.NewBoolQuery().MustNot(elastic.NewNestedQuery("fields", query)), nil
			} else {
				return nil, fmt.Errorf("unsupported text comparator: %s", c.Comparator())
			}

			return elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(fieldQuery, query)), nil

		} else if fieldType == assets.FieldTypeNumber {
			value, err := decimal.NewFromString(c.Value())
			if err != nil {
				return nil, errors.Errorf("can't convert '%s' to a number", c.Value())
			}

			if c.Comparator() == "=" {
				query = elastic.NewMatchQuery("fields.number", value)
			} else if c.Comparator() == ">" {
				query = elastic.NewRangeQuery("fields.number").Gt(value)
			} else if c.Comparator() == ">=" {
				query = elastic.NewRangeQuery("fields.number").Gte(value)
			} else if c.Comparator() == "<" {
				query = elastic.NewRangeQuery("fields_number").Lt(value)
			} else if c.Comparator() == "<=" {
				query = elastic.NewRangeQuery("fields.number").Lte(value)
			} else {
				return nil, fmt.Errorf("unsupported number comparator: %s", c.Comparator())
			}

			return elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(fieldQuery, query)), nil

		} else if fieldType == assets.FieldTypeDatetime {
			value, err := envs.DateTimeFromString(env, c.Value(), false)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to parse datetime: %s", c.Value())
			}
			start, end := dates.DayToUTCRange(value, value.Location())

			if c.Comparator() == "=" {
				query = elastic.NewRangeQuery("fields.datetime").Gte(start).Lt(end)
			} else if c.Comparator() == ">" {
				query = elastic.NewRangeQuery("fields.datetime").Gte(end)
			} else if c.Comparator() == ">=" {
				query = elastic.NewRangeQuery("fields.datetime").Gte(start)
			} else if c.Comparator() == "<" {
				query = elastic.NewRangeQuery("fields.datetime").Lt(start)
			} else if c.Comparator() == "<=" {
				query = elastic.NewRangeQuery("fields.datetime").Lt(end)
			} else {
				return nil, fmt.Errorf("unsupported datetime comparator: %s", c.Comparator())
			}

			return elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(fieldQuery, query)), nil

		} else if fieldType == assets.FieldTypeState || fieldType == assets.FieldTypeDistrict || fieldType == assets.FieldTypeWard {
			value := strings.ToLower(c.Value())
			var name = fmt.Sprintf("fields.%s_keyword", string(fieldType))

			if c.Comparator() == "=" {
				query = elastic.NewTermQuery(name, value)
			} else if c.Comparator() == "!=" {
				return elastic.NewBoolQuery().MustNot(
					elastic.NewNestedQuery("fields",
						elastic.NewBoolQuery().Must(
							elastic.NewTermQuery(name, value),
							elastic.NewExistsQuery(name),
						),
					),
				), nil
			} else {
				return nil, fmt.Errorf("unsupported location comparator: %s", c.Comparator())
			}

			return elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(fieldQuery, query)), nil
		} else {
			return nil, fmt.Errorf("unsupported contact field type: %s", field.Type())
		}
	} else if c.PropertyType() == contactql.PropertyTypeAttribute {
		value := strings.ToLower(c.Value())

		// special case for set/unset for name and language
		if (c.Comparator() == "=" || c.Comparator() == "!=") && value == "" &&
			(key == contactql.AttributeName || key == contactql.AttributeLanguage) {

			query = elastic.NewBoolQuery().Must(
				elastic.NewExistsQuery(key),
				elastic.NewBoolQuery().MustNot(elastic.NewTermQuery(fmt.Sprintf("%s.keyword", key), "")),
			)

			if c.Comparator() == "=" {
				query = elastic.NewBoolQuery().MustNot(query)
			}

			return query, nil
		}

		if key == contactql.AttributeName {
			if c.Comparator() == "=" {
				return elastic.NewTermQuery("name.keyword", c.Value()), nil
			} else if c.Comparator() == "~" {
				return elastic.NewMatchQuery("name", value), nil
			} else if c.Comparator() == "!=" {
				return elastic.NewBoolQuery().MustNot(elastic.NewTermQuery("name.keyword", c.Value())), nil
			} else {
				return nil, fmt.Errorf("unsupported name query comparator: %s", c.Comparator())
			}
		} else if key == contactql.AttributeID {
			if c.Comparator() == "=" {
				return elastic.NewIdsQuery().Ids(value), nil
			}
			return nil, fmt.Errorf("unsupported comparator for id: %s", c.Comparator())
		} else if key == contactql.AttributeLanguage {
			if c.Comparator() == "=" {
				return elastic.NewTermQuery("language", value), nil
			} else if c.Comparator() == "!=" {
				return elastic.NewBoolQuery().MustNot(elastic.NewTermQuery("language", value)), nil
			} else {
				return nil, fmt.Errorf("unsupported language comparator: %s", c.Comparator())
			}
		} else if key == contactql.AttributeCreatedOn {
			value, err := envs.DateTimeFromString(env, c.Value(), false)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to parse datetime: %s", c.Value())
			}
			start, end := dates.DayToUTCRange(value, value.Location())

			if c.Comparator() == "=" {
				return elastic.NewRangeQuery("created_on").Gte(start).Lt(end), nil
			} else if c.Comparator() == ">" {
				return elastic.NewRangeQuery("created_on").Gte(end), nil
			} else if c.Comparator() == ">=" {
				return elastic.NewRangeQuery("created_on").Gte(start), nil
			} else if c.Comparator() == "<" {
				return elastic.NewRangeQuery("created_on").Lt(start), nil
			} else if c.Comparator() == "<=" {
				return elastic.NewRangeQuery("created_on").Lt(end), nil
			} else {
				return nil, fmt.Errorf("unsupported created_on comparator: %s", c.Comparator())
			}
		} else {
			return nil, fmt.Errorf("unsupported contact attribute: %s", key)
		}
	} else if c.PropertyType() == contactql.PropertyTypeScheme {
		value := strings.ToLower(c.Value())

		// special case for set/unset
		if (c.Comparator() == "=" || c.Comparator() == "!=") && value == "" {
			query = elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("urns.scheme", key),
				elastic.NewExistsQuery("urns.path"),
			))
			if c.Comparator() == "=" {
				query = elastic.NewBoolQuery().MustNot(query)
			}
			return query, nil
		}

		if c.Comparator() == "=" {
			return elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("urns.path.keyword", value),
				elastic.NewTermQuery("urns.scheme", key)),
			), nil
		} else if c.Comparator() == "~" {
			return elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
				elastic.NewMatchPhraseQuery("urns.path", value),
				elastic.NewTermQuery("urns.scheme", key)),
			), nil
		} else {
			return nil, fmt.Errorf("unsupported scheme comparator: %s", c.Comparator())
		}
	}

	return nil, errors.Errorf("unsupported property type: %s", c.PropertyType())
}
