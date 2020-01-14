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
)

// ToElasticQuery converts a contactql query string to an Elastic query returning the normalized view as well as the elastic query
func ToElasticQuery(env envs.Environment, resolver contactql.FieldResolverFunc, query string) (string, elastic.Query, error) {
	node, err := contactql.ParseQuery(query, env.RedactionPolicy(), resolver)
	if err != nil {
		return "", nil, errors.Wrapf(err, "error parsing query: %s", query)
	}

	eq, err := nodeToElasticQuery(env, resolver, node.Root())
	if err != nil {
		return "", nil, errors.Wrapf(err, "error parsing query: %s", query)
	}

	return node.String(), eq, nil
}

// ToElasticFieldSort returns the FieldSort for the passed in field
func ToElasticFieldSort(resolver contactql.FieldResolverFunc, fieldName string) (*elastic.FieldSort, error) {
	// no field name? default to most recent first by id
	if fieldName == "" {
		return elastic.NewFieldSort("id").Desc(), nil
	}

	// figure out if we are ascending or descending (default is ascending, can be changed with leading -)
	ascending := true
	if strings.HasPrefix(fieldName, "-") {
		ascending = false
		fieldName = fieldName[1:]
	}

	fieldName = strings.ToLower(fieldName)

	// we are sorting by an attribute
	if fieldName == contactql.AttributeID || fieldName == contactql.AttributeCreatedOn ||
		fieldName == contactql.AttributeLanguage || fieldName == contactql.AttributeName {
		return elastic.NewFieldSort(fieldName).Order(ascending), nil
	}

	// we are sorting by a custom field
	field := resolver(fieldName)
	if field == nil {
		return nil, errors.Errorf("unable to find field with name: %s", fieldName)
	}

	sort := elastic.NewFieldSort(fmt.Sprintf("fields.%s", field.Type()))
	sort = sort.Nested(elastic.NewNestedSort("fields").Filter(elastic.NewTermQuery("fields.field", field.UUID())))
	sort = sort.Order(ascending)
	return sort, nil
}

func nodeToElasticQuery(env envs.Environment, resolver contactql.FieldResolverFunc, node contactql.QueryNode) (elastic.Query, error) {
	switch n := node.(type) {
	case *contactql.BoolCombination:
		return boolCombinationToElasticQuery(env, resolver, n)
	case *contactql.Condition:
		return conditionToElasticQuery(env, resolver, n)
	default:
		return nil, errors.Errorf("unknown type converting to elastic query: %v", n)
	}
}

func boolCombinationToElasticQuery(env envs.Environment, resolver contactql.FieldResolverFunc, combination *contactql.BoolCombination) (elastic.Query, error) {
	queries := make([]elastic.Query, len(combination.Children()))
	for i, child := range combination.Children() {
		childQuery, err := nodeToElasticQuery(env, resolver, child)
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

func conditionToElasticQuery(env envs.Environment, resolver contactql.FieldResolverFunc, c *contactql.Condition) (elastic.Query, error) {
	var query elastic.Query
	key := c.PropertyKey()

	if c.PropertyType() == contactql.PropertyTypeField {
		field := resolver(key)
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
