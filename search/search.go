package search

import (
	"fmt"
	"sort"
	"strings"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/envs"
	"github.com/nyaruka/goflow/utils"
	"github.com/nyaruka/goflow/utils/dates"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
)

// ParseQuery parses the passed in query returning the result
func ParseQuery(env envs.Environment, resolver contactql.Resolver, query string) (*contactql.ContactQuery, error) {
	parsed, err := contactql.ParseQuery(query, env.RedactionPolicy(), env.DefaultCountry(), resolver)
	if err != nil {
		return nil, NewError(err.Error())
	}
	return parsed, nil
}

// ToElasticQuery converts a contactql query to an Elastic query returning the normalized view as well as the elastic query
func ToElasticQuery(env envs.Environment, resolver contactql.Resolver, query *contactql.ContactQuery) (elastic.Query, error) {
	eq, err := nodeToElasticQuery(env, resolver, query.Root())
	if err != nil {
		return nil, NewError(err.Error())
	}

	return eq, nil
}

// FieldDependencies returns all the field this query is dependent on. This includes attributes such as "id" and "name"
func FieldDependencies(query *contactql.ContactQuery) []string {
	if query == nil {
		return []string{}
	}

	seen := make(map[string]bool)
	var appendFields func(node contactql.QueryNode, seen map[string]bool)
	appendFields = func(node contactql.QueryNode, seen map[string]bool) {
		switch n := node.(type) {
		case *contactql.BoolCombination:
			for _, c := range n.Children() {
				appendFields(c, seen)
			}

		case *contactql.Condition:
			seen[n.PropertyKey()] = true

		default:
			panic(fmt.Sprintf("unknown type in contactql query: %v", n))
		}
	}

	appendFields(query.Root(), seen)
	fields := make([]string, 0, len(seen))
	for k := range seen {
		fields = append(fields, k)
	}

	// order to make deterministic
	sort.Strings(fields)

	return fields
}

// ToElasticFieldSort returns the FieldSort for the passed in field
func ToElasticFieldSort(resolver contactql.Resolver, fieldName string) (*elastic.FieldSort, error) {
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

	// name needs to be sorted by keyword field
	if fieldName == contactql.AttributeName {
		return elastic.NewFieldSort("name.keyword").Order(ascending), nil
	}

	// other attributes are straight sorts
	if fieldName == contactql.AttributeID || fieldName == contactql.AttributeCreatedOn || fieldName == contactql.AttributeLanguage {
		return elastic.NewFieldSort(fieldName).Order(ascending), nil
	}

	// we are sorting by a custom field
	field := resolver.ResolveField(fieldName)
	if field == nil {
		return nil, NewError("unable to find field with name: %s", fieldName)
	}

	var key string
	switch field.Type() {
	case assets.FieldTypeState,
		assets.FieldTypeDistrict,
		assets.FieldTypeWard:
		key = fmt.Sprintf("fields.%s_keyword", field.Type())
	default:
		key = fmt.Sprintf("fields.%s", field.Type())
	}

	sort := elastic.NewFieldSort(key)
	sort = sort.Nested(elastic.NewNestedSort("fields").Filter(elastic.NewTermQuery("fields.field", field.UUID())))
	sort = sort.Order(ascending)
	return sort, nil
}

// AllowAsGroup returns whether a query can be used as a dynamic group
func AllowAsGroup(fields []string) bool {
	return !(utils.StringSliceContains(fields, "id", false) || utils.StringSliceContains(fields, "group", false))
}

func nodeToElasticQuery(env envs.Environment, resolver contactql.Resolver, node contactql.QueryNode) (elastic.Query, error) {
	switch n := node.(type) {
	case *contactql.BoolCombination:
		return boolCombinationToElasticQuery(env, resolver, n)
	case *contactql.Condition:
		return conditionToElasticQuery(env, resolver, n)
	default:
		return nil, errors.Errorf("unknown type converting to elastic query: %v", n)
	}
}

func boolCombinationToElasticQuery(env envs.Environment, resolver contactql.Resolver, combination *contactql.BoolCombination) (elastic.Query, error) {
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

func conditionToElasticQuery(env envs.Environment, resolver contactql.Resolver, c *contactql.Condition) (elastic.Query, error) {
	var query elastic.Query
	key := c.PropertyKey()

	if c.PropertyType() == contactql.PropertyTypeField {
		field := resolver.ResolveField(key)
		if field == nil {
			return nil, NewError("unable to find field: %s", key)
		}

		fieldQuery := elastic.NewTermQuery("fields.field", field.UUID())
		fieldType := field.Type()

		// special cases for set/unset
		if (c.Comparator() == contactql.ComparatorEqual || c.Comparator() == contactql.ComparatorNotEqual) && c.Value() == "" {
			query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
				fieldQuery,
				elastic.NewExistsQuery("fields."+string(field.Type())),
			))

			// if we are looking for unset, inverse our query
			if c.Comparator() == contactql.ComparatorEqual {
				query = elastic.NewBoolQuery().MustNot(query)
			}
			return query, nil
		}

		if fieldType == assets.FieldTypeText {
			value := strings.ToLower(c.Value())
			if c.Comparator() == contactql.ComparatorEqual {
				query = elastic.NewTermQuery("fields.text", value)
			} else if c.Comparator() == contactql.ComparatorNotEqual {
				query = elastic.NewBoolQuery().Must(
					fieldQuery,
					elastic.NewTermQuery("fields.text", value),
					elastic.NewExistsQuery("fields.text"),
				)
				return elastic.NewBoolQuery().MustNot(elastic.NewNestedQuery("fields", query)), nil
			} else {
				return nil, NewError("unsupported text comparator: %s", c.Comparator())
			}

			return elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(fieldQuery, query)), nil

		} else if fieldType == assets.FieldTypeNumber {
			value, err := decimal.NewFromString(c.Value())
			if err != nil {
				return nil, NewError("can't convert '%s' to a number", c.Value())
			}

			if c.Comparator() == contactql.ComparatorEqual {
				query = elastic.NewMatchQuery("fields.number", value)
			} else if c.Comparator() == contactql.ComparatorNotEqual {
				return elastic.NewBoolQuery().MustNot(
					elastic.NewNestedQuery("fields",
						elastic.NewBoolQuery().Must(
							fieldQuery,
							elastic.NewMatchQuery("fields.number", value),
						),
					),
				), nil
			} else if c.Comparator() == contactql.ComparatorGreaterThan {
				query = elastic.NewRangeQuery("fields.number").Gt(value)
			} else if c.Comparator() == contactql.ComparatorGreaterThanOrEqual {
				query = elastic.NewRangeQuery("fields.number").Gte(value)
			} else if c.Comparator() == contactql.ComparatorLessThan {
				query = elastic.NewRangeQuery("fields.number").Lt(value)
			} else if c.Comparator() == contactql.ComparatorLessThanOrEqual {
				query = elastic.NewRangeQuery("fields.number").Lte(value)
			} else {
				return nil, NewError("unsupported number comparator: %s", c.Comparator())
			}

			return elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(fieldQuery, query)), nil

		} else if fieldType == assets.FieldTypeDatetime {
			value, err := envs.DateTimeFromString(env, c.Value(), false)
			if err != nil {
				return nil, NewError("string '%s' couldn't be parsed as a date", c.Value())
			}
			start, end := dates.DayToUTCRange(value, value.Location())

			if c.Comparator() == contactql.ComparatorEqual {
				query = elastic.NewRangeQuery("fields.datetime").Gte(start).Lt(end)
			} else if c.Comparator() == contactql.ComparatorNotEqual {
				return elastic.NewBoolQuery().MustNot(
					elastic.NewNestedQuery("fields",
						elastic.NewBoolQuery().Must(
							fieldQuery,
							elastic.NewRangeQuery("fields.datetime").Gte(start).Lt(end),
						),
					),
				), nil
			} else if c.Comparator() == contactql.ComparatorGreaterThan {
				query = elastic.NewRangeQuery("fields.datetime").Gte(end)
			} else if c.Comparator() == contactql.ComparatorGreaterThanOrEqual {
				query = elastic.NewRangeQuery("fields.datetime").Gte(start)
			} else if c.Comparator() == contactql.ComparatorLessThan {
				query = elastic.NewRangeQuery("fields.datetime").Lt(start)
			} else if c.Comparator() == contactql.ComparatorLessThanOrEqual {
				query = elastic.NewRangeQuery("fields.datetime").Lt(end)
			} else {
				return nil, NewError("unsupported datetime comparator: %s", c.Comparator())
			}

			return elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(fieldQuery, query)), nil

		} else if fieldType == assets.FieldTypeState || fieldType == assets.FieldTypeDistrict || fieldType == assets.FieldTypeWard {
			value := strings.ToLower(c.Value())
			var name = fmt.Sprintf("fields.%s_keyword", string(fieldType))

			if c.Comparator() == contactql.ComparatorEqual {
				query = elastic.NewTermQuery(name, value)
			} else if c.Comparator() == contactql.ComparatorNotEqual {
				return elastic.NewBoolQuery().MustNot(
					elastic.NewNestedQuery("fields",
						elastic.NewBoolQuery().Must(
							elastic.NewTermQuery(name, value),
							elastic.NewExistsQuery(name),
						),
					),
				), nil
			} else {
				return nil, NewError("unsupported location comparator: %s", c.Comparator())
			}

			return elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(fieldQuery, query)), nil
		} else {
			return nil, NewError("unsupported contact field type: %s", field.Type())
		}
	} else if c.PropertyType() == contactql.PropertyTypeAttribute {
		value := strings.ToLower(c.Value())

		// special case for set/unset for name and language
		if (c.Comparator() == contactql.ComparatorEqual || c.Comparator() == contactql.ComparatorNotEqual) && value == "" &&
			(key == contactql.AttributeName || key == contactql.AttributeLanguage) {

			query = elastic.NewBoolQuery().Must(
				elastic.NewExistsQuery(key),
				elastic.NewBoolQuery().MustNot(elastic.NewTermQuery(fmt.Sprintf("%s.keyword", key), "")),
			)

			if c.Comparator() == contactql.ComparatorEqual {
				query = elastic.NewBoolQuery().MustNot(query)
			}

			return query, nil
		}

		if key == contactql.AttributeName {
			if c.Comparator() == contactql.ComparatorEqual {
				return elastic.NewTermQuery("name.keyword", c.Value()), nil
			} else if c.Comparator() == contactql.ComparatorContains {
				return elastic.NewMatchQuery("name", value), nil
			} else if c.Comparator() == contactql.ComparatorNotEqual {
				return elastic.NewBoolQuery().MustNot(elastic.NewTermQuery("name.keyword", c.Value())), nil
			} else {
				return nil, NewError("unsupported name query comparator: %s", c.Comparator())
			}
		} else if key == contactql.AttributeID {
			if c.Comparator() == contactql.ComparatorEqual {
				return elastic.NewIdsQuery().Ids(value), nil
			}
			return nil, NewError("unsupported comparator for id: %s", c.Comparator())
		} else if key == contactql.AttributeLanguage {
			if c.Comparator() == contactql.ComparatorEqual {
				return elastic.NewTermQuery("language", value), nil
			} else if c.Comparator() == contactql.ComparatorNotEqual {
				return elastic.NewBoolQuery().MustNot(elastic.NewTermQuery("language", value)), nil
			} else {
				return nil, NewError("unsupported language comparator: %s", c.Comparator())
			}
		} else if key == contactql.AttributeCreatedOn {
			value, err := envs.DateTimeFromString(env, c.Value(), false)
			if err != nil {
				return nil, NewError("string '%s' couldn't be parsed as a date", c.Value())
			}
			start, end := dates.DayToUTCRange(value, value.Location())

			if c.Comparator() == contactql.ComparatorEqual {
				return elastic.NewRangeQuery("created_on").Gte(start).Lt(end), nil
			} else if c.Comparator() == contactql.ComparatorNotEqual {
				return elastic.NewBoolQuery().MustNot(elastic.NewRangeQuery("created_on").Gte(start).Lt(end)), nil
			} else if c.Comparator() == contactql.ComparatorGreaterThan {
				return elastic.NewRangeQuery("created_on").Gte(end), nil
			} else if c.Comparator() == contactql.ComparatorGreaterThanOrEqual {
				return elastic.NewRangeQuery("created_on").Gte(start), nil
			} else if c.Comparator() == contactql.ComparatorLessThan {
				return elastic.NewRangeQuery("created_on").Lt(start), nil
			} else if c.Comparator() == contactql.ComparatorLessThanOrEqual {
				return elastic.NewRangeQuery("created_on").Lt(end), nil
			} else {
				return nil, NewError("unsupported created_on comparator: %s", c.Comparator())
			}
		} else if key == contactql.AttributeURN {
			value := strings.ToLower(c.Value())

			// special case for set/unset
			if (c.Comparator() == contactql.ComparatorEqual || c.Comparator() == contactql.ComparatorNotEqual) && value == "" {
				query = elastic.NewNestedQuery("urns", elastic.NewExistsQuery("urns.path"))
				if c.Comparator() == contactql.ComparatorEqual {
					query = elastic.NewBoolQuery().MustNot(query)
				}
				return query, nil
			}

			if c.Comparator() == contactql.ComparatorEqual {
				return elastic.NewNestedQuery("urns", elastic.NewTermQuery("urns.path.keyword", value)), nil
			} else if c.Comparator() == contactql.ComparatorContains {
				return elastic.NewNestedQuery("urns", elastic.NewMatchPhraseQuery("urns.path", value)), nil
			} else {
				return nil, NewError("unsupported urn comparator: %s", c.Comparator())
			}

		} else if key == contactql.AttributeGroup {
			if c.Value() == "" {
				return nil, NewError("empty values not supported for group conditions")
			}

			group := resolver.ResolveGroup(c.Value())
			if group == nil {
				return nil, NewError("no such group with name '%s", c.Value())
			}

			if c.Comparator() == contactql.ComparatorEqual {
				return elastic.NewTermQuery("groups", group.UUID()), nil
			} else if c.Comparator() == contactql.ComparatorNotEqual {
				return elastic.NewBoolQuery().MustNot(elastic.NewTermQuery("groups", group.UUID())), nil
			} else {
				return nil, NewError("unsupported group comparator: %s", c.Comparator())
			}

		} else {
			return nil, NewError("unsupported contact attribute: %s", key)
		}
	} else if c.PropertyType() == contactql.PropertyTypeScheme {
		value := strings.ToLower(c.Value())

		// special case for set/unset
		if (c.Comparator() == contactql.ComparatorEqual || c.Comparator() == contactql.ComparatorNotEqual) && value == "" {
			query = elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("urns.scheme", key),
				elastic.NewExistsQuery("urns.path"),
			))
			if c.Comparator() == contactql.ComparatorEqual {
				query = elastic.NewBoolQuery().MustNot(query)
			}
			return query, nil
		}

		if c.Comparator() == contactql.ComparatorEqual {
			return elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("urns.path.keyword", value),
				elastic.NewTermQuery("urns.scheme", key)),
			), nil
		} else if c.Comparator() == contactql.ComparatorContains {
			return elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
				elastic.NewMatchPhraseQuery("urns.path", value),
				elastic.NewTermQuery("urns.scheme", key)),
			), nil
		} else {
			return nil, NewError("unsupported scheme comparator: %s", c.Comparator())
		}
	}

	return nil, NewError("unsupported property type: %s", c.PropertyType())
}

// Error is used when an error is in the parsing of a field or query format
type Error struct {
	error string
}

func (e *Error) Error() string {
	return e.error
}

func NewError(err string, args ...interface{}) *Error {
	return &Error{fmt.Sprintf(err, args...)}
}
