package search

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/utils"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
)

// FieldCategory is our type for the category of field this is
type FieldCategory string

// ContactAttribute is a system attribute of the contact, such as id, name or language
const ContactAttribute = FieldCategory("attribute")

// ContactField is a user created field on a contact
const ContactField = FieldCategory("field")

// Scheme is a URN scheme
const Scheme = FieldCategory("scheme")

// Unavailable means this field is not available for querying, such as schemes on anon orgs
const Unavailable = FieldCategory("unavailable")

// Implicit is used when no explicit query is asked for
const Implicit = FieldCategory("implicit")

// FieldType represents the type of the data for an elastic field
type FieldType string

// Text is our FieldType for text fields
const Text = FieldType("text")

// Number is our FieldType for number fields
const Number = FieldType("number")

// DateTime is our FieldType for date time fields
const DateTime = FieldType("datetime")

// State is our FieldType for state fields
const State = FieldType("state")

// District is our FieldType for district fields
const District = FieldType("district")

// Ward is our field type for ward fields
const Ward = FieldType("ward")

// NameTel is our key for name_tel implicit queries
const NameTel = "name_tel"

// NameID is our key for name_id implicit queries
const NameID = "name_id"

// NameAttribute is our key for name queries
const NameAttribute = "name"

// CreatedOnAttribute is our key for created_on queries
const CreatedOnAttribute = "created_on"

// LanguageAttribute is our key for language queries
const LanguageAttribute = "language"

// IDAttribute is our key for id queries
const IDAttribute = "id"

var contactAttributes = map[string]bool{
	NameAttribute:      true,
	CreatedOnAttribute: true,
	LanguageAttribute:  true,
	IDAttribute:        true,
}

// IsContactAttribute returns whether the passed in field is a contact attribute
func IsContactAttribute(field string) bool {
	return contactAttributes[field]
}

// Field represents a field that elastic can search against
type Field struct {
	Key      string
	Category FieldCategory
	Type     FieldType
	UUID     utils.UUID
}

// FieldRegistry provides an interface for looking up queryable fields
type FieldRegistry interface {
	LookupSearchField(key string) *Field
}

// ToElasticQuery converts a contactql query to an Elastic query
func ToElasticQuery(env utils.Environment, registry FieldRegistry, node contactql.QueryNode) (elastic.Query, error) {
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

func boolCombinationToElasticQuery(env utils.Environment, registry FieldRegistry, combination *contactql.BoolCombination) (elastic.Query, error) {
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

func conditionToElasticQuery(env utils.Environment, registry FieldRegistry, c *contactql.Condition) (elastic.Query, error) {
	field := registry.LookupSearchField(c.Key())
	if field == nil {
		return nil, errors.Errorf("unable to find field: %s", c.Key())
	}

	var query elastic.Query

	if field.Category == Implicit {
		number, _ := strconv.Atoi(c.Value())

		if field.Key == NameTel {
			if number != 0 {
				return elastic.NewNestedQuery("urns", elastic.NewMatchPhraseQuery("urns.path", number)), nil
			}
			return elastic.NewMatchQuery("name", c.Value()), nil

		} else if field.Key == NameID {
			if number != 0 {
				return elastic.NewIdsQuery().Ids(c.Value()), nil
			}
			return elastic.NewMatchQuery("name", c.Value()), nil
		} else {
			return nil, fmt.Errorf("unknown implicit field key: %s", field.Key)
		}
	} else if field.Category == ContactField {
		fieldQuery := elastic.NewTermQuery("fields.field", field.UUID)

		// special cases for set/unset
		if (c.Comparator() == "=" || c.Comparator() == "!=") && c.Value() == "" {
			query = elastic.NewNestedQuery("fields", elastic.NewBoolQuery().Must(
				fieldQuery,
				elastic.NewExistsQuery("fields."+string(field.Type)),
			))

			// if we are looking for unset, inverse our query
			if c.Comparator() == "=" {
				query = elastic.NewBoolQuery().MustNot(query)
			}
			return query, nil
		}

		if field.Type == Text {
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

		} else if field.Type == Number {
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

		} else if field.Type == DateTime {
			value, err := utils.DateTimeFromString(env, c.Value(), false)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to parse datetime: %s", c.Value())
			}
			start, end := utils.DateToUTCRange(value, value.Location())

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

		} else if field.Type == State || field.Type == District || field.Type == Ward {
			value := strings.ToLower(c.Value())
			var name = ""

			if field.Type == Ward {
				name = "fields.ward"
			} else if field.Type == District {
				name = "fields.district"
			} else if field.Type == State {
				name = "fields.state"
			}
			name += "_keyword"

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
			return nil, fmt.Errorf("unsupported contact field type: %s", field.Type)
		}
	} else if field.Category == ContactAttribute {
		value := strings.ToLower(c.Value())

		// special case for set/unset for name and language
		if (c.Comparator() == "=" || c.Comparator() == "!=") && (field.Key == NameAttribute || field.Key == LanguageAttribute) && value == "" {
			query = elastic.NewBoolQuery().Must(
				elastic.NewExistsQuery("name"),
				elastic.NewBoolQuery().MustNot(elastic.NewTermQuery("name.keyword", "")),
			)

			if c.Comparator() == "=" {
				query = elastic.NewBoolQuery().MustNot(query)
			}

			return query, nil
		}

		if field.Key == NameAttribute {
			if c.Comparator() == "=" {
				return elastic.NewTermQuery("name.keyword", c.Value()), nil
			} else if c.Comparator() == "~" {
				return elastic.NewMatchQuery("name", value), nil
			} else if c.Comparator() == "!=" {
				return elastic.NewBoolQuery().MustNot(elastic.NewTermQuery("name.keyword", c.Value())), nil
			} else {
				return nil, fmt.Errorf("unsupported name query comparator: %s", c.Comparator())
			}
		} else if field.Key == IDAttribute {
			if c.Comparator() == "=" {
				return elastic.NewIdsQuery().Ids(value), nil
			}
			return nil, fmt.Errorf("unsupported comparator for id: %s", c.Comparator())
		} else if field.Key == LanguageAttribute {
			if c.Comparator() == "=" {
				return elastic.NewTermQuery("language", value), nil
			} else if c.Comparator() == "!=" {
				return elastic.NewBoolQuery().MustNot(elastic.NewTermQuery("language", value)), nil
			} else {
				return nil, fmt.Errorf("unsupported language comparator: %s", c.Comparator())
			}
		} else if field.Key == CreatedOnAttribute {
			value, err := utils.DateTimeFromString(env, c.Value(), false)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to parse datetime: %s", c.Value())
			}
			start, end := utils.DateToUTCRange(value, value.Location())

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
			return nil, fmt.Errorf("unsupported contact attribute: %s", field.Key)
		}
	} else if field.Category == Scheme {
		value := strings.ToLower(c.Value())

		// special case for set/unset
		if (c.Comparator() == "=" || c.Comparator() == "!=") && value == "" {
			query = elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
				elastic.NewTermQuery("urns.scheme", field.Key),
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
				elastic.NewTermQuery("urns.scheme", field.Key)),
			), nil
		} else if c.Comparator() == "~" {
			return elastic.NewNestedQuery("urns", elastic.NewBoolQuery().Must(
				elastic.NewMatchPhraseQuery("urns.path", value),
				elastic.NewTermQuery("urns.scheme", field.Key)),
			), nil
		} else {
			return nil, fmt.Errorf("unsupported scheme comparator: %s", c.Comparator())
		}
	} else if field.Category == Unavailable {
		return elastic.NewIdsQuery().Ids("-1"), nil
	}

	return nil, errors.Errorf("unsupported category type: %s", field.Category)
}
