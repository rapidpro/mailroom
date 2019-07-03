package elastic

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nyaruka/goflow/contactql"
	"github.com/nyaruka/goflow/utils"
	q "github.com/olivere/elastic"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
)

type FieldCategory string

const ContactAttribute = FieldCategory("attribute")
const ContactField = FieldCategory("field")
const Scheme = FieldCategory("scheme")
const Unavailable = FieldCategory("unavailable")
const Implicit = FieldCategory("implicit")

type FieldType string

const Text = FieldType("text")
const DateTime = FieldType("datetime")
const Number = FieldType("number")
const State = FieldType("state")
const District = FieldType("district")
const Ward = FieldType("ward")

type Field struct {
	Key      string
	Category FieldCategory
	Type     FieldType
	UUID     utils.UUID
}

// FieldRegistry provides an interface for looking up queryable fields
type FieldRegistry interface {
	LookupField(key string) *Field
}

func ToElasticQuery(env utils.Environment, registry FieldRegistry, node contactql.QueryNode) (q.Query, error) {
	switch n := node.(type) {
	case *contactql.ContactQuery:
		return ToElasticQuery(env, registry, n.Root())
	case *contactql.BoolCombination:
		return BoolCombinationToElasticQuery(env, registry, n)
	case *contactql.Condition:
		return ConditionToElasticQuery(env, registry, n)
	default:
		return nil, errors.Errorf("unknown type converting to elastic query: %v", n)
	}
}

func BoolCombinationToElasticQuery(env utils.Environment, registry FieldRegistry, combination *contactql.BoolCombination) (q.Query, error) {
	queries := make([]q.Query, len(combination.Children()))
	for i, child := range combination.Children() {
		childQuery, err := ToElasticQuery(env, registry, child)
		if err != nil {
			return nil, errors.Wrapf(err, "error evaluating child query")
		}
		queries[i] = childQuery
	}

	if combination.Operator() == contactql.BoolOperatorAnd {
		return q.NewBoolQuery().Must(queries...), nil
	}

	return q.NewBoolQuery().Should(queries...), nil
}

func ConditionToElasticQuery(env utils.Environment, registry FieldRegistry, c *contactql.Condition) (q.Query, error) {
	field := registry.LookupField(c.Key())
	if field == nil {
		return nil, errors.Errorf("unable to find field: %s", c.Key())
	}

	var query q.Query

	if field.Category == Implicit {
		number, _ := strconv.Atoi(c.Value())

		if field.Key == "name_tel" {
			if number != 0 {
				return q.NewNestedQuery("urns", q.NewMatchPhraseQuery("urns.path", number)), nil
			}
			return q.NewMatchQuery("name", c.Value()), nil

		} else if field.Key == "name_id" {
			if number != 0 {
				return q.NewTermQuery("ids", number), nil
			}
			return q.NewMatchQuery("name", c.Value()), nil
		} else {
			return nil, fmt.Errorf("unknown implicit field key: %s", field.Key)
		}
	} else if field.Category == ContactField {
		fieldQuery := q.NewTermQuery("fields.field", field.UUID)

		if field.Type == Text {
			value := strings.ToLower(c.Value())
			if c.Comparator() == "=" {
				query = q.NewTermQuery("fields.text", value)
			} else if c.Comparator() == "!=" {
				query = q.NewBoolQuery().Must(
					fieldQuery,
					q.NewTermQuery("fields.text", value),
					q.NewExistsQuery("fields.text"),
				)
				return q.NewBoolQuery().MustNot(q.NewNestedQuery("fields", query)), nil
			} else {
				return nil, fmt.Errorf("unknown text comparator: %s", c.Comparator())
			}

			return q.NewBoolQuery().Must(fieldQuery, query), nil

		} else if field.Type == Number {
			value, err := decimal.NewFromString(c.Value())
			if err != nil {
				return nil, errors.Errorf("can't convert '%s' to a number", c.Value())
			}

			if c.Comparator() == "=" {
				query = q.NewMatchQuery("fields.number", value)
			} else if c.Comparator() == ">" {
				query = q.NewRangeQuery("fields.number").Gt(value)
			} else if c.Comparator() == ">=" {
				query = q.NewRangeQuery("fields.number").Gte(value)
			} else if c.Comparator() == "<" {
				query = q.NewRangeQuery("fields_number").Lt(value)
			} else if c.Comparator() == "<=" {
				query = q.NewRangeQuery("fields.number").Lte(value)
			} else {
				return nil, fmt.Errorf("unknown number comparator: %s", c.Comparator())
			}

			return q.NewBoolQuery().Must(fieldQuery, query), nil

		} else if field.Type == DateTime {
			value, err := utils.DateTimeFromString(env, c.Value(), false)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to parse datetime: %s", c.Value())
			}
			start, end := utils.DateToUTCRange(value, value.Location())

			if c.Comparator() == "=" {
				query = q.NewRangeQuery("fields.datetime").Gte(start).Lt(end)
			} else if c.Comparator() == ">" {
				query = q.NewRangeQuery("fields.datetime").Gte(end)
			} else if c.Comparator() == ">=" {
				query = q.NewRangeQuery("fields.datetime").Gte(start)
			} else if c.Comparator() == "<" {
				query = q.NewRangeQuery("fields.datetime").Lt(start)
			} else if c.Comparator() == "<=" {
				query = q.NewRangeQuery("fields.datetime").Lt(end)
			} else {
				return nil, fmt.Errorf("unknown datetime comparator: %s", c.Comparator())
			}

			return q.NewBoolQuery().Must(fieldQuery, query), nil

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
				query = q.NewTermQuery(name, value)
			} else if c.Comparator() == "!=" {
				return q.NewBoolQuery().MustNot(
					q.NewNestedQuery("fields",
						q.NewBoolQuery().Must(
							q.NewTermQuery(name, value),
							q.NewExistsQuery(name),
						),
					),
				), nil
			} else {
				return nil, fmt.Errorf("unknown location comparator: %s", c.Comparator())
			}

			return q.NewBoolQuery().Must(fieldQuery, query), nil
		} else {
			return nil, fmt.Errorf("unknown contact field type: %s", field.Type)
		}
	} else if field.Category == ContactAttribute {
		value := strings.ToLower(c.Value())

		if field.Key == "name" {
			if c.Comparator() == "=" {
				return q.NewTermQuery("name.keyword", c.Value()), nil
			} else if c.Comparator() == "~" {
				return q.NewMatchQuery("name", value), nil
			} else if c.Comparator() == "!=" {
				return q.NewBoolQuery().MustNot(q.NewTermQuery("name.keyword", c.Value())), nil
			} else {
				return nil, fmt.Errorf("unknown name query comparator: %s", c.Comparator())
			}
		} else if field.Key == "id" {
			if c.Comparator() == "=" {
				return q.NewTermQuery("ids", value), nil
			}
			return nil, fmt.Errorf("unknown comparator for id: %s", c.Comparator())
		} else if field.Key == "language" {
			if c.Comparator() == "=" {
				return q.NewTermQuery("language", value), nil
			} else if c.Comparator() == "!=" {
				return q.NewBoolQuery().MustNot(q.NewTermQuery("language", value)), nil
			} else {
				return nil, fmt.Errorf("unknown language comparator: %s", c.Comparator())
			}
		} else if field.Key == "created_on" {
			value, err := utils.DateTimeFromString(env, c.Value(), false)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to parse datetime: %s", c.Value())
			}
			start, end := utils.DateToUTCRange(value, value.Location())

			if c.Comparator() == "=" {
				return q.NewRangeQuery("created_on").Gte(start).Lt(end), nil
			} else if c.Comparator() == ">" {
				return q.NewRangeQuery("created_on").Gte(end), nil
			} else if c.Comparator() == ">=" {
				return q.NewRangeQuery("created_on").Gte(start), nil
			} else if c.Comparator() == "<" {
				return q.NewRangeQuery("created_on").Lt(start), nil
			} else if c.Comparator() == "<=" {
				return q.NewRangeQuery("created_on").Lt(end), nil
			} else {
				return nil, fmt.Errorf("unknown created_on comparator: %s", c.Comparator())
			}
		} else {
			return nil, fmt.Errorf("unknown contact attribute: %s", field.Key)
		}
	} else if field.Category == Scheme {
		value := strings.ToLower(c.Value())

		if c.Comparator() == "=" {
			return q.NewNestedQuery("urns", q.NewTermQuery("urns.path.keyword", value)), nil
		} else if c.Comparator() == "!=" {
			return q.NewNestedQuery("urns", q.NewMatchPhraseQuery("urns.path", value)), nil
		} else {
			return nil, fmt.Errorf("unknown scheme comparator: %s", c.Comparator())
		}
	} else if field.Category == Unavailable {
		return q.NewTermQuery("ids", "-1"), nil
	}

	return nil, errors.Errorf("unknown category type: %s", field.Category)
}
