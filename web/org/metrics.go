package org

import (
	"context"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/golang/protobuf/proto"
	"github.com/nyaruka/gocommon/uuids"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/mailroom/models"
	"github.com/nyaruka/mailroom/web"
	"github.com/pkg/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

func init() {
	web.RegisterRoute(http.MethodGet, "/mr/org/{uuid:[0-9a-f\\-]+}/metrics", handleMetrics)
}

const groupCountsSQL = `
SELECT 
  g.id AS id, 
  g.name AS name, 
  g.uuid AS uuid, 
  g.group_type AS group_type, 
  COALESCE(SUM(c.count), 0) AS count 
FROM 
  contacts_contactgroup g 
LEFT OUTER JOIN 
  contacts_contactgroupcount c 
ON 
  c.group_id = g.id 
WHERE 
  g.org_id = $1 AND 
  g.is_active = TRUE 
GROUP BY 
 g.id;
`

type groupCountRow struct {
	ID    models.GroupID   `db:"id"`
	Name  string           `db:"name"`
	UUID  assets.GroupUUID `db:"uuid"`
	Type  string           `db:"group_type"`
	Count int64            `db:"count"`
}

func calculateGroupCounts(ctx context.Context, s *web.Server, org *models.OrgReference) (*dto.MetricFamily, error) {
	rows, err := s.DB.QueryxContext(ctx, groupCountsSQL, org.ID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying group counts for org")
	}
	defer rows.Close()

	family := &dto.MetricFamily{
		Name:   proto.String("rapidpro_group_contact_count"),
		Help:   proto.String("the number of contacts in various groups"),
		Type:   dto.MetricType_GAUGE.Enum(),
		Metric: []*dto.Metric{},
	}

	row := &groupCountRow{}
	for rows.Next() {
		err = rows.StructScan(row)
		if err != nil {
			return nil, errors.Wrapf(err, "error scanning group count row")
		}

		groupType := "user"
		if row.Type != "U" {
			groupType = "system"
		}

		family.Metric = append(family.Metric,
			&dto.Metric{
				Label: []*dto.LabelPair{
					&dto.LabelPair{
						Name:  proto.String("group_name"),
						Value: proto.String(row.Name),
					},
					&dto.LabelPair{
						Name:  proto.String("group_uuid"),
						Value: proto.String(string(row.UUID)),
					},
					&dto.LabelPair{
						Name:  proto.String("group_type"),
						Value: proto.String(groupType),
					},
					&dto.LabelPair{
						Name:  proto.String("org"),
						Value: proto.String(org.Name),
					},
				},
				Gauge: &dto.Gauge{
					Value: proto.Float64(float64(row.Count)),
				},
			},
		)
	}

	return family, err
}

const channelCountsSQL = `
SELECT 
  ch.id AS id, 
  ch.uuid AS uuid, 
  ch.name AS name, 
  ch.role AS role,
  ch.channel_type AS channel_type,
  c.count_type AS count_type, 
  COALESCE(SUM(c.count), 0) as count 
FROM 
  channels_channel ch 
LEFT OUTER JOIN 
  channels_channelcount c
ON 
  c.channel_id = ch.id 
WHERE 
  ch.org_id = $1 AND 
  ch.is_active = TRUE
GROUP BY 
  (ch.id, c.count_type);
`

type channelCountRow struct {
	ID          models.ChannelID   `db:"id"`
	UUID        assets.ChannelUUID `db:"uuid"`
	Name        string             `db:"name"`
	Role        string             `db:"role"`
	ChannelType string             `db:"channel_type"`
	CountType   *string            `db:"count_type"`
	Count       int64              `db:"count"`
}

type channelStats struct {
	ID          models.ChannelID
	UUID        assets.ChannelUUID
	Name        string
	Role        string
	ChannelType string
	Counts      map[string]int64
}

func calculateChannelCounts(ctx context.Context, s *web.Server, org *models.OrgReference) (*dto.MetricFamily, error) {
	rows, err := s.DB.QueryxContext(ctx, channelCountsSQL, org.ID)
	if err != nil {
		return nil, errors.Wrapf(err, "error querying channel counts for org")
	}
	defer rows.Close()

	// we build an intermediate struct here of possible values because we always want to expose all
	// possible metrics for a channel even if they aren't set. (IE, outgoing messages even if no messages
	// have been sent yet) So we build a channel dictionarly that initializes possible values based on the
	// role of the channel
	channels := make(map[assets.ChannelUUID]*channelStats)
	row := &channelCountRow{}
	for rows.Next() {
		err = rows.StructScan(row)
		if err != nil {
			return nil, errors.Wrapf(err, "error scanning channel count row")
		}

		channel, found := channels[row.UUID]
		if !found {
			channel = &channelStats{
				ID:          row.ID,
				UUID:        row.UUID,
				Name:        row.Name,
				Role:        row.Role,
				ChannelType: row.ChannelType,
				Counts:      make(map[string]int64),
			}
			channels[row.UUID] = channel

			// populate expected stats
			for _, role := range row.Role {
				switch role {
				case 'S':
					channel.Counts["OM"] = 0
				case 'R':
					channel.Counts["IM"] = 0
				case 'C':
					channel.Counts["OV"] = 0
				case 'A':
					channel.Counts["IV"] = 0
				}
			}
		}

		// set our count if we have one and it isn't a channel log count
		if row.CountType != nil {
			channel.Counts[*row.CountType] = row.Count
		}
	}

	// now convert our normalized channels into our family of metrics
	family := &dto.MetricFamily{
		Name:   proto.String("rapidpro_channel_msg_count"),
		Help:   proto.String("the number of messages sent and received for a channel"),
		Type:   dto.MetricType_GAUGE.Enum(),
		Metric: []*dto.Metric{},
	}

	for _, channel := range channels {
		for countType, count := range channel.Counts {
			// ignore channel log counts
			if countType[0] == 'L' {
				continue
			}

			direction := "in"
			if countType[0] == 'O' {
				direction = "out"
			}

			countType := "message"
			if countType[1] == 'V' {
				countType = "voice"
			}

			family.Metric = append(family.Metric,
				&dto.Metric{
					Label: []*dto.LabelPair{
						&dto.LabelPair{
							Name:  proto.String("channel_name"),
							Value: proto.String(channel.Name),
						},
						&dto.LabelPair{
							Name:  proto.String("channel_uuid"),
							Value: proto.String(string(channel.UUID)),
						},
						&dto.LabelPair{
							Name:  proto.String("channel_type"),
							Value: proto.String(channel.ChannelType),
						},
						&dto.LabelPair{
							Name:  proto.String("msg_direction"),
							Value: proto.String(direction),
						},
						&dto.LabelPair{
							Name:  proto.String("msg_type"),
							Value: proto.String(countType),
						},
						&dto.LabelPair{
							Name:  proto.String("org"),
							Value: proto.String(org.Name),
						},
					},
					Gauge: &dto.Gauge{
						Value: proto.Float64(float64(count)),
					},
				},
			)
		}
	}

	return family, err
}

func handleMetrics(ctx context.Context, s *web.Server, r *http.Request, rawW http.ResponseWriter) error {
	// we should have basic auth headers, username should be metrics
	username, token, ok := r.BasicAuth()
	if !ok || username != "metrics" {
		rawW.WriteHeader(http.StatusUnauthorized)
		rawW.Write([]byte(`{"error": "invalid authentication"}`))
		return nil
	}

	orgUUID := uuids.UUID(chi.URLParam(r, "uuid"))
	org, err := models.LookupOrgByUUIDAndToken(ctx, s.DB, orgUUID, "Prometheus", token)
	if err != nil {
		return errors.Wrapf(err, "error looking up org for token")
	}

	if org == nil {
		rawW.WriteHeader(http.StatusUnauthorized)
		rawW.Write([]byte(`{"error": "invalid authentication"}`))
		return nil
	}

	groups, err := calculateGroupCounts(ctx, s, org)
	if err != nil {
		return errors.Wrapf(err, "error calculating group counts for org: %d", org.ID)
	}

	channels, err := calculateChannelCounts(ctx, s, org)
	if err != nil {
		return errors.Wrapf(err, "error calculating channel counts for org: %d", org.ID)
	}

	rawW.WriteHeader(http.StatusOK)

	_, err = expfmt.MetricFamilyToText(rawW, groups)
	if err != nil {
		return err
	}

	if len(channels.Metric) > 0 {
		_, err = expfmt.MetricFamilyToText(rawW, channels)
		if err != nil {
			return err
		}
	}

	return err
}
