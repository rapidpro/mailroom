package models

import (
	"context"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
)

// Track represents the combination of the environment, assets, context, db etc.. that is needed
// for a run to execute.
type Track struct {
	ctx context.Context
	db  *sqlx.DB
	rp  redis.Pool

	org *OrgAssets
}

func NewTrack(ctx context.Context, db *sqlx.DB, rp redis.Pool, org *OrgAssets) *Track {
	return &Track{
		ctx: ctx,
		db:  db,
		rp:  rp,
		org: org,
	}
}

func (t *Track) Org() *OrgAssets { return t.org }
