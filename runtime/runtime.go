package runtime

import (
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/storage"
	"github.com/olivere/elastic/v7"
)

// Runtime represents the set of services required to run many Mailroom functions. Used as a wrapper for
// those services to simplify call signatures but not create a direct dependency to Mailroom or Server
type Runtime struct {
	DB             *sqlx.DB
	ReadonlyDB     *sqlx.DB
	RP             *redis.Pool
	ES             *elastic.Client
	MediaStorage   storage.Storage
	SessionStorage storage.Storage
	Config         *Config
}
