package models

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/dbutil"
	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/null"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// NilUserID is the id 0 considered as nil user id
	NilUserID = UserID(0)
)

// UserID is our type for user ids, which can be null
type UserID null.Int

// MarshalJSON marshals into JSON. 0 values will become null
func (i UserID) MarshalJSON() ([]byte, error) {
	return null.Int(i).MarshalJSON()
}

// UnmarshalJSON unmarshals from JSON. null values become 0
func (i *UserID) UnmarshalJSON(b []byte) error {
	return null.UnmarshalInt(b, (*null.Int)(i))
}

// Value returns the db value, null is returned for 0
func (i UserID) Value() (driver.Value, error) {
	return null.Int(i).Value()
}

// Scan scans from the db value. null values become 0
func (i *UserID) Scan(value interface{}) error {
	return null.ScanInt(value, (*null.Int)(i))
}

type UserRole string

const (
	UserRoleAdministrator UserRole = "A"
	UserRoleEditor        UserRole = "E"
	UserRoleViewer        UserRole = "V"
	UserRoleAgent         UserRole = "T"
	UserRoleSurveyor      UserRole = "S"
)

// User is our type for a user asset
type User struct {
	u struct {
		ID        UserID   `json:"id"`
		Email     string   `json:"email"`
		FirstName string   `json:"first_name"`
		LastName  string   `json:"last_name"`
		Role      UserRole `json:"role"`
		Team      *Team    `json:"team"`
	}
}

// ID returns the ID
func (u *User) ID() UserID { return u.u.ID }

// Email returns the email address
func (u *User) Email() string { return u.u.Email }

// Role returns the user's role in the current org
func (u *User) Role() UserRole { return u.u.Role }

// Name returns the name
func (u *User) Name() string {
	names := make([]string, 0, 2)
	if u.u.FirstName != "" {
		names = append(names, u.u.FirstName)
	}
	if u.u.LastName != "" {
		names = append(names, u.u.LastName)
	}
	return strings.Join(names, " ")
}

// Team returns the user's ticketing team if any
func (u *User) Team() *Team {
	return u.u.Team
}

var _ assets.User = (*User)(nil)

const selectOrgUsersSQL = `
SELECT ROW_TO_JSON(r) FROM (SELECT
    u.id AS "id",
    u.email AS "email",
	u.first_name as "first_name",
	u.last_name as "last_name",
    o.role AS "role",
	row_to_json(team_struct) AS team
FROM
    auth_user u
INNER JOIN (
    SELECT user_id, 'A' AS "role" FROM orgs_org_administrators WHERE org_id = $1
    UNION
    SELECT user_id, 'E' AS "role" FROM orgs_org_editors WHERE org_id = $1
    UNION
    SELECT user_id, 'V' AS "role" FROM orgs_org_viewers WHERE org_id = $1
    UNION
    SELECT user_id, 'T' AS "role" FROM orgs_org_agents WHERE org_id = $1
    UNION
    SELECT user_id, 'S' AS "role" FROM orgs_org_surveyors WHERE org_id = $1
) o ON o.user_id = u.id
LEFT JOIN orgs_usersettings s ON s.user_id = u.id 
LEFT JOIN LATERAL (SELECT id, uuid, name FROM tickets_team WHERE tickets_team.id = s.team_id) AS team_struct ON True
WHERE 
    u.is_active = TRUE
ORDER BY
    u.email ASC
) r;`

// loadUsers loads all the users for the passed in org
func loadUsers(ctx context.Context, db sqlx.Queryer, orgID OrgID) ([]assets.User, error) {
	start := time.Now()

	rows, err := db.Queryx(selectOrgUsersSQL, orgID)
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error querying users for org: %d", orgID)
	}
	defer rows.Close()

	users := make([]assets.User, 0, 10)
	for rows.Next() {
		user := &User{}
		err := dbutil.ScanJSON(rows, &user.u)
		if err != nil {
			return nil, errors.Wrapf(err, "error unmarshalling user")
		}
		users = append(users, user)
	}

	logrus.WithField("elapsed", time.Since(start)).WithField("org_id", orgID).WithField("count", len(users)).Debug("loaded users")

	return users, nil
}
