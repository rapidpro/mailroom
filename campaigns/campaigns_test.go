package campaigns

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/jmoiron/sqlx"
)

func TestMain(m *testing.M) {
	resetDB()
	os.Exit(m.Run())
}

func mustExec(command string, args ...string) {
	cmd := exec.Command(command, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("error restoring database: %s: %s", err, string(output)))
	}
}

func resetDB() {
	db := sqlx.MustOpen("postgres", "postgres://temba@localhost/temba?sslmode=disable")
	db.MustExec("drop owned by temba cascade")
	mustExec("pg_restore", "-d", "temba", "../temba.dump")
}

func getDB() *sqlx.DB {
	db := sqlx.MustOpen("postgres", "postgres://temba@localhost/temba?sslmode=disable")
	return db
}
func TestCampaigns(t *testing.T) {
	// create a campaign and event

}
