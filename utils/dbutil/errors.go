package dbutil

import "github.com/lib/pq"

// IsUniqueViolation returns true if the given error is a violation of unique constraint
func IsUniqueViolation(err error) bool {
	if pqErr, ok := err.(*pq.Error); ok {
		return pqErr.Code.Name() == "unique_violation"
	}
	return false
}
