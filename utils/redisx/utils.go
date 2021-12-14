package redisx

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/gocommon/random"
)

// StringsWithScores parses an array reply which is alternating pairs of strings and scores (floats)
func StringsWithScores(reply interface{}, err error) ([]string, []float64, error) {
	pairs, err := redis.Values(reply, err)
	if err != nil {
		return nil, nil, err
	}

	strings := make([]string, len(pairs)/2)
	scores := make([]float64, len(pairs)/2)

	for i := 0; i < len(pairs)/2; i++ {
		rawString := pairs[2*i].([]byte)
		rawScore := pairs[2*i+1].([]byte)

		score, err := strconv.ParseFloat(string(rawScore), 64)
		if err != nil {
			return nil, nil, err
		}

		strings[i] = string(rawString)
		scores[i] = score
	}

	return strings, scores, nil
}

func intervalTimestamp(t time.Time, interval time.Duration) string {
	t = t.UTC().Truncate(interval)
	if interval < time.Hour*24 {
		return t.Format("2006-01-02T15:04")
	}
	return t.Format("2006-01-02")
}

func intervalKeys(keyBase string, interval time.Duration, size int) []string {
	now := dates.Now()
	keys := make([]string, size)
	for i := range keys {
		timestamp := intervalTimestamp(now.Add(-interval*time.Duration(i)), interval)
		keys[i] = fmt.Sprintf("%s:%s", keyBase, timestamp)
	}
	return keys
}

const base64Charset = `ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/`

// RandomBase64 creates a random string of the length passed in
func RandomBase64(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = base64Charset[random.IntN(64)]
	}
	return string(b)
}
