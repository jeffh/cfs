package s3fs

import (
	"bytes"
	"io"
	"strconv"
	"time"

	"github.com/jeffh/cfs/ninep/kvp"
)

func interpretTimeKeyValues(m kvp.Map) time.Duration {
	total := time.Duration(0)
	total += time.Duration(m.GetOneInt64("seconds"))
	total += time.Duration(m.GetOneInt64("second"))
	total += time.Duration(m.GetOneInt64("minute")) * time.Minute
	total += time.Duration(m.GetOneInt64("minutes")) * time.Minute
	total += time.Duration(m.GetOneInt64("hour")) * time.Hour
	total += time.Duration(m.GetOneInt64("hours")) * time.Hour
	total += time.Duration(m.GetOneInt64("day")) * 24 * time.Hour
	total += time.Duration(m.GetOneInt64("days")) * 24 * time.Hour
	return total
}

func writeDuration(w io.Writer, d time.Duration) error {
	days := int64(d.Hours() / 24)
	hours := int64(d.Hours()) % 24
	minutes := int64(d.Minutes()) % 60
	seconds := int64(d.Seconds()) % 60

	pairs := [][2]string{}
	if days > 0 {
		pairs = append(pairs, [2]string{"days", strconv.FormatInt(days, 10)})
	}
	if hours > 0 {
		pairs = append(pairs, [2]string{"hours", strconv.FormatInt(hours, 10)})
	}
	if minutes > 0 {
		pairs = append(pairs, [2]string{"minutes", strconv.FormatInt(minutes, 10)})
	}
	if seconds > 0 || len(pairs) == 0 {
		pairs = append(pairs, [2]string{"seconds", strconv.FormatInt(seconds, 10)})
	}

	_, err := io.WriteString(w, kvp.NonEmptyKeyPairs(pairs))
	return err
}

func stringDuration(d time.Duration) string {
	var buf bytes.Buffer
	_ = writeDuration(&buf, d)
	return buf.String()
}

func stringPtrIfNotEmpty(s string) *string {
	if s != "" {
		return &s
	}
	return nil
}

func int64PtrIfNotEmpty(s string) *int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil
	}
	return &i
}

func timePtrIfNotEmpty(s string) *time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}
	return &t
}

