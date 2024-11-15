package s3fs

import (
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

func mapPtrIfNotEmpty(m kvp.Map) map[string]*string {
	if len(m) != 0 {
		res := make(map[string]*string)
		for k, v := range m {
			if len(v) > 0 {
				res[k] = &v[0]
			}
		}
		return res
	}
	return nil
}
