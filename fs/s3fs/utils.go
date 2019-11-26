package s3fs

import (
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/jeffh/cfs/ninep"
)

func interpretTimeKeyValues(kv ninep.KVMap) time.Duration {
	total := time.Duration(0)
	total += time.Duration(kv.GetOneInt64("seconds"))
	total += time.Duration(kv.GetOneInt64("second"))
	total += time.Duration(kv.GetOneInt64("minute")) * time.Minute
	total += time.Duration(kv.GetOneInt64("minutes")) * time.Minute
	total += time.Duration(kv.GetOneInt64("hour")) * time.Hour
	total += time.Duration(kv.GetOneInt64("hours")) * time.Hour
	total += time.Duration(kv.GetOneInt64("day")) * 24 * time.Hour
	total += time.Duration(kv.GetOneInt64("days")) * 24 * time.Hour
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

func mapPtrIfNotEmpty(m ninep.KVMap) map[string]*string {
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

func writeKeyStringPtr(w io.Writer, key string, value *string) (err error) {
	if v := value; v != nil {
		_, err = fmt.Fprintf(w, "%s\n", ninep.KeyPair(key, *v))
	}
	return
}

func writeKeyInt64Ptr(w io.Writer, key string, value *int64) (err error) {
	if v := value; v != nil {
		_, err = fmt.Fprintf(w, "%s\n", ninep.KeyPair(key, strconv.FormatInt(*v, 10)))
	}
	return
}

func writeKeyBoolPtr(w io.Writer, key string, value *bool) (err error) {
	if v := value; v != nil {
		var s string
		if *v {
			s = "true"
		} else {
			s = "false"
		}
		_, err = fmt.Fprintf(w, "%s\n", ninep.KeyPair(key, s))
	}
	return
}
func writeKeyTimePtr(w io.Writer, key string, value *time.Time) (err error) {
	if v := value; v != nil {
		_, err = fmt.Fprintf(w, "%s\n", ninep.KeyPair(key, v.String()))
	}
	return
}
