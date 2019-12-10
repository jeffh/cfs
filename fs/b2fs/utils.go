package b2fs

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

func writeKeyString(w io.Writer, key string, value string) (err error) {
	_, err = fmt.Fprintf(w, "%s\n", ninep.KeyPair(key, value))
	return
}

func writeKeyInt64(w io.Writer, key string, value int64) (err error) {
	_, err = fmt.Fprintf(w, "%s\n", ninep.KeyPair(key, strconv.FormatInt(value, 10)))
	return
}

func writeKeyTime(w io.Writer, key string, value time.Time) (err error) {
	var t time.Time
	if value != t {
		_, err = fmt.Fprintf(w, "%s\n", ninep.KeyPair(key, value.String()))
	}
	return
}
