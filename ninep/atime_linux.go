// +build linux

package ninep

import (
	"os"
	"syscall"
	"time"
)

func Atime(info os.FileInfo) (t time.Time, ok bool) {
	var statT *syscall.Stat_t
	statT, ok = info.Sys().(*syscall.Stat_t)
	if ok {
		t = time.Unix(statT.Atim.Sec, statT.Atim.Nsec)
		return
	}
}
