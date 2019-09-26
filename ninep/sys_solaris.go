// +build solaris

package ninep

import (
	"os"
	"syscall"
	"time"
)

func GetBlockSize() (int64, error) {
	var s syscall.Statfs_t
	if err := syscall.Statfs(".", &s); err != nil {
		return 0, err
	}
	return s.Bsize
}

func Atime(info os.FileInfo) (t time.Time, ok bool) {
	var statT *syscall.Stat_t
	statT, ok = info.Sys().(*syscall.Stat_t)
	if ok {
		t = time.Unix(statT.Atim.Sec, statT.Atim.Nsec)
	}
	return
}
