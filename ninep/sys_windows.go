// +build windows

package ninep

import (
	"os"
	"syscall"
	"time"
)

func GetBlockSize() (int64, error) {
	return 64 * 1024 * 1024, nil
}

func Atime(info os.FileInfo) (t time.Time, ok bool) {

	attr, ok := fi.Sys().(*syscall.Win32FileAttributeData)

	if ok {
		t = time.Unix(0, attr.LastAccessTime.Nanoseconds())
	}
	return
}
