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

        attr, ok := info.Sys().(*syscall.Win32FileAttributeData)

	if ok {
		t = time.Unix(0, attr.LastAccessTime.Nanoseconds())
	}
	return
}

func FileId(info os.FileInfo) (fileId uint64, ok bool) {
	var h *syscall.Handle
	h, ok = info.Sys().(*syscall.Handle)
	if ok {
		var hInfo syscall.ByHandleFileInformation
		ok = syscall.GetFileInformationByHandle(h, &hInfo)
		if ok {
			fileId = hInfo.FileIndexHigh<<32 | hInfo.FileIndexLow
		}
	}
	return
}

// TODO: add windows support
func FileUsers(info os.FileInfo) (uid, gid, muid string, err error) {
	if s, ok := info.Sys().(Stat); ok {
		return s.fileUsers()
	} else {
		err = ErrUnsupported
	}
	return
}
