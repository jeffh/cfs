//go:build darwin || freebsd || netbsd || openbsd
// +build darwin freebsd netbsd openbsd

package ninep

import (
	"io/fs"
	"os/user"
	"strconv"
	"syscall"
	"time"
)

func GetBlockSize() (int64, error) {
	var s syscall.Statfs_t
	if err := syscall.Statfs(".", &s); err != nil {
		return 0, err
	}
	return int64(s.Bsize), nil
}

func Atime(info fs.FileInfo) (t time.Time, ok bool) {
	var statT *syscall.Stat_t
	statT, ok = info.Sys().(*syscall.Stat_t)
	if ok {
		t = time.Unix(statT.Atimespec.Sec, statT.Atimespec.Nsec)
	}
	return
}

func FileId(info fs.FileInfo) (inode uint64, ok bool) {
	var statT *syscall.Stat_t
	statT, ok = info.Sys().(*syscall.Stat_t)
	if ok {
		inode = statT.Ino
	}
	return
}

func FileUsers(info fs.FileInfo) (uid, gid, muid string, err error) {
	statT, ok := info.Sys().(*syscall.Stat_t)
	if ok {
		var usr *user.User
		usr, err = user.LookupId(strconv.Itoa(int(statT.Uid)))
		if err != nil {
			return
		}
		uid = usr.Username

		var grp *user.Group
		grp, err = user.LookupGroupId(strconv.Itoa(int(statT.Gid)))
		if err != nil {
			return
		}
		gid = grp.Name

		// unix do not support last modified user
		muid = ""
	} else if s, ok := info.Sys().(Stat); ok {
		return s.fileUsers()
	} else {
		err = ErrUnsupported
	}
	return
}
