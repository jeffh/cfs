package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	ninep "git.sr.ht/~jeffh/cfs/ninep"
)

////////////////////////////////////////////////

// Implements a basic file system to the local file system with a given root dir
type Dir string

func (d Dir) MakeDir(path string, mode ninep.Mode) error {
	fullPath := filepath.Join(string(d), path)
	return os.Mkdir(fullPath, mode.ToOsMode()&os.ModePerm)
}

func (d Dir) CreateFile(path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	fullPath := filepath.Join(string(d), path)
	return os.OpenFile(fullPath, flag.ToOsFlag()|os.O_CREATE, mode.ToOsMode())
}

func (d Dir) OpenFile(path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	fullPath := filepath.Join(string(d), path)
	return os.OpenFile(fullPath, flag.ToOsFlag(), 0)
}

func (d Dir) ListDir(path string) ([]os.FileInfo, error) {
	fullPath := filepath.Join(string(d), path)
	infos, err := ioutil.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}
	for i, info := range infos {
		uid, gid, muid, err := ninep.FileUsers(info)
		if err != nil {
			return nil, err
		}

		if fullPath == string(d) {
			info = ninep.FileInfoWithName(info, "")
		}

		info = ninep.FileInfoWithUsers(info, uid, gid, muid)

		infos[i] = info
	}
	return infos, nil
}

func (d Dir) Stat(path string) (os.FileInfo, error) {
	fullPath := filepath.Join(string(d), path)
	info, err := os.Stat(fullPath)
	if err == nil {
		uid, gid, muid, err := ninep.FileUsers(info)
		if err != nil {
			return nil, err
		}

		if fullPath == string(d) {
			info = ninep.FileInfoWithName(info, "")
		}

		info = ninep.FileInfoWithUsers(info, uid, gid, muid)
	}
	return info, err
}

func (d Dir) WriteStat(path string, s ninep.Stat) error {
	fullPath := filepath.Join(string(d), path)
	// for restoring:
	// "Either all the changes in wstat request happen, or none of them does:
	// if the request succeeds, all changes were made; if it fails, none were."
	info, err := os.Stat(fullPath)
	if err != nil {
		return err
	}

	if !s.NameNoTouch() && path != s.Name() {
		newPath := filepath.Join(string(d), s.Name())
		err = os.Rename(fullPath, newPath)
		if err != nil {
			return err
		}

		defer func() {
			if err != nil {
				os.Rename(newPath, fullPath)
			}
		}()

		fullPath = newPath
	}

	if !s.ModeNoTouch() {
		old := info.Mode()
		err = os.Chmod(fullPath, s.Mode().ToOsMode())
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				os.Chmod(fullPath, old)
			}
		}()
	}

	changeGid := !s.GidNoTouch()
	changeUid := !s.UidNoTouch()
	// NOTE(jeff): technically, the spec disallows changing uids
	// if changeUid != "" {
	// 	return errChangeUidNotAllowed
	// }
	if changeGid || changeUid {
		oldUid := -1
		oldGid := -1

		statT, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return ninep.ErrUnsupported
		}
		oldUid = int(statT.Uid)
		oldGid = int(statT.Gid)

		uid := -1
		gid := -1
		if changeUid {
			var usr *user.User
			usr, err = user.Lookup(s.Uid())
			if err != nil {
				return err
			}
			uid, err = strconv.Atoi(usr.Uid)
			if err != nil {
				return err
			}
		}
		if changeGid {
			var grp *user.Group
			grp, err = user.LookupGroup(s.Gid())
			if err != nil {
				return err
			}
			gid, err = strconv.Atoi(grp.Gid)
			if err != nil {
				return err
			}
		}

		if changeUid && changeGid {
			err = os.Chown(fullPath, uid, gid)
		} else if changeGid {
			err = os.Chown(fullPath, -1, gid)
		} else if changeUid {
			err = os.Chown(fullPath, uid, -1)
		}
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				os.Chown(fullPath, oldUid, oldGid)
			}
		}()
	}

	changeMtime := !s.MtimeNoTouch()
	changeAtime := !s.AtimeNoTouch()
	if changeAtime || changeMtime {
		var oldAtime, oldMtime time.Time
		var ok bool
		oldAtime, ok = ninep.Atime(info)
		if !ok {
			oldAtime = info.ModTime()
		}
		oldMtime = info.ModTime()

		if changeMtime && changeAtime {
			err = os.Chtimes(fullPath, time.Unix(int64(s.Atime()), 0), time.Unix(int64(s.Mtime()), 0))
		} else if changeMtime {
			err = os.Chtimes(fullPath, oldAtime, time.Unix(int64(s.Mtime()), 0))
		} else if changeAtime {
			err = os.Chtimes(fullPath, time.Unix(int64(s.Atime()), 0), oldMtime)
		}
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				os.Chtimes(fullPath, oldAtime, oldMtime)
			}
		}()
	}

	// this should be last since it's really hard to undo this
	if !s.LengthNoTouch() {
		err = os.Truncate(fullPath, int64(s.Length()))
		if err != nil {
			return err
		}
	}
	return err
}

func (d Dir) Delete(path string) error {
	fullPath := filepath.Join(string(d), path)
	if fullPath == string(d) {
		return fmt.Errorf("Cannot delete root dir")
	}
	return os.RemoveAll(fullPath)
}
