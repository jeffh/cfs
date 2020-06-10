package fs

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	ninep "github.com/jeffh/cfs/ninep"
)

////////////////////////////////////////////////

// Dir implements a basic file system to the local file system with a given root dir.
// The type represents the root directory for this file system
type Dir string

var _ ninep.FileSystem = Dir("")

// MakeDir creates a local directory as subdirectory of the root directory of Dir
func (d Dir) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	fullPath := filepath.Join(string(d), path)
	return os.MkdirAll(fullPath, mode.ToOsMode()&os.ModePerm)
}

// CreateFile creates a new file as a descendent of the root directory of Dir
func (d Dir) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	fullPath := filepath.Join(string(d), path)
	return os.OpenFile(fullPath, flag.ToOsFlag()|os.O_CREATE, mode.ToOsMode())
}

// OpenFile opens an existing file that is a descendent of the root directory of Dir for reading/writing
func (d Dir) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	fullPath := filepath.Join(string(d), path)
	return os.OpenFile(fullPath, flag.ToOsFlag(), 0)
}

// ListDir lists all files and directories in a given subdirectory
func (d Dir) ListDir(ctx context.Context, path string) (ninep.FileInfoIterator, error) {
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

		if fullPath == string(d) && (info.Name() == "" || info.Name() == ".") {
			info = ninep.FileInfoWithName(info, "")
		}

		info = ninep.FileInfoWithUsers(info, uid, gid, muid)

		infos[i] = info
	}
	return ninep.FileInfoSliceIterator(infos), nil
}

// Stat returns information about a given file or directory
func (d Dir) Stat(ctx context.Context, path string) (os.FileInfo, error) {
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

// WriteStat updates file or directory metadata.
func (d Dir) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
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

	changeGID := !s.GidNoTouch()
	changeUID := !s.UidNoTouch()
	// NOTE(jeff): technically, the spec disallows changing uids
	// if changeUID != "" {
	// 	return errChangeUidNotAllowed
	// }
	if changeGID || changeUID {
		oldUID := -1
		oldGID := -1

		statT, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return ninep.ErrUnsupported
		}
		oldUID = int(statT.Uid)
		oldGID = int(statT.Gid)

		uid := -1
		gid := -1
		if changeUID {
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
		if changeGID {
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

		if changeUID && changeGID {
			err = os.Chown(fullPath, uid, gid)
		} else if changeGID {
			err = os.Chown(fullPath, -1, gid)
		} else if changeUID {
			err = os.Chown(fullPath, uid, -1)
		}
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				os.Chown(fullPath, oldUID, oldGID)
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

// Delete a file or directory. Deleting the root directory will be an error.
func (d Dir) Delete(ctx context.Context, path string) error {
	fullPath := filepath.Join(string(d), path)
	if fullPath == string(d) {
		return fmt.Errorf("Cannot delete root dir")
	}
	return os.RemoveAll(fullPath)
}

func (d Dir) Traverse(path string) (ninep.TraversableFile, error) {
	return ninep.BasicTraverse(d, path)
}
