package ninep

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// Represent a file that can be read or written to. Can be either a file or directory
type FileHandle interface {
	io.ReaderAt
	io.WriterAt
	io.Closer

	Sync() error
}

// Special file handle used for authentication
type AuthFileHandle interface {
	FileHandle
	// Returns true if the user is authorized to access the FileSystem
	// Called when Tattach occurs, after ta user has authenticated from Tauth
	Authorized() bool
}

// return os.FileInfo from FileSystem can implement this if they want to
// utilize modes only available in 9P protocol
type FileInfoMode9P interface{ Mode9P() Mode }

// return os.FileInfo from FileSystem can implement this if they want more
// precisely control the Qid version, which should change every time the
// version number changes.
type FileInfoVersion interface{ Version() uint32 }

// if this file info supports plan9 usernames for files
type FileInfoUid interface{ Uid() string }

// if this file info supports plan9 group names for files
type FileInfoGid interface{ Gid() string }

// if this file info supports plan9 usernames names for last modified
type FileInfoMuid interface{ Muid() string }

// TODO: support this feature
// return os.FileInfo from FileSystem can implement this if they want more
// precisely control the Qid path, which represents the same internal file
// in the file system (aka - rm foo.txt; touch foo.txt operate on two different
// paths)
// type FileInfoPath interface{ Path() uint32 }

// Return nil, nil to indicate no authentication needed
type Authorizer interface {
	Auth(ctx context.Context, addr, user, access string) (AuthFileHandle, error)
}

type FileSystem interface {
	MakeDir(path string, mode Mode) error
	CreateFile(path string, flag OpenMode, mode Mode) (FileHandle, error)
	OpenFile(path string, flag OpenMode, mode Mode) (FileHandle, error)
	ListDir(path string) ([]os.FileInfo, error)
	Stat(path string) (os.FileInfo, error)
	WriteStat(path string, s Stat) error
	Delete(path string) error
}

////////////////////////////////////////////////

// file info helper wrappers
type fileInfoWithName struct {
	fi   os.FileInfo
	name string
}

func FileInfoWithName(fi os.FileInfo, name string) os.FileInfo {
	return &fileInfoWithName{fi, name}
}

func (f *fileInfoWithName) Name() string       { return f.name }
func (f *fileInfoWithName) Size() int64        { return f.fi.Size() }
func (f *fileInfoWithName) Mode() os.FileMode  { return f.fi.Mode() }
func (f *fileInfoWithName) ModTime() time.Time { return f.fi.ModTime() }
func (f *fileInfoWithName) IsDir() bool        { return f.fi.IsDir() }
func (f *fileInfoWithName) Sys() interface{}   { return f.fi.Sys() }

// file info unix to plan9 wrappers
type fileInfoWithUsers struct {
	fi             os.FileInfo
	uid, gid, muid string
}

type FileInfoUsers interface {
	os.FileInfo
	FileInfoUid
	FileInfoGid
	FileInfoMuid
}

func FileInfoWithUsers(fi os.FileInfo, uid, gid, muid string) FileInfoUsers {
	return &fileInfoWithUsers{fi, uid, gid, muid}
}

func (f *fileInfoWithUsers) Name() string       { return f.fi.Name() }
func (f *fileInfoWithUsers) Size() int64        { return f.fi.Size() }
func (f *fileInfoWithUsers) Mode() os.FileMode  { return f.fi.Mode() }
func (f *fileInfoWithUsers) ModTime() time.Time { return f.fi.ModTime() }
func (f *fileInfoWithUsers) IsDir() bool        { return f.fi.IsDir() }
func (f *fileInfoWithUsers) Sys() interface{}   { return f.fi.Sys() }
func (f *fileInfoWithUsers) Uid() string        { return f.uid }
func (f *fileInfoWithUsers) Gid() string        { return f.gid }
func (f *fileInfoWithUsers) Muid() string       { return f.muid }

/*
func (f *fileInfoWithUsers) Path() uint32 {
	fileId, ok := FileId(f.fi)
	if ok {
		// this is practically more unique that the number we're probably generating?
		return uint32(ufid&0xffffffff) ^ uint32((ufid&0xffffffff00000000)>>32)
	} else {
		return 0
	}
}
*/

////////////////////////////////////////////////

// Implements a basic file system to the
type Dir string

func (d Dir) MakeDir(path string, mode Mode) error {
	fullPath := filepath.Join(string(d), path)
	return os.Mkdir(fullPath, mode.ToOsMode()&os.ModePerm)
}

func (d Dir) CreateFile(path string, flag OpenMode, mode Mode) (FileHandle, error) {
	fullPath := filepath.Join(string(d), path)
	return os.OpenFile(fullPath, flag.ToOsFlag()|os.O_CREATE, mode.ToOsMode())
}

func (d Dir) OpenFile(path string, flag OpenMode, mode Mode) (FileHandle, error) {
	fullPath := filepath.Join(string(d), path)
	return os.OpenFile(fullPath, mode.ToOsFlag(flag), mode.ToOsMode())
}

func (d Dir) ListDir(path string) ([]os.FileInfo, error) {
	fullPath := filepath.Join(string(d), path)
	return ioutil.ReadDir(fullPath)
}

func (d Dir) Stat(path string) (os.FileInfo, error) {
	fullPath := filepath.Join(string(d), path)
	info, err := os.Stat(fullPath)
	if err == nil {
		if fullPath == string(d) {
			info = FileInfoWithName(info, "")
		}

		uid, gid, muid, err := FileUsers(info)
		if err != nil {
			return nil, err
		}
		info = FileInfoWithUsers(info, uid, gid, muid)
	}
	return info, err
}

func (d Dir) WriteStat(path string, s Stat) error {
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
			return ErrUnsupported
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
		oldAtime, ok = Atime(info)
		if !ok {
			oldAtime = info.ModTime()
		}
		oldMtime = info.ModTime()

		if changeMtime && changeAtime {
			fmt.Printf("change Mtime (%v) & Atime (%v) -> %s\n", time.Unix(int64(s.Atime()), 0), time.Unix(int64(s.Mtime()), 0), err)
			err = os.Chtimes(fullPath, time.Unix(int64(s.Atime()), 0), time.Unix(int64(s.Mtime()), 0))
		} else if changeMtime {
			fmt.Printf("change Mtime\n")
			err = os.Chtimes(fullPath, oldAtime, time.Unix(int64(s.Mtime()), 0))
		} else if changeAtime {
			fmt.Printf("change Atime\n")
			err = os.Chtimes(fullPath, time.Unix(int64(s.Atime()), 0), oldMtime)
		} else {
			fmt.Printf("change nothing\n")
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
	err = nil
	return err
}

func (d Dir) Delete(path string) error {
	fullPath := filepath.Join(string(d), path)
	return os.RemoveAll(fullPath)
}

////////////////////////////////////////////////

// Loggable
type TraceFileHandle struct {
	H    FileHandle
	Path string
	Loggable
}

func (h *TraceFileHandle) ReadAt(p []byte, offset int64) (int, error) {
	n, err := h.H.ReadAt(p, offset)
	if err != nil {
		h.tracef("File(%v).ReadAt(_, %v) => (%d, %s)", h.Path, offset, n, err)
		h.errorf("File(%v).ReadAt(_, %v) => (%d, %s)", h.Path, offset, n, err)
	} else {
		h.tracef("File(%v).ReadAt(_, %v) => (%d, nil)", h.Path, offset, n)
	}
	return n, err
}

func (h *TraceFileHandle) WriteAt(p []byte, offset int64) (int, error) {
	n, err := h.H.WriteAt(p, offset)
	if err != nil {
		h.tracef("File(%v).WriteAt(len(%d), %v) => (%d, %s)", h.Path, len(p), offset, n, err)
		h.errorf("File(%v).WriteAt(len(%d), %v) => (%d, %s)", h.Path, len(p), offset, n, err)
	} else {
		h.tracef("File(%v).WriteAt(len(%d), %v) => (%d, nil)", h.Path, len(p), offset, n)
	}
	return n, err
}

func (h *TraceFileHandle) Sync() error {
	err := h.H.Sync()
	if err != nil {
		h.tracef("File(%v).Sync() => %s", h.Path, err)
		h.errorf("File(%v).Sync() => %s", h.Path, err)
	} else {
		h.tracef("File(%v).Sync() => nil", h.Path)
	}
	return err
}

func (h *TraceFileHandle) Close() error {
	err := h.H.Close()
	if err != nil {
		h.tracef("File(%v).Close() => %s", h.Path, err)
		h.errorf("File(%v).Close() => %s", h.Path, err)
	} else {
		h.tracef("File(%v).Close() => nil", h.Path)
	}
	return err
}

////////////////////

type TraceFileSystem struct {
	Fs FileSystem
	Loggable
}

func (f TraceFileSystem) MakeDir(path string, mode Mode) error {
	err := f.Fs.MakeDir(path, mode)
	f.tracef("FS.MakeDir(%v, %s) => %s", path, mode, err)
	if err != nil {
		f.errorf("FS.MakeDir(%v, %s) => %s", path, mode, err)
	}
	return err
}

func (f TraceFileSystem) CreateFile(path string, flag OpenMode, mode Mode) (FileHandle, error) {
	h, err := f.Fs.CreateFile(path, flag, mode)
	f.tracef("FS.CreateFile(%v, %s, %s) => (%v, %s)", path, flag, mode, h, err)
	if err != nil || h == nil {
		f.errorf("FS.CreateFile(%v, %s, %s) => (%v, %s)", path, flag, mode, h, err)
	}
	h = &TraceFileHandle{
		H:        h,
		Path:     path,
		Loggable: f.Loggable,
	}
	return h, err
}

func (f TraceFileSystem) OpenFile(path string, flag OpenMode, mode Mode) (FileHandle, error) {
	h, err := f.Fs.OpenFile(path, flag, mode)
	f.tracef("FS.OpenFile(%v, %s, %s) => (%v, %s)", path, flag, mode, h, err)
	if err != nil || h == nil {
		f.errorf("FS.OpenFile(%v, %s, %s) => (%v, %s)", path, flag, mode, h, err)
	}
	h = &TraceFileHandle{
		H:        h,
		Path:     path,
		Loggable: f.Loggable,
	}
	return h, err
}

func (f TraceFileSystem) ListDir(path string) ([]os.FileInfo, error) {
	infos, err := f.Fs.ListDir(path)
	f.tracef("FS.ListDir(%v) => (%v, %s)", infos, err)
	if err != nil {
		f.errorf("FS.ListDir(%v) => (_, %s)", path, err)
	}
	return infos, err
}

func (f TraceFileSystem) Stat(path string) (os.FileInfo, error) {
	info, err := f.Fs.Stat(path)
	if info != nil {
		f.tracef("FS.Stat(%v) => (os.FileInfo{name: %#v, size: %d...}, %s)", path, info.Name(), info.Size(), err)
	} else {
		f.tracef("FS.Stat(%v) => (nil, %s)", path, err)
	}
	if err != nil {
		f.errorf("FS.Stat(%v) => (_, %s)", path, err)
	}
	return info, err
}

func (f TraceFileSystem) WriteStat(path string, s Stat) error {
	f.tracef("FS.WriteStat(%v, %s)", path, s)
	err := f.Fs.WriteStat(path, s)
	if err != nil {
		f.errorf("FS.WriteStat(%v, %s) => %s", path, s, err)
	}
	return err
}

func (f TraceFileSystem) Delete(path string) error {
	f.tracef("FS.Delete(%v)", path)
	err := f.Fs.Delete(path)
	if err != nil {
		f.errorf("FS.Delete(%v) => %s", path, err)
	}
	return err
}
