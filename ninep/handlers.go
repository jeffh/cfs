package ninep

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var (
	errWriteNotAllowed     = errors.New("not allowed to write")
	errSeekNotAllowed      = errors.New("seeking is not allowed")
	errChangeUidNotAllowed = errors.New("changing uid is not allowed by protocol")
	errUnsupported         = errors.New("unsupported")
)

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

type Dir string

func (d Dir) MakeDir(path string, mode Mode) error {
	fullPath := filepath.Join(string(d), path)
	return os.Mkdir(fullPath, mode.ToOsMode()&os.ModePerm)
}

func (d Dir) CreateFile(path string, flag OpenMode, mode Mode) (FileHandle, error) {
	fullPath := filepath.Join(string(d), path)
	return os.OpenFile(fullPath, flag.ToOsFlags(), mode.ToOsMode()|0700)
}

func (d Dir) OpenFile(path string, flag OpenMode, mode Mode) (FileHandle, error) {
	fullPath := filepath.Join(string(d), path)
	return os.OpenFile(fullPath, flag.ToOsFlags(), mode.ToOsMode())
}

func (d Dir) ListDir(path string) ([]os.FileInfo, error) {
	fullPath := filepath.Join(string(d), path)
	return ioutil.ReadDir(fullPath)
}

func (d Dir) Stat(path string) (os.FileInfo, error) {
	fullPath := filepath.Join(string(d), path)
	return os.Stat(fullPath)
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

	if s.Length() != 0 || s.Muid() != "" || s.Dev() != 0 || s.Type() != 0 {
		return errUnsupported
	}

	if s.Name() != "" && path != s.Name() {
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

	if s.Mode() != 0 {
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

	changeGid := s.Gid() != ""
	changeUid := s.Uid() != ""
	// NOTE(jeff): technically, the spec disallows changing uids
	// if changeUid != "" {
	// 	return errChangeUidNotAllowed
	// }
	if changeGid || changeUid {
		oldUid := -1
		oldGid := -1

		statT, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return errUnsupported
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
			os.Chown(fullPath, oldUid, oldGid)
		}()
	}

	changeMtime := s.Mtime() != 0
	changeAtime := s.Atime() != 0
	if changeAtime || changeMtime {
		var oldAtime, oldMtime time.Time
		var ok bool
		oldAtime, ok = Atime(info)
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
			os.Chtimes(fullPath, oldAtime, oldMtime)
		}()
	}
	err = nil
	return err
}

func (d Dir) Delete(path string) error {
	return os.RemoveAll(path)
}

////////////////////////////////////////////////

type directoryHandle struct {
	fs       FileSystem
	offset   int64
	index    int
	allFiles []Stat
	session  *Session
	path     string
	fetched  bool
}

func (h *directoryHandle) ReadAt(p []byte, offset int64) (int, error) {
	if offset == 0 {
		// reset
		h.offset = 0
		h.index = 0
		h.allFiles = h.allFiles[:0]
		h.fetched = false
	}
	if h.offset != offset {
		return 0, errSeekNotAllowed
	}
	if !h.fetched {
		entries, err := h.fs.ListDir(h.path)
		if err != nil {
			return 0, err
		}
		h.allFiles = make([]Stat, len(entries))
		for i, info := range entries {
			subpath := filepath.Join(h.path, info.Name())
			q := h.session.PutQid(subpath, ModeFromOS(info.Mode()).QidType())
			st := fileInfoToStat(q, info)
			h.allFiles[i] = st
		}
		h.fetched = true
	}
	if h.index >= len(h.allFiles) {
		return 0, io.EOF
	}
	next := h.allFiles[h.index]
	size := next.Nbytes()
	if len(p) < size {
		return 0, errors.New("buffer too small")
	}
	copy(p, next.Bytes())

	h.offset += int64(size)
	h.index++
	return size, nil
}

func (h *directoryHandle) WriteAt(p []byte, offset int64) (int, error) {
	return 0, errWriteNotAllowed
}

func (h *directoryHandle) Sync() error {
	return nil
}

func (h *directoryHandle) Close() error {
	// reset
	h.offset = 0
	h.index = 0
	h.allFiles = nil
	h.fetched = false
	return nil
}

////////////////////////////////////////////////
type readOnlyMemoryBuffer struct{ bytes.Reader }

func (b *readOnlyMemoryBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, errWriteNotAllowed
}

func (b *readOnlyMemoryBuffer) Close() error { return nil }

////////////////////////////////////////////////

type FileHandle interface {
	io.ReaderAt
	io.WriterAt
	io.Closer

	Sync() error
}

func prepareFileInfoForStat(info os.FileInfo) (size int, name, uid, gid, muid string) {
	ownerInfo := func(v interface{}) (uid, gid, muid string, ok bool) {
		type hasUid interface{ Uid() string }
		type hasGid interface{ Gid() string }
		type hasMuid interface{ Muid() string }

		satisfactory := false
		if v, ok := v.(hasUid); ok {
			satisfactory = true
			uid = v.Uid()
			muid = uid
		} else {
			return uid, gid, muid, satisfactory
		}
		if v, ok := v.(hasGid); ok {
			gid = v.Gid()
		}
		if v, ok := v.(hasMuid); ok {
			muid = v.Muid()
		}

		return uid, gid, muid, satisfactory
	}

	var ok bool
	if uid, gid, muid, ok = ownerInfo(info); !ok {
		uid, gid, muid, ok = ownerInfo(info.Sys())
	}

	if info.Name() == "/" {
		name = "."
	} else {
		name = info.Name()
	}
	size = statSize(name, uid, gid, muid)
	return size, name, uid, gid, muid
}

func fillStatFromFileInfo(s Stat, qid Qid, info os.FileInfo) {
	size, name, uid, gid, muid := prepareFileInfoForStat(info)
	if len(s) < size {
		panic("Given a smaller buffer than needed")
	}
	s.fill(name, uid, gid, muid)

	s.SetQid(qid)
	s.SetMode(ModeFromOS(info.Mode()))
	s.SetAtime(uint32(info.ModTime().Unix()))
	s.SetMtime(uint32(info.ModTime().Unix()))
	s.SetLength(uint64(info.Size()))
}

func fileInfoToStat(qid Qid, info os.FileInfo) Stat {
	_, name, uid, gid, muid := prepareFileInfoForStat(info)
	s := NewStat(name, uid, gid, muid)
	fillStatFromFileInfo(s, qid, info)
	return s
}

func cleanPath(path string) string {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		path = filepath.Clean(string(os.PathSeparator) + path)
		// this can't fail: we're relative to root
		var err error
		path, err = filepath.Rel(string(os.PathSeparator), path)
		if err != nil {
			panic("unreachable")
		}
	}
	return filepath.Clean(path)
}

////////////////////////////////////////////////

type UnauthenticatedHandler struct {
	Fs FileSystem

	ErrorLog, TraceLog Logger

	st SessionTracker
}

func (h *UnauthenticatedHandler) errorf(format string, values ...interface{}) {
	if h.ErrorLog != nil {
		h.ErrorLog.Printf(format, values...)
	}
}

func (h *UnauthenticatedHandler) tracef(format string, values ...interface{}) {
	if h.TraceLog != nil {
		h.TraceLog.Printf(format, values...)
	}
}

func (h *UnauthenticatedHandler) Connected(addr string) {
	h.st.Add(addr)
}

func (h *UnauthenticatedHandler) Disconnected(addr string) {
	h.st.Remove(addr)
}

func (h *UnauthenticatedHandler) Handle9P(ctx context.Context, m Message, w Replier) {
	session := h.st.Lookup(w.RemoteAddr())
	if session == nil {
		h.errorf("No previous session for %s", w.RemoteAddr())
		return
	}
	switch m := m.(type) {
	case Tattach:
		if m.Afid() != NO_FID {
			h.tracef("local: Tattach: reject auth request")
			w.Rerror("no authentication")
			return
		}

		// associate fid to root
		h.tracef("local: Tattach: %v", m.Fid())
		session.PutFid(m.Fid(), File{
			Name: "/",
			User: m.Uname(),
			Flag: OREAD,
			Mode: M_DIR,
		})
		w.Rattach(session.PutQid("", QT_DIR))

	case Topen:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			h.errorf("local: Topen: unknown fid: %v", m.Fid())
			w.Rerror("unknown fid %d", m.Fid())
			return
		}
		fullPath := cleanPath(fil.Name)
		info, err := h.Fs.Stat(fullPath)
		if err != nil {
			h.errorf("local: Topen: failed to call stat on %v: %s", fullPath, err)
			w.Rerror("file does not exist: %s", fullPath)
			return
		}
		if info.IsDir() {
			// From Topen docs:
			//   "It is illegal to write a directory, truncate it, or attempt to remove it on close"
			if m.Mode()&ORCLOSE != 0 {
				h.tracef("local: Topen: client error: requested dir with ORCLOSE")
				w.Rerror("dir cannot have ORCLOSE set")
				return
			} else if m.Mode()&OTRUNC != 0 {
				h.tracef("local: Topen: client error: requested dir with OTRUNC")
				w.Rerror("dir cannot have OTRUNC set")
				return
			}
			fil.Mode = ModeFromOS(info.Mode())
			fil.H = &directoryHandle{
				fs:      h.Fs,
				session: session,
				path:    fullPath,
			}
		} else {
			f, err := h.Fs.OpenFile(fullPath, m.Mode(), fil.Mode)
			if err != nil || f == nil {
				h.tracef("local: Topen: error opening file %v: %s", fullPath, err)
				w.Rerror("cannot open: %s", err)
				return
			}
			fil.Mode = ModeFromOS(info.Mode())
			fil.Flag = m.Mode()
			fil.H = f
		}
		h.tracef("local: Topen: %v -> %v (isDir=%v, mode=%v)", m.Fid(), fil.Name, info.IsDir(), info.Mode())
		session.PutFid(m.Fid(), fil)
		q := session.PutQid(fil.Name, ModeFromOS(info.Mode()).QidType())
		w.Ropen(q, 0) // TODO: would be nice to support iounit

	case Twalk:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			h.errorf("local: Twalk: unknown fid: %v", m.Fid())
			w.Rerror("unknown fid %d", m.Fid())
			return
		}
		if m.Fid() != m.NewFid() {
			if _, found := session.FileForFid(m.NewFid()); found {
				h.errorf("local: Twalk: %v wanted new fid %v which is already taken", m.Fid(), m.NewFid())
				w.Rerror(fmt.Sprintf("conflict with newfid (fid=%d, newfid=%d)", m.Fid(), m.NewFid()))
				return
			}
		}
		if fil.Mode&M_DIR == 0 && m.NumWname() > 0 {
			h.errorf("local: Twalk: failed to because file is not a dir (fid %v): %v", m.Fid(), fil.Name)
			w.Rerror("walk in non-directory")
			return
		}

		walkedQids := make([]Qid, 0, int(m.NumWname()))
		path := cleanPath(fil.Name)
		var info os.FileInfo
		for i, size := 0, int(m.NumWname()); i < size; i++ {
			// TODO: Wname(i) is O(n) which we could optimize for this case
			name := cleanPath(m.Wname(i))
			if name == "/" {
				path = "/"
			} else if strings.Contains(name, string(os.PathSeparator)) {
				h.errorf("local: Twalk: invalid walk element for fid %d: %v", m.Fid(), name)
				w.Rerror("invalid walk element: %v", name)
				return
			} else {
				path = filepath.Join(path, name)
			}
			var err error
			info, err = h.Fs.Stat(path)
			if err != nil {
				h.errorf("local: Twalk: failed to call stat on %v for fid %v", path, m.Fid())
				break
			}

			h.tracef("local: Twalk: %v :: %v", m.Fid(), info.Mode())
			if err == nil {
				q := session.PutQid(fil.Name, ModeFromOS(info.Mode()).QidType())
				walkedQids = append(walkedQids, q)
			} else {
				q := session.PutQid(fil.Name, 0)
				walkedQids = append(walkedQids, q)
			}

			if fil.H != nil {
				fil.H.Close()
			}
			fil = File{
				Name: path,
				Mode: ModeFromOS(info.Mode()),
			}
		}
		// From 9p docs:
		//   "If the full sequence of nwname elements is walked successfully,
		//   newfid will represent the file that results. If not, newfid (and
		//   fid) will be unaffected"
		if len(walkedQids) == int(m.NumWname()) {
			session.PutFid(m.NewFid(), fil)
		}
		h.tracef("local: Twalk: %v: %v -> %v", m.Fid(), m.NumWname(), len(walkedQids))
		w.Rwalk(walkedQids)

	case Tstat:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			h.errorf("local: Tstat: unknown fid: %v", m.Fid())
			w.Rerror("unknown fid %d", m.Fid())
			return
		}
		fullPath := cleanPath(fil.Name)
		info, err := h.Fs.Stat(fullPath)
		if err != nil {
			h.errorf("local: Tstat: failed to call stat on %v: %s", fullPath, err)
			w.Rerror("file does not exist: %s", fullPath)
			return
		}
		qid := session.PutQid(fil.Name, ModeFromOS(info.Mode()).QidType())
		stat := fileInfoToStat(qid, info)
		session.PutFid(m.Fid(), File{
			Name: fil.Name,
			User: stat.Uid(),
			Flag: fil.Flag,
			Mode: ModeFromOS(info.Mode()),
			H:    fil.H,
		})
		h.tracef("local: Tstat %v %v -> %s", m.Fid(), fil.Name, stat)
		w.Rstat(stat)

	case Tread:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			h.errorf("local: Tread: unknown fid: %v", m.Fid())
			w.Rerror("unknown fid %d", m.Fid())
			return
		}

		if fil.H == nil {
			h.errorf("local: Tread: file not opened: %v", m.Fid())
			w.Rerror("fid %d wasn't opened", m.Fid())
			return
		}
		data := w.RreadBuffer()
		// TODO: handle overflow of converting uint64 -> int64
		// TODO: handle retriable errors
		n, err := fil.H.ReadAt(data, int64(m.Offset()))
		if n == 0 {
			if err == io.EOF {
				w.Rread(nil)
				return
			}
			h.errorf("local: Tread: error: fid %d couldn't read: %s", m.Fid(), err)
			w.Rerror("failed to read: %s", err)
			return
		} else if err != nil && err != io.EOF {
			h.tracef("local: Tread: warn: fid %d couldn't read full buffer (only %d bytes): %s", m.Fid(), n, err)
		}
		h.tracef("local: Tread: fid %d (offset=%d, bytes=%d)", m.Fid(), m.Offset(), n)
		w.Rread(data[:n])

	case Tclunk:
		fil, ok := session.FileForFid(m.Fid())
		if ok {
			if fil.Mode&ORCLOSE != 0 {
				// delete the file
				h.Fs.Delete(fil.Name)
			}
			session.DeleteFid(m.Fid())
			h.tracef("local: Tclunk %v %v", m.Fid(), fil.Name)
			w.Rclunk()
		} else {
			w.Rerror("local: Tclunk: unknown fid %d", m.Fid())
		}

	case Tremove:
		fil, ok := session.FileForFid(m.Fid())
		if ok {
			h.Fs.Delete(fil.Name)
			session.DeleteFid(m.Fid())
			h.tracef("local: Tremove %v %v", m.Fid(), fil.Name)
			w.Rremove()
		} else {
			w.Rerror("local: Tremove: unknown fid %d", m.Fid())
		}

	case Tcreate:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			h.errorf("local: Tcreate: unknown fid: %v", m.Fid())
			w.Rerror("unknown fid %d", m.Fid())
			return
		}
		fullPath := filepath.Join(cleanPath(fil.Name), m.Name())
		info, err := h.Fs.Stat(fullPath)
		if !os.IsNotExist(err) || err == nil {
			h.errorf("local: Tcreate: file exists %v: %s", fullPath, err)
			w.Rerror("file exists: %s", fullPath)
			return
		}
		isDir := m.Perm()&M_DIR != 0
		if isDir {
			// From Topen docs:
			//   "It is illegal to write a directory, truncate it, or attempt to remove it on close"
			if m.Mode()&ORCLOSE != 0 {
				h.tracef("local: Tcreate: client error: requested dir with ORCLOSE")
				w.Rerror("dir cannot have ORCLOSE set")
				return
			} else if m.Mode()&OTRUNC != 0 {
				h.tracef("local: Tcreate: client error: requested dir with OTRUNC")
				w.Rerror("dir cannot have OTRUNC set")
				return
			}

			if err := h.Fs.MakeDir(fullPath, m.Perm()); err != nil {
				h.errorf("local: Tcreate: failed to create dir: %s", err)
				w.Rerror("dir cannot be created: %s", err)
				return
			}
			fil.Flag = m.Mode()
			fil.H = &directoryHandle{
				fs:      h.Fs,
				session: session,
				path:    fullPath,
			}
		} else {
			f, err := h.Fs.CreateFile(fullPath, m.Mode(), fil.Mode)
			if err != nil || f == nil {
				h.tracef("local: Tcreate: error creating file %v: %s", fullPath, err)
				w.Rerror("cannot create: %s", err)
				return
			}
			fil.Flag = m.Mode()
			fil.H = f
		}
		if info != nil {
			fil.Mode = ModeFromOS(info.Mode())
		}
		h.tracef("local: Tcreate: %v -> %v (isDir=%v, mode=%d)", m.Fid(), fil.Name, isDir, m.Mode())
		session.PutFid(m.Fid(), fil)
		q := session.PutQid(fil.Name, m.Perm().QidType())
		w.Rcreate(q, 0) // TODO: would be nice to support iounit

	case Twrite:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			h.errorf("local: Twrite: unknown fid: %v", m.Fid())
			w.Rerror("unknown fid %d", m.Fid())
			return
		}

		if fil.H == nil {
			h.errorf("local: Twrite: file not opened: %v", m.Fid())
			w.Rerror("fid %d wasn't opened", m.Fid())
			return
		}
		data := m.Data()
		h.tracef("local: Twrite: want fid %d (offset=%d, data=%v)", m.Fid(), m.Offset(), data)
		// TODO: handle overflow of converting uint64 -> int64
		// TODO: handle retriable errors
		n, err := fil.H.WriteAt(data, int64(m.Offset()))
		if n == 0 && err != nil {
			h.errorf("local: Twrite: error: fid %d couldn't write: %s", m.Fid(), err)
			w.Rerror("failed to write to fid %d", m.Fid())
			return
		}
		if err != nil {
			h.tracef("local: Twrite: warn: fid %d couldn't write full buffer (only %d/%d bytes): %s", m.Fid(), n, len(data), err)
		}
		h.tracef("local: Twrite: fid %d (offset=%d, bytes=%d)", m.Fid(), m.Offset(), n)
		w.Rwrite(uint32(n))

	case Twstat:
		fil, ok := session.FileForFid(m.Fid())
		if !ok {
			h.errorf("local: Twstat: unknown fid: %v", m.Fid())
			w.Rerror("unknown fid %d", m.Fid())
			return
		}
		fullPath := cleanPath(fil.Name)
		stat := m.Stat()

		// From docs:
		// "As a special case, if all the elements of the directory entry in a
		// Twstat message are ``don't touch'' values, the server may interpret it
		// as a request to guarantee that the contents of the associated file are
		// committed to stable storage before the Rwstat message is returned.
		// (Consider the message to mean, ``make the state of the file exactly what
		// it claims to be.'')"
		if stat.IsZero() {
			if fil.H != nil {
				if err := fil.H.Sync(); err != nil {
					h.errorf("local: Twstat: failed to fsync %v: %s", fullPath, err)
					w.Rerror("failed syncing stat for %s: %s", fullPath, err)
					return
				}
			}
		} else {
			err := h.Fs.WriteStat(fullPath, stat)
			if err != nil {
				h.errorf("local: Twstat: failed for %v: %s", fullPath, err)
				w.Rerror("failed writing stat for %s: %s", fullPath, err)
				return
			}
		}
		// session.PutFid(m.Fid(), File{
		// 	Name: fil.Name,
		// 	User: fil.User,
		// 	Flag: fil.Flag,
		// 	Mode: ModeFromOS(m.Stat()),
		// 	H:    fil.H,
		// })
		// qid := session.PutQid(fil.Name, ModeFromOS(m.Mode()).QidType())
		h.tracef("local: Twstat %v %v -> %s", m.Fid(), fil.Name, stat)
		w.Rstat(stat)

	default:
		break // let default behavior run
	}
}
